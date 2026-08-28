#!/usr/bin/env python3
from __future__ import annotations
"""
NexGen Cloud -- GPU Commissioning Report Generator v3.2

Pulls all commissioning data directly from the MAAS API.
User provides only a hostname; the script resolves it to a system_id,
fetches GPU script outputs (97/98/99), machine hardware details,
lshw DIMM inventory, and network/storage info, then generates a
consolidated HTML certification report.

Usage:
  # Set credentials via .env file (recommended)
  cp .env.example .env   # then edit with your MAAS_URL and MAAS_API_KEY

  # Or export manually
  export MAAS_URL=http://maas.example.com:5240/MAAS
  export MAAS_API_KEY=consumer:token:secret

  # Generate report by hostname (outputs EXAMPLE-GPU-001-MAAS-validation.html)
  python3 nexgen-gpu-report.py --host EXAMPLE-GPU-001

  # Or override output name
  python3 nexgen-gpu-report.py --host EXAMPLE-GPU-001 -o custom.html

  # File-based fallback (no MAAS API needed)
  python3 nexgen-gpu-report.py \\
    --install 97-output.json \\
    --inventory 98-output.json \\
    --stress 99-output.json \\
    -o report.html

Requirements:
  pip install requests-oauthlib
"""

import argparse
import base64
import json
import os
import re
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from html import escape
from pathlib import Path

__version__ = "3.3.0"

# ---------------------------------------------------------------------------
# .env file support — load key=value pairs into os.environ
# ---------------------------------------------------------------------------
def _load_dotenv() -> None:
    """Load .env file from the repo root (parent of reporting/) if it exists."""
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if not env_path.is_file():
        return
    with env_path.open() as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            key, _, value = line.partition("=")
            key, value = key.strip(), value.strip().strip("\"'")
            if key and key not in os.environ:
                os.environ[key] = value

_load_dotenv()

# ---------------------------------------------------------------------------
# MAAS API CLIENT
# ---------------------------------------------------------------------------

# Stages whose absence makes a report incomplete rather than merely partial.
# Install alone proves nothing about the hardware's health.
REQUIRED_STAGES = ("Inventory", "DCGM Diagnostics")

# Map short names used internally to the MAAS script names
SCRIPT_ALIASES = {
    "install":   "90-nexgen-gpu-install-595-13.sh",
    "config":    "91-nexgen-gpu-mig-ecc-config.sh",
    "inventory": "92-nexgen-gpu-inventory.sh",
    "stress":    "98-nexgen-gpu-stress-test.sh",
    "burnin":    "99-nexgen-gpu-burn-in.sh",
}


def _as_int(value) -> int:
    """Best-effort int, 0 when absent or unparseable."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _strip_sh(name: str) -> str:
    """Drop a trailing ".sh" from a script name.

    str.removesuffix is the obvious call, but it is Python 3.9+ and this
    generator runs on hosts still on 3.8 -- the module itself imports there
    only because `from __future__ import annotations` defers the `X | None`
    annotations, so the failure surfaced at runtime rather than on import.
    """
    return name[:-3] if name.endswith(".sh") else name


class MAASClient:
    """Minimal MAAS REST API client with OAuth1 PLAINTEXT auth."""

    def __init__(self, maas_url: str, api_key: str):
        try:
            from oauthlib.oauth1 import SIGNATURE_PLAINTEXT
            from requests_oauthlib import OAuth1Session
        except ImportError:
            print(
                "Error: requests-oauthlib is required for MAAS API mode.\n"
                "  pip install requests-oauthlib",
                file=sys.stderr,
            )
            sys.exit(1)

        self.base = maas_url.rstrip("/")
        self.api = f"{self.base}/api/2.0"

        parts = api_key.split(":")
        if len(parts) != 3:
            print(
                f"Error: MAAS API key must be consumer_key:token_key:token_secret\n"
                f"  Got {len(parts)} part(s)",
                file=sys.stderr,
            )
            sys.exit(1)

        consumer_key, token_key, token_secret = parts
        self.session = OAuth1Session(
            consumer_key,
            resource_owner_key=token_key,
            resource_owner_secret=token_secret,
            signature_method=SIGNATURE_PLAINTEXT,
        )

    def _get(self, path: str, params: dict | None = None) -> "requests.Response":
        url = f"{self.api}/{path.lstrip('/')}"
        resp = self.session.get(url, params=params or {})
        resp.raise_for_status()
        return resp

    # -- Machine lookup --

    def resolve_hostname(self, hostname: str) -> dict:
        """Find machine by hostname, return full machine dict."""
        resp = self._get("machines/", {"hostname": hostname})
        machines = resp.json()
        if not machines:
            print(f"Error: No machine found with hostname '{hostname}'", file=sys.stderr)
            sys.exit(1)
        if len(machines) > 1:
            names = [m.get("hostname", "?") for m in machines]
            print(
                f"Warning: {len(machines)} machines match '{hostname}': {names}. "
                f"Using first match.",
                file=sys.stderr,
            )
        return machines[0]

    def get_machine_details(self, system_id: str) -> dict:
        """Full machine details (CPU, RAM, disks, NICs, NUMA, hardware_info)."""
        return self._get(f"machines/{system_id}/").json()

    def get_machine_lshw(self, system_id: str) -> bytes | None:
        """Fetch lshw XML via commissioning script output."""
        log = lambda m: print(f"[maas-report]   lshw: {m}", file=sys.stderr)

        # MAAS uses different lshw script names across versions
        lshw_names = [
            "00-maas-01-lshw",
            "lshw",
            "maas-lshw",
            "00-maas-00-lshw",
        ]

        # Strategy 1: Try each name via commissioning results API
        for script_name in lshw_names:
            try:
                data = self.get_commissioning_results(system_id, [script_name])
                results = data.get("results", [])
                if not results:
                    continue
                log(f"found {len(results)} result(s) for '{script_name}'")
                for result in results:
                    name = result.get("name", "")
                    stdout_b64 = result.get("stdout", "")
                    if stdout_b64:
                        raw = base64.b64decode(stdout_b64)
                        if b"<?xml" in raw or b"<list>" in raw or b"<node" in raw:
                            log(f"got {len(raw)} bytes XML from {name}")
                            return raw
                        else:
                            log(f"{name}: {len(raw)} bytes but not XML")
            except Exception:
                continue

        # Strategy 2: Fetch ALL commissioning results, find anything with lshw
        try:
            log("trying unfiltered search for lshw scripts...")
            data = self.get_commissioning_results(system_id)
            for result in data.get("results", []):
                name = result.get("name", "")
                if "lshw" in name.lower():
                    stdout_b64 = result.get("stdout", "")
                    if stdout_b64:
                        raw = base64.b64decode(stdout_b64)
                        if b"<?xml" in raw or b"<list>" in raw or b"<node" in raw:
                            log(f"found lshw as '{name}' — {len(raw)} bytes")
                            return raw
            log("no lshw script found in commissioning results")
        except Exception as e:
            log(f"unfiltered search failed: {e}")

        # Strategy 3: ?op=details BSON (requires pymongo)
        try:
            import bson
            resp = self._get(f"machines/{system_id}/", {"op": "details"})
            details = bson.BSON(resp.content).decode()
            lshw_data = details.get("lshw", b"")
            if lshw_data:
                log(f"BSON decode OK — {len(lshw_data)} bytes")
                return lshw_data if isinstance(lshw_data, bytes) else lshw_data.encode()
        except ImportError:
            log("pymongo not installed, skipping BSON strategy")
        except Exception as e:
            log(f"BSON details failed: {e}")

        log("all strategies exhausted — no lshw data")
        return None

    def get_machine_resources(self, system_id: str) -> dict | None:
        """Fetch machine-resources JSON (detailed PCI, memory, etc)."""
        names = [
            "40-maas-01-machine-resources",
            "machine-resources",
            "maas-machine-resources",
        ]
        # Try specific names first
        for script_name in names:
            try:
                data = self.get_commissioning_results(system_id, [script_name])
                for result in data.get("results", []):
                    if "machine-resources" in result.get("name", "") or result.get("name", "") == script_name:
                        stdout_b64 = result.get("stdout", "")
                        if stdout_b64:
                            raw = base64.b64decode(stdout_b64).decode("utf-8", errors="replace")
                            return _extract_json(raw)
            except Exception:
                continue

        # Fallback: search all results
        try:
            data = self.get_commissioning_results(system_id)
            for result in data.get("results", []):
                name = result.get("name", "")
                if "machine-resources" in name or "machine_resources" in name:
                    stdout_b64 = result.get("stdout", "")
                    if stdout_b64:
                        raw = base64.b64decode(stdout_b64).decode("utf-8", errors="replace")
                        return _extract_json(raw)
        except Exception as e:
            print(f"Warning: machine-resources fetch failed: {e}", file=sys.stderr)
        return None

    def get_commissioning_script_stdout(
        self, system_id: str, script_name_hint: str
    ) -> str | None:
        """Fetch raw stdout of a commissioning script by partial name match."""
        try:
            # Try exact name first
            data = self.get_commissioning_results(system_id, [script_name_hint])
            results = data.get("results", [])
            if results:
                stdout_b64 = results[0].get("stdout", "")
                if stdout_b64:
                    return base64.b64decode(stdout_b64).decode("utf-8", errors="replace")
            # Search all results
            data = self.get_commissioning_results(system_id)
            for result in data.get("results", []):
                name = result.get("name", "")
                if script_name_hint.lower() in name.lower():
                    stdout_b64 = result.get("stdout", "")
                    if stdout_b64:
                        return base64.b64decode(stdout_b64).decode("utf-8", errors="replace")
        except Exception as e:
            print(f"Warning: Could not fetch {script_name_hint}: {e}", file=sys.stderr)
        return None

    # -- Commissioning results --

    def get_commissioning_results(
        self, system_id: str, script_names: list[str] | None = None
    ) -> dict:
        """Fetch current commissioning results, optionally filtered by script names."""
        params = {"include_output": "1"}
        if script_names:
            params["filters"] = ",".join(script_names)
        resp = self._get(
            f"nodes/{system_id}/results/current-commissioning/", params
        )
        return resp.json()

    def get_script_json(self, system_id: str, script_name: str) -> dict | None:
        """Fetch a specific script's stdout and parse as JSON.

        Newer MAAS versions store scripts with .sh extension, older ones
        without.  We try both variants so the lookup works either way.
        """
        candidates = [script_name]
        if script_name.endswith(".sh"):
            candidates.append(_strip_sh(script_name))
        else:
            candidates.append(script_name + ".sh")
        try:
            data = self.get_commissioning_results(system_id, candidates)
            for result in data.get("results", []):
                if result.get("name") in candidates:
                    stdout_b64 = result.get("stdout", "")
                    if stdout_b64:
                        raw = base64.b64decode(stdout_b64).decode("utf-8", errors="replace")
                        return _extract_json(raw)
            return None
        except Exception as e:
            print(f"Warning: Could not fetch {script_name}: {e}", file=sys.stderr)
            return None

    def get_all_commissioning_scripts(self, system_id: str) -> list[dict]:
        """List all commissioning result entries (names, statuses, runtimes)."""
        data = self.get_commissioning_results(system_id)
        results = []
        for r in data.get("results", []):
            results.append({
                "name": r.get("name", "?"),
                "status": r.get("status_name", "?"),
                "exit_status": r.get("exit_status"),
                "runtime": r.get("runtime", ""),
            })
        return results

    def get_script_stdout_raw(self, system_id: str, script_name: str,
                              output: str = "stdout") -> str | None:
        """Download raw text for a script (no base64).

        `output` selects the stream: "stdout", "stderr" or "all".  The scripts
        put their JSON payload on stdout and every log line, nvidia-smi dump and
        diagnostic artifact on stderr, so "stderr" is where the evidence
        artifacts live -- MAAS retains them as part of the commissioning result.

        Tries both with and without .sh extension for MAAS compatibility.
        """
        candidates = [script_name]
        if script_name.endswith(".sh"):
            candidates.append(_strip_sh(script_name))
        else:
            candidates.append(script_name + ".sh")
        for name in candidates:
            try:
                resp = self._get(
                    f"nodes/{system_id}/results/current-commissioning/",
                    {
                        "op": "download",
                        "output": output,
                        "filetype": "txt",
                        "filters": name,
                    },
                )
                if resp.text.strip():
                    return resp.text
            except Exception:
                continue
        return None


def _extract_json(text: str) -> dict | None:
    """Extract the first valid JSON object from text that may have non-JSON content."""
    text = text.strip()
    # Try direct parse first
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    # Scan for the first balanced top-level object, skipping over string
    # literals.  A naive depth counter desyncs on any brace inside a string
    # value -- and the scripts now embed base64 blobs, dmesg excerpts and
    # nvidia-smi error text, all of which can contain braces.
    start = text.find("{")
    if start < 0:
        return None
    while start >= 0:
        depth = 0
        in_str = False
        escaped = False
        for i in range(start, len(text)):
            ch = text[i]
            if in_str:
                if escaped:
                    escaped = False
                elif ch == "\\":
                    escaped = True
                elif ch == '"':
                    in_str = False
                continue
            if ch == '"':
                in_str = True
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    try:
                        return json.loads(text[start : i + 1])
                    except json.JSONDecodeError:
                        break
        # That candidate did not parse -- try the next top-level brace.
        start = text.find("{", start + 1)
    return None


# ---------------------------------------------------------------------------
# PER-GPU ACCEPTANCE ADJUDICATION
# ---------------------------------------------------------------------------
# Hardware acceptance criteria, evaluated per GPU against the evidence the
# commissioning scripts collected.  `reject` is the numbered condition in the
# customer specification each check implements.
#
# Two deliberate departures from a literal reading of that specification, both
# to avoid failing healthy hardware:
#
#   * Throttling (#6). A card at sustained full load will sit against its power
#     limiter, reporting sw_power_cap. That is the limiter working, not a
#     defect, so only the hardware slowdowns and a sustained clock floor are
#     treated as faults.
#   * PCIe (#7). Links downtrain to Gen1 at idle to save power, so the idle
#     value proves nothing. What is checked is that the link trained to the
#     best generation BOTH ends support, and that width is full. A Gen4 host
#     capping a Gen5 card is reported, not failed -- which is what the
#     specification's own Gen4-host clause asks for.
ACCEPTANCE_MAX_AGE_DAYS = 30
ACCEPTANCE_REPLAY_DELTA_LIMIT = 10
XID_DISQUALIFYING = {48, 63, 64, 79, 92, 94, 95}


def _n(v):
    """Coerce to a number, or None. Treats bools as absent."""
    if v is None or isinstance(v, bool):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _crit(cid, reject, label, status, detail):
    return {"id": cid, "reject": reject, "label": label,
            "status": status, "detail": detail}


def _check_identity(gpu):
    ident = gpu.get("identity") or {}
    serial = gpu.get("serial")
    serial_q = ident.get("serial_from_query")
    uuid = gpu.get("uuid")
    out = []

    missing = [n for n, v in (("serial", serial), ("UUID", uuid)) if not v]
    if missing:
        out.append(_crit("identity_present", 8, "Serial and UUID reported",
                         "FAIL", "missing: " + ", ".join(missing)))
    else:
        out.append(_crit("identity_present", 8, "Serial and UUID reported",
                         "PASS", f"{serial} / {uuid}"))

    # Cross-check the bulk query against the -q document. Both come from
    # nvidia-smi, so disagreement means something is genuinely inconsistent.
    if serial and serial_q:
        if str(serial).strip() == str(serial_q).strip():
            out.append(_crit("serial_match", 8, "Serial consistent across sources",
                             "PASS", f"query and -q agree: {serial}"))
        else:
            out.append(_crit("serial_match", 8, "Serial consistent across sources",
                             "FAIL", f"bulk query {serial} != -q {serial_q}"))
    else:
        out.append(_crit("serial_match", 8, "Serial consistent across sources",
                         "N/A", "second source unavailable"))

    corrupt = ident.get("inforom_corrupted")
    if corrupt is True:
        out.append(_crit("inforom", 8, "InfoROM not corrupted", "FAIL",
                         "nvidia-smi reports InfoROM corruption"))
    elif corrupt is False:
        irom = ident.get("inforom") or {}
        ver = irom.get("image") or "version unreported"
        out.append(_crit("inforom", 8, "InfoROM not corrupted", "PASS", str(ver)))
    else:
        out.append(_crit("inforom", 8, "InfoROM not corrupted", "N/A",
                         "not collected"))
    return out


def _check_memory(gpu, cfg=None):
    out = []
    remap = gpu.get("remapped_rows")
    if not isinstance(remap, dict):
        out.append(_crit("remap_failure", 1, "No row-remapping failure", "N/A",
                         "row remapper data not collected"))
        out.append(_crit("remap_uce", 1, "No uncorrectable-error remapped rows",
                         "N/A", "row remapper data not collected"))
        out.append(_crit("remap_pending", 2, "No pending row remap", "N/A",
                         "row remapper data not collected"))
    else:
        if remap.get("failure_occurred") is True:
            out.append(_crit("remap_failure", 1, "No row-remapping failure",
                             "FAIL", "Remapping Failure Occurred = Yes (RMA condition)"))
        else:
            out.append(_crit("remap_failure", 1, "No row-remapping failure",
                             "PASS", "no failure"))

        ruce = _n(remap.get("uncorrectable"))
        rce = _n(remap.get("correctable"))
        if ruce is None:
            out.append(_crit("remap_uce", 1, "No uncorrectable-error remapped rows",
                             "N/A", "count unavailable"))
        elif ruce > 0:
            out.append(_crit("remap_uce", 1, "No uncorrectable-error remapped rows",
                             "FAIL", f"{int(ruce)} row(s) remapped for uncorrectable errors"))
        else:
            ce_note = f"0 uncorrectable, {int(rce)} correctable (delivery baseline)" \
                      if rce is not None else "0 uncorrectable"
            out.append(_crit("remap_uce", 1, "No uncorrectable-error remapped rows",
                             "PASS", ce_note))

        if remap.get("pending") is True:
            out.append(_crit("remap_pending", 2, "No pending row remap", "FAIL",
                             "Pending = Yes: card not reset and re-queried, test incomplete"))
        elif remap.get("pending") is False:
            out.append(_crit("remap_pending", 2, "No pending row remap", "PASS",
                             "no remap pending"))
        else:
            out.append(_crit("remap_pending", 2, "No pending row remap", "N/A",
                             "state unavailable"))

    banks = gpu.get("bank_remap_availability")
    if isinstance(banks, dict):
        degraded = sum(int(_n(banks.get(k)) or 0) for k in ("partial", "low", "none"))
        hi = ", ".join(f"{k}={int(_n(banks.get(k)) or 0)}" for k in
                       ("max", "high", "partial", "low", "none"))
        if degraded > 0:
            out.append(_crit("bank_hist", 2, "Bank remap availability at Max/High",
                             "FAIL", f"{degraded} bank(s) below High -- {hi}"))
        else:
            out.append(_crit("bank_hist", 2, "Bank remap availability at Max/High",
                             "PASS", hi))
    else:
        out.append(_crit("bank_hist", 2, "Bank remap availability at Max/High",
                         "N/A", "histogram not collected"))

    # ECC must be on, and the counters must be the aggregate ones: volatile
    # counters reset on reboot, so they cannot evidence a used card's history.
    mode = gpu.get("ecc_mode")
    ecc = gpu.get("ecc") or {}
    if mode and str(mode).strip().lower().startswith("enabled"):
        out.append(_crit("ecc_mode", 3, "ECC enabled during test", "PASS", str(mode)))
    elif mode:
        out.append(_crit("ecc_mode", 3, "ECC enabled during test", "FAIL",
                         f"ECC mode is {mode}"))
    else:
        out.append(_crit("ecc_mode", 3, "ECC enabled during test", "FAIL",
                         "ECC mode not reported (N/A is not acceptable)"))

    agg_c = _n(ecc.get("corrected_aggregate"))
    agg_u = _n(ecc.get("uncorrected_aggregate"))
    vol_c = _n(ecc.get("corrected_volatile"))
    vol_u = _n(ecc.get("uncorrected_volatile"))
    if agg_c is None and agg_u is None:
        # With ECC disabled nvidia-smi returns N/A for every counter, volatile
        # included -- so distinguish "no counters at all" from "volatile only".
        if vol_c is None and vol_u is None:
            detail = ("no ECC counters at all -- nothing was being detected, "
                      "so there is no error history to inspect")
        else:
            detail = "only volatile counters available; these reset on reboot"
        out.append(_crit("ecc_aggregate", 3, "Aggregate ECC counters reported",
                         "FAIL", detail))
    else:
        out.append(_crit("ecc_aggregate", 3, "Aggregate ECC counters reported",
                         "PASS",
                         f"corrected={int(agg_c) if agg_c is not None else '?'}, "
                         f"uncorrected={int(agg_u) if agg_u is not None else '?'}"))

    # An aggregate counter only evidences the card's prior life if ECC was on
    # during it.  Script 91 turns ECC on when it finds it off -- after which the
    # counters read zero and start counting from that moment.  Enabling ECC does
    # not backfill, so without this the report would let a meaningless zero
    # imply a clean history.  This is the single most misleading number on an
    # ex-mining or ex-render card, so it is called out rather than inferred.
    if cfg and cfg.get("ecc_changed") is True:
        before = cfg.get("ecc_before") or "Disabled"
        out.append(_crit("ecc_history", 3, "ECC history covers the card's prior life",
                         "FAIL",
                         f"ECC was {before} and was enabled during THIS run -- "
                         "aggregate counters start from zero here, so this run "
                         "cannot evidence the card's memory health. "
                         "RE-RUN COMMISSIONING to produce a valid report."))
    elif cfg is not None:
        out.append(_crit("ecc_history", 3, "ECC history covers the card's prior life",
                         "PASS", "ECC already enabled before commissioning"))
    else:
        out.append(_crit("ecc_history", 3, "ECC history covers the card's prior life",
                         "N/A", "script 91 result unavailable -- prior ECC state unknown"))

    if agg_u is not None and agg_u > 0:
        out.append(_crit("ecc_uncorrected", 1, "No uncorrectable ECC errors",
                         "FAIL", f"{int(agg_u)} aggregate uncorrectable error(s)"))
    elif agg_u is None:
        out.append(_crit("ecc_uncorrected", 1, "No uncorrectable ECC errors",
                         "N/A", "aggregate count unavailable"))
    else:
        out.append(_crit("ecc_uncorrected", 1, "No uncorrectable ECC errors",
                         "PASS", "0 aggregate uncorrectable"))

    sram_u = _n(ecc.get("sram_uncorrected_aggregate"))
    thresh = gpu.get("sram_threshold_exceeded")
    if thresh is True:
        out.append(_crit("sram", 1, "SRAM uncorrectable errors zero", "FAIL",
                         "SRAM threshold exceeded (RMA condition)"))
    elif sram_u is not None and sram_u > 0:
        out.append(_crit("sram", 1, "SRAM uncorrectable errors zero", "FAIL",
                         f"{int(sram_u)} aggregate SRAM uncorrectable error(s)"))
    elif sram_u is None and thresh is None:
        out.append(_crit("sram", 1, "SRAM uncorrectable errors zero", "N/A",
                         "SRAM counters not collected"))
    else:
        out.append(_crit("sram", 1, "SRAM uncorrectable errors zero", "PASS",
                         "0 SRAM uncorrectable"))
    return out


def _check_pcie(gpu, burn_gpu, burn_csb=None, burn_ran=None):
    out = []
    p = gpu.get("pcie") or {}
    w_max = _n(p.get("width_max")) or _n(gpu.get("pcie_width_max"))
    w_cur = _n(p.get("width_current")) or _n(gpu.get("pcie_width_current"))
    dev_max = _n(p.get("gen_device_max"))
    host_max = _n(p.get("gen_host_max"))
    neg_max = _n(p.get("gen_negotiated_max")) or _n(gpu.get("pcie_gen_max"))

    if w_max is None or w_cur is None:
        out.append(_crit("pcie_width", 7, "Link width x16", "N/A",
                         "width not reported"))
    elif int(w_max) == 16 and int(w_cur) == 16:
        out.append(_crit("pcie_width", 7, "Link width x16", "PASS", "x16 / x16"))
    else:
        out.append(_crit("pcie_width", 7, "Link width x16", "FAIL",
                         f"trained x{int(w_cur)} of a possible x{int(w_max)}"))

    # The link should reach whatever both ends support.  Comparing against a
    # fixed Gen5 would fail every card in a Gen4 host, which is a property of
    # the test rig and not of the card.
    if neg_max is None:
        out.append(_crit("pcie_gen", 7, "Link trained to host+card maximum",
                         "N/A", "generation not reported"))
    else:
        ceiling = min([g for g in (dev_max, host_max) if g is not None],
                      default=None)
        if ceiling is None:
            out.append(_crit("pcie_gen", 7, "Link trained to host+card maximum",
                             "PASS", f"negotiated Gen{int(neg_max)} (no separate ceilings reported)"))
        elif neg_max >= ceiling:
            note = f"Gen{int(neg_max)} = min(card Gen{int(dev_max)}, host Gen{int(host_max)})" \
                   if dev_max is not None and host_max is not None \
                   else f"negotiated Gen{int(neg_max)}"
            if host_max is not None and dev_max is not None and host_max < dev_max:
                note += " -- host is the limit, not the card"
            out.append(_crit("pcie_gen", 7, "Link trained to host+card maximum",
                             "PASS", note))
        else:
            out.append(_crit("pcie_gen", 7, "Link trained to host+card maximum",
                             "FAIL",
                             f"negotiated Gen{int(neg_max)} below the Gen{int(ceiling)} both ends support"))

    # The burn-in derives this delta twice. counter_deltas comes from the CSV
    # --query-gpu path, where driver 595 does not support pcie.replay_counter at
    # all, so the value is null there. counters_since_baseline comes from
    # `nvidia-smi -q` ("Replays Since Reset"), which does work on 595 -- the
    # burn-in log prints "replay+0" from it. Prefer the CSV value, fall back to
    # the one that survives on this driver rather than reporting no evidence.
    delta = _n((burn_gpu or {}).get("pcie_replay_delta"))
    if delta is None:
        delta = _n((burn_csb or {}).get("replay_delta"))
    abs_replay = _n(p.get("replays_since_reset"))
    if delta is not None:
        if delta > ACCEPTANCE_REPLAY_DELTA_LIMIT:
            out.append(_crit("pcie_replay", 7, "PCIe replay counter stable under load",
                             "FAIL", f"+{int(delta)} replays across the load window"))
        else:
            out.append(_crit("pcie_replay", 7, "PCIe replay counter stable under load",
                             "PASS", f"+{int(delta)} replays across the load window"))
    elif abs_replay is not None:
        # "burn-in not run" is the wrong explanation once it HAS run and the
        # driver simply did not report the counter.
        if burn_ran:
            why = "the driver reported no load-window replay delta"
        else:
            why = "no load window (burn-in not run)"
        out.append(_crit("pcie_replay", 7, "PCIe replay counter stable under load",
                         "WARN", f"{int(abs_replay)} since reset; {why}"))
    else:
        out.append(_crit("pcie_replay", 7, "PCIe replay counter stable under load",
                         "N/A", "replay counter not reported"))
    return out


# DCGM skips nvbandwidth when there is no NVLink to measure. H100 PCIe ships
# with or without bridges, so on a host with none this is expected operation,
# not missing evidence -- the pcie test already measures host/device bandwidth
# (it reported 84 GB/s bidirectional here). It stays a WARN when NVLink IS
# present, because then the skip is unexplained.
SKIP_EXPECTED_WITHOUT_NVLINK = {"nvbandwidth"}


def skipped_test_names(diag) -> set:
    """Names of DCGM tests with at least one skipped result."""
    names = set()
    for t in (diag or {}).get("test_results") or []:
        for r in t.get("results") or []:
            if "skip" in str(r.get("status", "")).lower() \
               or "not run" in str(r.get("status", "")).lower():
                names.add(t.get("test", "?"))
                break
    return names


def _check_stress(gpu_index, stress, gpu_results, context=None):
    """gpu_results: list of {test, status, info} for this GPU."""
    diag = (stress or {}).get("dcgm_diagnostics") or {}
    if not stress:
        return [_crit("stress_present", 5, "DCGM diagnostic executed and passed",
                      "FAIL", "no stress-test result at all")]
    out = []
    level = str(diag.get("run_level", "?"))
    if not gpu_results:
        out.append(_crit("stress_present", 5, "DCGM diagnostic executed and passed",
                         "FAIL", f"level {level} run produced no result for this GPU"))
        return out

    bad = [r for r in gpu_results
           if any(k in str(r.get("status", "")).lower()
                  for k in ("config", "retest"))]
    skipped = [r for r in gpu_results
               if "skip" in str(r.get("status", "")).lower()
               or "not run" in str(r.get("status", "")).lower()]
    failed = [r for r in gpu_results if "fail" in str(r.get("status", "")).lower()]
    passed = [r for r in gpu_results if "pass" in str(r.get("status", "")).lower()]

    if bad:
        out.append(_crit("stress_present", 5, "DCGM diagnostic executed and passed",
                         "FAIL",
                         "CONFIG/RETEST result is a rig failure, not a hardware pass: "
                         + ", ".join(sorted({r.get("test", "?") for r in bad}))))
    elif failed:
        out.append(_crit("stress_present", 5, "DCGM diagnostic executed and passed",
                         "FAIL", "failed: " + ", ".join(sorted({r.get("test", "?") for r in failed}))))
    elif not passed:
        out.append(_crit("stress_present", 5, "DCGM diagnostic executed and passed",
                         "FAIL", f"{len(gpu_results)} result(s), none passed"))
    elif skipped:
        skipped_tests = {r.get("test", "?") for r in skipped}
        nvlink = (context or {}).get("nvlink_present")
        # Only excuse the skip when we positively know there is no NVLink.
        excused = (SKIP_EXPECTED_WITHOUT_NVLINK
                   if nvlink is False else set())
        unexplained = skipped_tests - excused
        if unexplained:
            out.append(_crit("stress_present", 5, "DCGM diagnostic executed and passed",
                             "WARN",
                             f"level {level}, {len(passed)} passed but skipped: "
                             + ", ".join(sorted(unexplained))))
        else:
            out.append(_crit("stress_present", 5, "DCGM diagnostic executed and passed",
                             "PASS",
                             f"level {level}, {len(passed)} test(s) passed; "
                             + ", ".join(sorted(skipped_tests))
                             + " skipped as expected (no NVLink fitted)"))
    else:
        out.append(_crit("stress_present", 5, "DCGM diagnostic executed and passed",
                         "PASS", f"level {level}, {len(passed)} test(s) passed"))

    try:
        lvl_ok = int(float(level)) >= 3
    except (TypeError, ValueError):
        lvl_ok = None
    if lvl_ok is True:
        out.append(_crit("stress_level", 5, "Diagnostic level 3 or higher",
                         "PASS", f"level {level}"))
    elif lvl_ok is False:
        out.append(_crit("stress_level", 5, "Diagnostic level 3 or higher",
                         "FAIL", f"level {level} is below the required minimum"))
    else:
        out.append(_crit("stress_level", 5, "Diagnostic level 3 or higher",
                         "WARN", f"non-numeric level {level!r}"))
    return out


def _check_cumulative(gpu_index, stress, burn):
    """Faults accumulated across the whole test sequence, not one window.

    A delta measured only across the burn-in window cannot see an error raised
    by the DCGM diagnostic, because that error is already in the window's own
    baseline. Scripts 98 and 99 both compare against the pre-load baseline that
    script 92 writes, so the last phase to run carries the cumulative figure.
    """
    src = None
    for cand in (((burn or {}).get("burn_in") or {}).get("counters_since_baseline"),
                 (stress or {}).get("counters_since_baseline")):
        if isinstance(cand, dict):
            src = cand
            break
    if src is None:
        return [_crit("cumulative_faults", 1,
                      "No new faults across the whole test sequence", "N/A",
                      "no post-test counter check in this report")]
    if not src.get("baseline_available"):
        return [_crit("cumulative_faults", 1,
                      "No new faults across the whole test sequence", "WARN",
                      "no pre-load baseline was written, so cumulative deltas "
                      "are unavailable; absolute counters only")]

    row = next((d for d in src.get("deltas") or []
                if int_or_none(d.get("gpu_index")) == gpu_index), None)
    if row is None:
        return [_crit("cumulative_faults", 1,
                      "No new faults across the whole test sequence", "FAIL",
                      "this GPU is absent from the post-test counter check")]

    # A counter lower than the baseline was cleared mid-sequence, so the
    # comparison has no history to reason over. "Cannot prove clean" is not
    # clean, so this is flagged rather than passed -- the same reasoning that
    # makes aggregate counters preferable to volatile ones in the first place.
    negatives = [k for k in ("ecc_uncorrected_delta", "ecc_corrected_delta",
                             "remapped_rows_uncorrectable_delta",
                             "remapped_rows_correctable_delta", "replay_delta")
                 if (_n(row.get(k)) or 0) < 0]
    if negatives or gpu_index in (src.get("counters_reset_gpus") or []):
        return [_crit("cumulative_faults", 1,
                      "No new faults across the whole test sequence", "WARN",
                      "counters went backwards since the baseline ("
                      + ", ".join(negatives or ["reset detected"])
                      + ") -- something cleared them mid-sequence, so this "
                        "comparison cannot evidence memory health")]

    bad = []
    uce = _n(row.get("ecc_uncorrected_delta"))
    ruce = _n(row.get("remapped_rows_uncorrectable_delta"))
    if uce and uce > 0:
        bad.append(f"+{int(uce)} uncorrectable ECC")
    if ruce and ruce > 0:
        bad.append(f"+{int(ruce)} uncorrectable remapped rows")
    if row.get("remapped_rows_failure_now") is True:
        bad.append("remap failure now set")
    if row.get("remapped_rows_pending_now") is True:
        bad.append("remap pending now set")
    if bad:
        return [_crit("cumulative_faults", 1,
                      "No new faults across the whole test sequence", "FAIL",
                      "since the pre-load baseline: " + ", ".join(bad))]

    ce = _n(row.get("ecc_corrected_delta"))
    rep = _n(row.get("replay_delta"))
    note = "no new uncorrectable errors or remapped rows"
    extra = []
    if ce is not None:
        extra.append(f"corrected ECC {int(ce):+d}")
    if rep is not None:
        extra.append(f"PCIe replays {int(rep):+d}")
    if extra:
        note += " (" + ", ".join(extra) + ")"
    return [_crit("cumulative_faults", 1,
                  "No new faults across the whole test sequence", "PASS", note)]


def _check_burn(gpu_index, burn, burn_tel, burn_delta, context=None):
    out = []
    if not burn:
        # A script MAAS records as Passed but which emitted no parseable report
        # is a different fact from a script that was never uploaded, and the
        # certificate must not present the first as the second. This is exactly
        # what happened when the burn-in report hit MAX_ARG_STRLEN: MAAS logged
        # "Passed 0:05:47" while the certificate said "not run".
        ran = (context or {}).get("burn_script_ran")
        if ran:
            note = ("burn-in ran in MAAS but produced no parseable report -- "
                    "no load evidence; see the commissioning log")
        elif ran is False:
            note = "burn-in script not uploaded to MAAS (optional)"
        else:
            note = "no burn-in result (script not run, or produced no report)"
        out.append(_crit("burn_present", 4, "Sustained load applied", "WARN", note))
        out.append(_crit("throttle", 6, "No thermal or power-brake throttling",
                         "N/A", "no sustained-load telemetry"))
        out.append(_crit("xid", 4, "No disqualifying Xid events", "N/A",
                         "no burn-in window to inspect"))
        return out

    b = burn.get("burn_in") or {}
    mode = b.get("mode", "?")
    tool = b.get("tool", "none")
    dur = _n(b.get("duration_actual_seconds")) or 0
    required = _n(b.get("duration_requested_seconds")) or 1800
    # Prefer how long THIS GPU was actually drawing load over the run's
    # wall-clock. A real run had a load generator that covered 4 of 8 cards for
    # the full window and the rest for ~290s, while the run duration looked fine.
    loaded = _n((burn_tel or {}).get("loaded_seconds"))
    if tool == "none":
        out.append(_crit("burn_present", 4, "Sustained load applied", "FAIL",
                         "no load generator available -- nothing was tested"))
    elif loaded is not None:
        if loaded < required * 0.9:
            out.append(_crit("burn_present", 4, "Sustained load applied", "FAIL",
                             f"this GPU drew load for only {int(loaded)}s of the "
                             f"{int(required)}s required -- the load generator did "
                             f"not cover it, so it was not tested"))
        elif loaded < 1800:
            out.append(_crit("burn_present", 4, "Sustained load applied", "WARN",
                             f"{tool}, this GPU under load {int(loaded)}s, "
                             f"below the 1800s the spec asks for"))
        else:
            out.append(_crit("burn_present", 4, "Sustained load applied", "PASS",
                             f"{tool}, this GPU under load {int(loaded)}s ({mode})"))
    elif dur < 1800:
        out.append(_crit("burn_present", 4, "Sustained load applied", "WARN",
                         f"{tool} for {int(dur)}s, below the 1800s the spec asks for; "
                         "per-GPU load coverage not reported"))
    else:
        out.append(_crit("burn_present", 4, "Sustained load applied", "PASS",
                         f"{tool} for {int(dur)}s ({mode}); "
                         "per-GPU load coverage not reported"))

    th = (burn_tel or {}).get("throttle_samples")
    if not isinstance(th, dict):
        out.append(_crit("throttle", 6, "No thermal or power-brake throttling",
                         "N/A", "throttle reasons unavailable on this driver"))
    else:
        hard = {k: int(_n(th.get(k)) or 0) for k in
                ("hw_thermal_slowdown", "hw_power_brake_slowdown", "sw_thermal_slowdown")}
        capped = int(_n(th.get("sw_power_cap")) or 0)
        hit = {k: v for k, v in hard.items() if v > 0}
        # sw_power_cap is the limiter doing its job at full load, and is
        # reported rather than penalised.
        cap_note = f"sw_power_cap active in {capped} sample(s), which is expected at full load" \
                   if capped else "no power capping observed"
        if hit:
            out.append(_crit("throttle", 6, "No thermal or power-brake throttling",
                             "FAIL",
                             "; ".join(f"{k} in {v} sample(s)" for k, v in hit.items())
                             + f" ({cap_note})"))
        else:
            out.append(_crit("throttle", 6, "No thermal or power-brake throttling",
                             "PASS", cap_note))

    tel = burn_tel or {}
    p = tel.get("power_w") or {}
    t = tel.get("temp_gpu_c") or {}
    c = tel.get("clocks_sm_mhz") or {}
    if p or t:
        out.append(_crit("load_telemetry", 4, "Loaded power and temperature recorded",
                         "PASS",
                         f"power max {p.get('max','?')} W, GPU temp max {t.get('max','?')} C, "
                         f"SM clock mean {c.get('mean','?')} MHz"))
    else:
        out.append(_crit("load_telemetry", 4, "Loaded power and temperature recorded",
                         "FAIL", "no telemetry captured during the load window"))

    # gpu-burn's own per-GPU verdict. A FAULTY card produced wrong arithmetic
    # under sustained load, which telemetry cannot show -- this is the strongest
    # single piece of evidence the burn-in phase produces.
    gb = {int_or_none(r.get("gpu_index")): r
          for r in (b.get("gpu_burn_results") or [])
          if int_or_none(r.get("gpu_index")) is not None}
    gb_errors = _n(b.get("gpu_burn_error_count"))
    if gpu_index in gb:
        st = str(gb[gpu_index].get("status", "")).upper()
        if st == "FAULTY":
            out.append(_crit("compute_correct", 4, "Arithmetic correct under load",
                             "FAIL", "gpu-burn reported this GPU FAULTY"))
        elif st == "OK":
            note = "gpu-burn: OK"
            if gb_errors:
                note += f" (run-wide error count {int(gb_errors)})"
            out.append(_crit("compute_correct", 4, "Arithmetic correct under load",
                             "PASS", note))
        else:
            out.append(_crit("compute_correct", 4, "Arithmetic correct under load",
                             "WARN", f"gpu-burn status {st!r} not recognised"))
    elif b.get("tool") == "gpu-burn":
        out.append(_crit("compute_correct", 4, "Arithmetic correct under load",
                         "FAIL", "gpu-burn produced no verdict for this GPU"))
    else:
        out.append(_crit("compute_correct", 4, "Arithmetic correct under load",
                         "N/A", f"load source {b.get('tool', '?')} reports no "
                                "per-GPU arithmetic verdict"))
    if gb_errors and gb_errors > 0:
        out.append(_crit("compute_errors", 4, "No computation errors under load",
                         "FAIL", f"{int(gb_errors)} computation error(s) counted"))

    xid = b.get("xid") or {}
    crit_list = xid.get("critical") or []
    other = xid.get("other") or []
    if crit_list:
        out.append(_crit("xid", 4, "No disqualifying Xid events", "FAIL",
                         "Xid " + ", ".join(str(x) for x in crit_list)))
    elif other:
        out.append(_crit("xid", 4, "No disqualifying Xid events", "WARN",
                         "non-disqualifying Xid " + ", ".join(str(x) for x in other)))
    else:
        out.append(_crit("xid", 4, "No disqualifying Xid events", "PASS",
                         "none in the burn-in window"))

    d = burn_delta or {}
    du = _n(d.get("ecc_uncorrected_aggregate_delta"))
    dr = _n(d.get("remapped_rows_uncorrectable_delta"))
    if du is None and dr is None:
        out.append(_crit("load_degradation", 4, "No new faults under load", "N/A",
                         "counter deltas unavailable"))
    elif (du or 0) > 0 or (dr or 0) > 0:
        out.append(_crit("load_degradation", 4, "No new faults under load", "FAIL",
                         f"+{int(du or 0)} uncorrectable ECC, +{int(dr or 0)} uncorrectable remapped rows"))
    else:
        out.append(_crit("load_degradation", 4, "No new faults under load", "PASS",
                         "no new uncorrectable errors or remapped rows"))
    return out


def _stress_results_for_gpu(stress, gpu_index):
    diag = (stress or {}).get("dcgm_diagnostics") or {}
    out = []
    for t in diag.get("test_results") or []:
        for r in t.get("results") or []:
            gid = r.get("gpu_id")
            if gid is None or int_or_none(gid) == gpu_index:
                out.append({"test": t.get("test"), "status": r.get("status"),
                            "info": r.get("info")})
    return out


def int_or_none(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _by_index(rows, key="gpu_index"):
    out = {}
    for r in rows or []:
        i = int_or_none(r.get(key))
        if i is not None:
            out[i] = r
    return out


# Raw logs are the evidence the acceptance specification asks to be retained,
# and the HTML certificate is the whole deliverable: the customer receiving it
# has no access to our MAAS, so the evidence has to be IN the file. Nothing is
# linked and nothing is summarised away.
#
# The one transformation applied is collapsing carriage-return repaints. A
# 3600 s gpu-burn run leaves ~22 MB, but gpu-burn redraws its progress line in
# place about 70 times per 0.1% step it actually reports -- those are terminal
# repaints of a step already shown, not distinct log content. Collapsing them
# keeps one line per reported step (~317 KB), which is complete at the tool's
# own reporting granularity, and the count of what was collapsed is stated on
# the block.
#
# The cap below is a backstop against a pathological log, not a normal path. If
# it ever engages the block says so in the summary, where it cannot be missed.
RAW_EVIDENCE_MAX_CHARS = 4_000_000


def condense_terminal_log(text: str):
    """Collapse carriage-return repaints. Returns (text, note).

    A progress line rewritten in place with \r is one step displayed many
    times. Only the most advanced repaint of each step is kept -- that is the
    one carrying the highest counters -- and every line that is not a repaint is
    passed through untouched.
    """
    if not text or "\r" not in text:
        return text, ""

    out: list = []
    dropped = 0
    last_key = None
    for line in text.split("\n"):
        if "\r" not in line:
            out.append(line)
            last_key = None
            continue
        for seg in line.split("\r"):
            if not seg.strip():
                continue
            parts = seg.split(None, 1)
            key = parts[0] if parts else ""
            if key and key == last_key and out:
                # Same step repainted: replace, keeping the latest counters.
                out[-1] = seg
                dropped += 1
                continue
            out.append(seg)
            last_key = key

    note = ""
    if dropped:
        note = f"{dropped:,} carriage-return repaints collapsed"
    return "\n".join(out), note


def decode_b64_text(blob) -> str:
    """Text from a base64 evidence blob; empty string when absent or unusable."""
    if not blob:
        return ""
    try:
        return base64.b64decode(blob).decode("utf-8", errors="replace")
    except Exception:
        return ""


def render_log_block(title: str, text: str, note: str = "",
                     keep: str = "tail",
                     max_chars: int = RAW_EVIDENCE_MAX_CHARS) -> str:
    """One collapsible raw-log block, embedded in full.

    No link is emitted: whoever reads this certificate is not assumed to have
    access to the MAAS that produced it, so the log has to be in the file.

    keep="tail" when a log's conclusion is printed last (gpu-burn ends with its
    per-GPU OK/FAULTY summary), and only matters if the backstop cap engages.
    """
    if not text or not text.strip():
        return ""

    raw_total = len(text)
    text, condensed_note = condense_terminal_log(text)
    total = len(text)

    truncated = ""
    if total > max_chars:
        if keep == "tail":
            body = text[-max_chars:]
            truncated = f"{total - max_chars:,} earlier characters omitted"
        else:
            body = text[:max_chars]
            truncated = f"{total - max_chars:,} further characters omitted"
    else:
        body = text

    bits = [f"{total:,} characters"]
    if condensed_note:
        bits.append(condensed_note + f" from {raw_total:,}")
    if note:
        bits.append(note)
    meta = " &middot; ".join(escape(b) for b in bits)
    warn_html = (f'<span class="ev-trunc">{escape(truncated)}</span>'
                 if truncated else "")
    return (f'<details class="ev-block"><summary>{escape(title)}'
            f'<span class="ev-meta">{meta}</span>{warn_html}</summary>'
            f'<pre class="ev-pre">{escape(body)}</pre></details>')


def render_raw_evidence(inventory=None, stress=None, burnin=None,
                        script_logs=None) -> str:
    """Raw log artifacts, embedded in full and collapsed by default.

    This certificate is the deliverable. Whoever reads it is not assumed to have
    access to the MAAS that produced it, so nothing is linked out -- the
    evidence is in the file or it is not evidence.

    Two sources, preferring the more complete one per stage:

    * the full commissioning log MAAS retained for each script, which carries
      the whole nvidia-smi dumps, the entire sustained-load run and the dmesg
      window;
    * failing that (file mode, or a log MAAS could not return), the bounded
      excerpt the script embedded in its own JSON.
    """
    b = (burnin or {}).get("burn_in") or {}
    logs = script_logs or {}
    blocks = []

    stages = (
        ("Driver install", "install"),
        ("MIG / ECC configuration", "config"),
        ("Inventory \u2014 full nvidia-smi -q and topology", "inventory"),
        ("DCGM diagnostics", "stress"),
        ("Sustained burn-in", "burnin"),
    )
    for label, alias in stages:
        text = logs.get(alias) or ""
        if not text:
            continue
        name = _strip_sh(SCRIPT_ALIASES.get(alias, alias))
        blocks.append(render_log_block(
            f"{label} \u2014 complete log ({name})", text,
            note="stdout and stderr, as recorded during commissioning"))

    # Excerpts the scripts embed themselves. Shown when the corresponding full
    # log is absent, so the same content is not presented twice.
    if not logs.get("burnin"):
        tool = b.get("tool") or "load"
        blocks.append(render_log_block(
            f"{tool} output (sustained load)",
            decode_b64_text(b.get("gpu_burn_output_b64")),
            note="per-GPU OK/FAULTY verdict is at the end"))
        blocks.append(render_log_block(
            "Kernel log across the burn-in window (NVRM / Xid)",
            decode_b64_text((b.get("xid") or {}).get("dmesg_window_b64")),
            note="empty means no NVRM or Xid line was raised"))
        blocks.append(render_log_block(
            "dcgmi diag output (burn-in)",
            decode_b64_text(b.get("dcgmi_diag_b64"))))

    blocks = [x for x in blocks if x]
    if not blocks:
        return ('<div class="table-note">No raw log artifacts were captured for '
                'this run.</div>')
    return ('<div class="table-note">Complete logs, embedded in this file and '
            'collapsed by default \u2014 no external system is needed to read '
            'them. Progress lines rewritten in place by the load tool are '
            'collapsed to one line per reported step; the count is stated on '
            'each block.</div>' + "".join(blocks))


def build_acceptance_context(inventory=None, all_scripts=None) -> dict:
    """Facts the per-GPU checks need that are not in the GPU record itself.

    nvlink_present    measured by the inventory script, so an nvbandwidth skip
                      is excused on evidence rather than assumption.
    burn_script_ran   whether MAAS holds a result for the burn-in script, which
                      separates "never uploaded" from "ran and produced no
                      parseable report".
    """
    sys_info = (inventory or {}).get("system") or {}
    ctx = {"nvlink_present": sys_info.get("nvlink_present")}

    if all_scripts is None:
        ctx["burn_script_ran"] = None
        return ctx

    burn_name = _strip_sh(SCRIPT_ALIASES["burnin"])
    ctx["burn_script_ran"] = any(
        _strip_sh(str(e.get("name", ""))) == burn_name for e in all_scripts)
    return ctx


def evaluate_gpu_acceptance(gpu, stress, burn, config=None, context=None):
    """Evaluate one GPU against the acceptance criteria."""
    idx = int_or_none(gpu.get("gpu_index")) or 0
    cfg = _by_index((config or {}).get("gpu_config")).get(idx)
    b = (burn or {}).get("burn_in") or {}
    tel = _by_index(b.get("telemetry")).get(idx)
    delta = _by_index(b.get("counter_deltas")).get(idx)
    csb_delta = _by_index(
        (b.get("counters_since_baseline") or {}).get("deltas")).get(idx)

    criteria = []
    criteria += _check_identity(gpu)
    criteria += _check_memory(gpu, cfg)
    criteria += _check_pcie(gpu, delta, csb_delta, bool(b))
    criteria += _check_stress(idx, stress, _stress_results_for_gpu(stress, idx), context)
    criteria += _check_burn(idx, burn, tel, delta, context)
    criteria += _check_cumulative(idx, stress, burn)

    if any(c["status"] == "FAIL" for c in criteria):
        verdict = "REJECT"
    elif any(c["status"] in ("WARN", "N/A") for c in criteria):
        verdict = "REVIEW"
    else:
        verdict = "ACCEPT"

    return {"gpu_index": idx, "serial": gpu.get("serial"),
            "uuid": gpu.get("uuid"), "verdict": verdict, "criteria": criteria}


def acceptance_test_age_days(inventory, stress, burn, now=None):
    """Age in days of the newest test evidence, or None."""
    stamps = []
    for src in (inventory, stress, burn):
        ts = ((src or {}).get("report_metadata") or {}).get("generated_at")
        if not ts:
            continue
        try:
            stamps.append(datetime.fromisoformat(str(ts).replace("Z", "+00:00")))
        except ValueError:
            continue
    if not stamps:
        return None
    now = now or datetime.now(timezone.utc)
    return (now - max(stamps)).total_seconds() / 86400.0


# ---------------------------------------------------------------------------
# LSHW XML PARSING -- DIMM INVENTORY
# ---------------------------------------------------------------------------

def parse_lshw_dimms(lshw_xml: bytes | None) -> list[dict]:
    """Parse lshw XML to extract DIMM slot inventory.
    
    Filters out cache, system board, and memory controller nodes.
    Only returns actual populated DIMM slots.
    """
    if not lshw_xml:
        return []
    try:
        if isinstance(lshw_xml, bytes):
            idx = lshw_xml.find(b"<?xml")
            if idx > 0:
                lshw_xml = lshw_xml[idx:]
            elif idx < 0:
                idx = lshw_xml.find(b"<list")
                if idx < 0:
                    idx = lshw_xml.find(b"<node")
                if idx > 0:
                    lshw_xml = lshw_xml[idx:]
        root = ET.fromstring(lshw_xml)
    except ET.ParseError as e:
        print(f"Warning: lshw XML parse error: {e}", file=sys.stderr)
        if lshw_xml:
            print(f"  XML starts with: {lshw_xml[:200]!r}", file=sys.stderr)
        return []

    dimms = []
    for node in root.iter("node"):
        node_id = node.get("id", "")
        node_class = node.get("class", "")
        desc = _xml_text(node, "description", "").lower()
        slot = _xml_text(node, "slot", "")

        # --- EXCLUDE non-DIMM memory nodes ---
        # Cache (L1, L2, L3)
        if "cache" in desc:
            continue
        # System board / motherboard aggregate
        if "system board" in desc or "motherboard" in desc:
            continue
        # Parent memory controller nodes (have child <node> elements)
        if node.find("node") is not None:
            continue

        # --- INCLUDE only actual DIMM slots ---
        is_bank = node_id.startswith("bank:")
        is_mem_slot = (
            node_class == "memory"
            and node.find("slot") is not None
            and node.find("size") is not None
        )
        is_mem_numbered = (
            node_class == "memory"
            and ":" in node_id
            and node.find("size") is not None
        )

        if not (is_bank or is_mem_slot or is_mem_numbered):
            continue

        # Extra validation: slot name should look like a DIMM slot
        # (contains DIMM, CPU, MEM, PROC, or similar)
        slot_upper = slot.upper()
        if slot and not any(kw in slot_upper for kw in
                           ("DIMM", "CPU", "MEM", "PROC", "BANK", "SLOT",
                            "CHANNEL", "P0_", "P1_", "NODE")):
            continue

        vendor = _xml_text(node, "vendor", "")
        product = _xml_text(node, "product", "")
        serial = _xml_text(node, "serial", "")

        # Size
        size_el = node.find("size")
        size_gb = 0
        if size_el is not None and size_el.text:
            try:
                raw_size = int(size_el.text)
                units = size_el.get("units", "bytes")
                if units == "bytes":
                    size_gb = raw_size / (1024 ** 3)
                elif units == "KiB":
                    size_gb = raw_size / (1024 ** 2)
                elif units == "MiB":
                    size_gb = raw_size / 1024
                elif units == "GiB":
                    size_gb = raw_size
                else:
                    size_gb = raw_size / (1024 ** 3)
            except (ValueError, TypeError):
                pass

        # Speed: extract from multiple sources
        clock_mhz = 0
        raw_desc = _xml_text(node, "description", "")

        # Strategy 1: Parse speed from description field
        # e.g. "DDR5 Synchronous Registered (Buffered) 4800 MHz (0.2 ns)"
        # e.g. "DDR4 Synchronous 3200 MHz"
        desc_speed = re.search(r'(\d{3,5})\s*MHz', raw_desc)
        if desc_speed:
            speed_val = int(desc_speed.group(1))
            if speed_val >= 800:  # Plausible DDR speed (DDR3-800 and above)
                clock_mhz = speed_val

        # Strategy 2: Check <configuration><setting> elements (rare but possible)
        if not clock_mhz:
            config = node.find("configuration")
            if config is not None:
                for setting in config.findall("setting"):
                    sid = (setting.get("id", "") or "").lower()
                    sval = setting.get("value", "") or ""
                    if sid in ("speed", "configured_speed", "configured_clock_speed"):
                        try:
                            num = int("".join(c for c in sval if c.isdigit()))
                            if num > 100000:
                                clock_mhz = num // 1_000_000
                            elif num > 0:
                                clock_mhz = num
                        except (ValueError, TypeError):
                            pass

        # Strategy 3: <clock> element (bus clock — use only if nothing else works)
        if not clock_mhz:
            clock_el = node.find("clock")
            if clock_el is not None and clock_el.text:
                try:
                    hz = int(clock_el.text)
                    clock_mhz = hz // 1_000_000
                except (ValueError, TypeError):
                    pass

        # Width
        width = 0
        width_el = node.find("width")
        if width_el is not None and width_el.text:
            try:
                width = int(width_el.text)
                units = width_el.get("units", "bits")
                if units == "bytes":
                    width *= 8
            except (ValueError, TypeError):
                pass

        if size_gb > 0:  # Only include populated slots
            dimms.append({
                "slot": slot,
                "description": _xml_text(node, "description", ""),
                "size_gb": round(size_gb, 1) if size_gb else 0,
                "vendor": vendor,
                "product": product,
                "serial": serial,
                "clock_mhz": clock_mhz,
                "width_bits": width,
            })

    if not dimms:
        mem_nodes = [(n.get("id",""), _xml_text(n, "description", ""))
                     for n in root.iter("node") if n.get("class") == "memory"]
        print(f"  lshw debug: {len(mem_nodes)} memory-class nodes: "
              f"{mem_nodes[:10]}", file=sys.stderr)

    return dimms


def parse_lshw_storage(lshw_xml: bytes | None) -> list[dict]:
    """Parse lshw XML for storage controller and disk details."""
    if not lshw_xml:
        return []
    try:
        root = ET.fromstring(lshw_xml)
    except ET.ParseError:
        return []

    disks = []
    for node in root.iter("node"):
        node_id = node.get("id", "")
        node_class = node.get("class", "")
        if node_class != "disk" and not node_id.startswith("disk"):
            continue

        size_el = node.find("size")
        size_gb = 0
        if size_el is not None and size_el.text:
            try:
                raw = int(size_el.text)
                units = size_el.get("units", "bytes")
                if units == "bytes":
                    size_gb = raw / (1000 ** 3)  # storage uses SI
                else:
                    size_gb = raw
            except (ValueError, TypeError):
                pass

        logicalname = _xml_text(node, "logicalname", "")
        if isinstance(logicalname, list):
            logicalname = logicalname[0] if logicalname else ""

        disks.append({
            "device": logicalname,
            "product": _xml_text(node, "product", ""),
            "vendor": _xml_text(node, "vendor", ""),
            "serial": _xml_text(node, "serial", ""),
            "size_gb": round(size_gb, 1),
            "description": _xml_text(node, "description", ""),
        })

    return disks


def parse_lshw_nics(lshw_xml: bytes | None) -> list[dict]:
    """Parse lshw XML for network device product names, mapped by MAC address.
    
    Returns list of dicts with: mac, product, vendor, description, businfo
    """
    if not lshw_xml:
        return []
    try:
        if isinstance(lshw_xml, bytes):
            idx = lshw_xml.find(b"<?xml")
            if idx > 0:
                lshw_xml = lshw_xml[idx:]
            elif idx < 0:
                idx = lshw_xml.find(b"<list")
                if idx < 0:
                    idx = lshw_xml.find(b"<node")
                if idx > 0:
                    lshw_xml = lshw_xml[idx:]
        root = ET.fromstring(lshw_xml)
    except ET.ParseError:
        return []

    nics = []
    for node in root.iter("node"):
        node_class = node.get("class", "")
        if node_class != "network":
            continue

        # Get MAC from <serial> (lshw uses serial for MAC on NICs)
        mac = _xml_text(node, "serial", "").lower()
        product = _xml_text(node, "product", "")
        vendor = _xml_text(node, "vendor", "")
        desc = _xml_text(node, "description", "")
        businfo = _xml_text(node, "businfo", "")  # e.g. "pci@0000:e5:00.0"
        logicalname = _xml_text(node, "logicalname", "")

        if product or vendor:
            nics.append({
                "mac": mac,
                "product": product,
                "vendor": vendor,
                "description": desc,
                "businfo": businfo,
                "logicalname": logicalname,
            })

    return nics


def enrich_nics_from_lshw(nics: list[dict], lshw_nics: list[dict]) -> list[dict]:
    """Enrich MAAS NIC list with product names from lshw, matched by MAC."""
    if not lshw_nics:
        return nics

    # Build MAC -> lshw NIC lookup
    by_mac = {}
    for ln in lshw_nics:
        mac = ln.get("mac", "").lower().strip()
        if mac:
            by_mac[mac] = ln

    for nic in nics:
        mac = (nic.get("mac", "") or "").lower().strip()
        lshw_nic = by_mac.get(mac)
        if lshw_nic:
            # Only override if MAAS didn't have product info
            if not nic.get("product"):
                nic["product"] = lshw_nic.get("product", "")
            if not nic.get("vendor") or nic["vendor"] == "--":
                nic["vendor"] = lshw_nic.get("vendor", "")

    return nics


def _xml_text(el, tag: str, default: str = "") -> str:
    child = el.find(tag)
    if child is not None and child.text:
        return child.text.strip()
    return default


def parse_machine_resources_dimms(resources: dict | None) -> list[dict]:
    """Parse DIMM info from 40-maas-01-machine-resources JSON output.
    
    The machine-resources output has memory info under .memory.nodes[]
    with per-NUMA-node DIMM details. Filters out cache and system-board entries.
    """
    if not resources:
        return []
    dimms = []

    # Keywords that indicate non-DIMM memory entries
    _EXCLUDE_KEYWORDS = ("cache", "l1", "l2", "l3", "system board", "motherboard")

    def _is_real_dimm(entry: dict) -> bool:
        """Check if this entry looks like an actual DIMM slot."""
        slot = str(entry.get("slot", entry.get("locator", ""))).lower()
        desc = str(entry.get("type", entry.get("description", ""))).lower()
        for kw in _EXCLUDE_KEYWORDS:
            if kw in slot or kw in desc:
                return False
        size = entry.get("size", 0)
        if not size or size <= 0:
            return False
        return True

    try:
        memory = resources.get("memory", {})
        # machine-resources stores DIMMs under memory.nodes[].dimms[] or memory.dimms[]
        nodes = memory.get("nodes", [])
        if nodes:
            for node in nodes:
                for dimm in node.get("dimms", []):
                    if not _is_real_dimm(dimm):
                        continue
                    size_mb = dimm.get("size", 0)  # in MiB
                    dimms.append({
                        "slot": dimm.get("slot", ""),
                        "description": dimm.get("type", ""),
                        "size_gb": round(size_mb / 1024, 1) if size_mb else 0,
                        "vendor": dimm.get("vendor", ""),
                        "product": dimm.get("part_number", ""),
                        "serial": dimm.get("serial", ""),
                        "clock_mhz": dimm.get("configured_speed", dimm.get("speed", 0)),
                        "width_bits": dimm.get("data_width", 0),
                    })
        # Alternate flat layout
        if not dimms:
            for dimm in memory.get("dimms", []):
                if not _is_real_dimm(dimm):
                    continue
                size_mb = dimm.get("size", 0)
                dimms.append({
                    "slot": dimm.get("slot", dimm.get("locator", "")),
                    "description": dimm.get("type", ""),
                    "size_gb": round(size_mb / 1024, 1) if size_mb else 0,
                    "vendor": dimm.get("vendor", ""),
                    "product": dimm.get("part_number", ""),
                    "serial": dimm.get("serial", ""),
                    "clock_mhz": dimm.get("configured_speed", dimm.get("speed", 0)),
                    "width_bits": dimm.get("data_width", 0),
                })
    except Exception as e:
        print(f"Warning: machine-resources DIMM parse error: {e}", file=sys.stderr)
    return [d for d in dimms if d.get("size_gb", 0) > 0]


def parse_dmidecode_dimm_speeds(dmidecode_text: str | None) -> dict:
    """Parse dmidecode output to extract DIMM speed per slot.
    
    Returns dict mapping slot name -> configured speed in MT/s.
    Parses SMBIOS Type 17 (Memory Device) entries.
    """
    if not dmidecode_text:
        return {}
    
    speeds = {}
    current_slot = ""
    current_speed = 0
    in_memory_device = False
    
    for line in dmidecode_text.split("\n"):
        stripped = line.strip()
        
        # Detect start of Type 17 block
        if "Memory Device" in stripped and not "Mapped" in stripped:
            in_memory_device = True
            current_slot = ""
            current_speed = 0
            continue
        
        # Detect end of block (new Handle or empty section)
        if in_memory_device and (stripped.startswith("Handle ") or 
                                  (stripped == "" and current_slot)):
            if current_slot and current_speed:
                speeds[current_slot] = current_speed
            if stripped.startswith("Handle "):
                in_memory_device = False
            continue
        
        if not in_memory_device:
            continue
            
        # Parse fields
        if stripped.startswith("Locator:"):
            current_slot = stripped.split(":", 1)[1].strip()
        elif stripped.startswith("Configured Memory Speed:") or stripped.startswith("Configured Clock Speed:"):
            val = stripped.split(":", 1)[1].strip()
            try:
                # Value like "4800 MT/s" or "4800 MHz"
                num = int("".join(c for c in val.split()[0] if c.isdigit()))
                if num > 0:
                    current_speed = num
            except (ValueError, IndexError):
                pass
        elif stripped.startswith("Speed:") and not current_speed:
            # Fallback to Speed if Configured Speed not found
            val = stripped.split(":", 1)[1].strip()
            try:
                num = int("".join(c for c in val.split()[0] if c.isdigit()))
                if num > 0:
                    current_speed = num
            except (ValueError, IndexError):
                pass
    
    # Flush last entry
    if current_slot and current_speed:
        speeds[current_slot] = current_speed
    
    return speeds


def enrich_dimm_speeds(dimms: list[dict], dmidecode_speeds: dict) -> list[dict]:
    """Merge dmidecode speed data into DIMM list from lshw/machine-resources."""
    if not dmidecode_speeds:
        return dimms
    for dimm in dimms:
        slot = dimm.get("slot", "")
        if slot in dmidecode_speeds:
            dimm["clock_mhz"] = dmidecode_speeds[slot]
        else:
            # Try partial match (MAAS sometimes trims slot names)
            for dmi_slot, speed in dmidecode_speeds.items():
                if slot and (slot in dmi_slot or dmi_slot in slot):
                    dimm["clock_mhz"] = speed
                    break
    return dimms


def extract_storage_details(machine: dict, machine_resources: dict | None) -> list[dict]:
    """Extract storage devices including RAID member disks.
    
    Combines MAAS block devices with any additional disk info
    from machine-resources (which may see behind RAID controllers).
    """
    devs = []
    seen_serials = set()
    
    # 1. MAAS physical block devices (top-level visible disks)
    for bd in machine.get("physicalblockdevice_set", []):
        size_bytes = bd.get("size", 0)
        serial = bd.get("serial", "")
        devs.append({
            "name": bd.get("name", "?"),
            "model": bd.get("model", ""),
            "serial": serial,
            "size_gb": round(size_bytes / (1000 ** 3), 1) if size_bytes else 0,
            "firmware": bd.get("firmware_version", ""),
            "numa_node": bd.get("numa_node", -1),
            "type": "block",
            "raid_member": False,
        })
        if serial:
            seen_serials.add(serial)
    
    # 2. MAAS RAID sets — extract member disk info
    for raid in machine.get("raid_set", machine.get("raids", [])):
        raid_name = raid.get("name", "")
        raid_level = raid.get("level", "")
        for member in raid.get("devices", []) + raid.get("spare_devices", []):
            serial = member.get("serial", "")
            if serial and serial in seen_serials:
                continue
            size_bytes = member.get("size", 0)
            devs.append({
                "name": member.get("name", "?"),
                "model": member.get("model", ""),
                "serial": serial,
                "size_gb": round(size_bytes / (1000 ** 3), 1) if size_bytes else 0,
                "firmware": member.get("firmware_version", ""),
                "numa_node": member.get("numa_node", -1),
                "type": "raid_member",
                "raid_member": True,
                "raid_name": raid_name,
                "raid_level": raid_level,
            })
            if serial:
                seen_serials.add(serial)
    
    # 3. machine-resources storage — may have disks behind RAID controllers
    if machine_resources:
        for disk in machine_resources.get("storage", {}).get("disks", []):
            serial = disk.get("serial", disk.get("serial_number", ""))
            if serial and serial in seen_serials:
                continue
            name = disk.get("name", disk.get("device_name", ""))
            size_bytes = disk.get("size", 0)
            devs.append({
                "name": name,
                "model": disk.get("model", disk.get("model_name", "")),
                "serial": serial,
                "size_gb": round(size_bytes / (1000 ** 3), 1) if size_bytes else 0,
                "firmware": disk.get("firmware_version", disk.get("firmware", "")),
                "numa_node": disk.get("numa_node", -1),
                "type": disk.get("type", "disk"),
                "raid_member": False,
            })
            if serial:
                seen_serials.add(serial)
    
    return devs


# ---------------------------------------------------------------------------
# MAAS MACHINE DATA EXTRACTION
# ---------------------------------------------------------------------------

def parse_machine_resources_cpu(machine_resources: dict | None) -> dict:
    """Physical CPU topology from MAAS machine-resources.

    MAAS surfaces two different numbers and calls both "cores": the machine
    summary shows logical CPUs (256 on a 2x64C/256T host) and each NUMA node
    lists logical cpu ids. The nested resources blob is the only place the
    physical layout survives:

        cpu.sockets[].cores[].threads[]

    so physical cores = sum of len(cores) over sockets, and threads = sum of
    len(threads) over those cores.

    Returns {} when the blob is absent or shaped differently, so callers can
    fall back rather than assert a number they cannot support.
    """
    if not machine_resources:
        return {}
    cpu = None
    for container in (machine_resources.get("resources") or {}, machine_resources):
        candidate = (container or {}).get("cpu")
        if isinstance(candidate, dict) and candidate.get("sockets"):
            cpu = candidate
            break
    if not cpu:
        return {}

    sockets = cpu.get("sockets") or []
    if not isinstance(sockets, list):
        return {}

    cores = 0
    threads = 0
    for sock in sockets:
        sock_cores = (sock or {}).get("cores") or []
        if not isinstance(sock_cores, list):
            continue
        cores += len(sock_cores)
        for core in sock_cores:
            core_threads = (core or {}).get("threads") or []
            threads += len(core_threads) if isinstance(core_threads, list) else 0

    if not cores:
        return {}
    out = {"sockets": len(sockets), "cores": cores}
    if threads:
        out["threads"] = threads
    return out


def cores_from_cpu_model(model: str, sockets: int) -> int:
    """Physical cores inferred from the vendor's own part name.

    Last resort, but a sound one: "AMD EPYC 9554 64-Core Processor" states the
    per-socket core count, and MAAS prints that string right beside the logical
    count it labels "cores".
    """
    if not model or sockets < 1:
        return 0
    m = re.search(r"(\d+)[- ]Core", str(model), re.IGNORECASE)
    return int(m.group(1)) * sockets if m else 0


def extract_pci_devices(machine_resources: dict | None) -> dict:
    """Extract PCI devices from machine-resources JSON, grouped by category.
    
    Returns dict with 'network' and 'storage' lists of PCI devices.
    Each device has: vendor, vendor_id, product, product_id, driver, numa_node, pci_address
    """
    result = {"network": [], "storage": []}
    if not machine_resources:
        return result

    log = lambda m: print(f"[maas-report]   pci: {m}", file=sys.stderr)
    top_keys = list(machine_resources.keys())
    log(f"machine-resources top-level keys: {top_keys}")

    all_pci = []

    # Strategy 1: Categorized arrays (network, storage, gpu)
    for dev in machine_resources.get("network", []):
        d = dict(dev) if isinstance(dev, dict) else {}
        d["_category"] = "network"
        all_pci.append(d)
    for dev in machine_resources.get("storage", []):
        d = dict(dev) if isinstance(dev, dict) else {}
        d["_category"] = "storage"
        all_pci.append(d)

    # Strategy 2: Flat pci array with class-based categorization
    for dev in machine_resources.get("pci", []):
        d = dict(dev) if isinstance(dev, dict) else {}
        if d.get("_category"):
            continue
        pci_class = str(d.get("class", d.get("pci_class", d.get("class_id", "")))).lower()
        driver = str(d.get("driver", "")).lower()
        product = str(d.get("product", d.get("device", ""))).lower()
        vendor = str(d.get("vendor", d.get("vendor_name", ""))).lower()
        
        # Categorize by class code, driver name, or product keywords
        if ("network" in pci_class or "ethernet" in pci_class or pci_class.startswith("02")
            or driver in ("mlx5_core", "i40e", "ice", "bnxt_en", "igb", "ixgbe", "e1000")
            or "ethernet" in product or "connectx" in product or "network" in product):
            d["_category"] = "network"
        elif ("storage" in pci_class or "mass" in pci_class or "raid" in pci_class or
              "nvme" in pci_class or "sata" in pci_class or "sas" in pci_class or
              pci_class.startswith("01")
              or driver in ("nvme", "megaraid_sas", "mpt3sas", "ahci")
              or "nvme" in product or "raid" in product or "sas" in product or "ssd" in product):
            d["_category"] = "storage"
        all_pci.append(d)

    # Strategy 3: Nested under resources.pci or resources.network etc
    res = machine_resources.get("resources", {})
    if isinstance(res, dict):
        for dev in res.get("pci", []):
            d = dict(dev) if isinstance(dev, dict) else {}
            if not d.get("_category"):
                # Use same classification as above
                pci_class = str(d.get("class", d.get("class_id", ""))).lower()
                driver = str(d.get("driver", "")).lower()
                if pci_class.startswith("02") or driver in ("mlx5_core", "i40e", "ice", "bnxt_en"):
                    d["_category"] = "network"
                elif pci_class.startswith("01") or driver in ("nvme", "megaraid_sas", "mpt3sas"):
                    d["_category"] = "storage"
            all_pci.append(d)

    if not all_pci:
        log(f"no PCI device arrays found in machine-resources")
        # Dump a sample of what IS there for debugging
        for k in top_keys[:10]:
            val = machine_resources[k]
            if isinstance(val, list):
                log(f"  key '{k}': list of {len(val)} items")
                if val and isinstance(val[0], dict):
                    log(f"    sample keys: {list(val[0].keys())[:10]}")
            elif isinstance(val, dict):
                log(f"  key '{k}': dict with keys {list(val.keys())[:10]}")

    for dev in all_pci:
        cat = dev.get("_category")
        if cat not in ("network", "storage"):
            continue
        # Normalize field names across MAAS versions
        entry = {
            "vendor": dev.get("vendor", dev.get("vendor_name", dev.get("subvendor", ""))),
            "vendor_id": dev.get("vendor_id", ""),
            "product": dev.get("product", dev.get("product_name", dev.get("device", ""))),
            "product_id": dev.get("product_id", dev.get("device_id", "")),
            "driver": dev.get("driver", dev.get("driver_name", dev.get("module", ""))),
            "numa_node": dev.get("numa_node", dev.get("numa", -1)),
            "pci_address": dev.get("pci_address", dev.get("address", dev.get("bus_address", dev.get("id", "")))),
        }
        # Clean up vendor_id/product_id: keep only hex IDs
        for id_field in ("vendor_id", "product_id"):
            val = str(entry[id_field]).strip()
            # If it's a full vendor name instead of an ID, clear it
            if len(val) > 6 and not all(c in "0123456789abcdefABCDEF" for c in val):
                entry[id_field] = ""
        result[cat].append(entry)

    return result


def extract_network_interfaces(machine: dict) -> list[dict]:
    """Extract physical NIC devices from MAAS machine detail (fallback for no machine-resources)."""
    nics = []
    for iface in machine.get("interface_set", []):
        if iface.get("type") == "physical":
            nics.append({
                "name": iface.get("name", "?"),
                "mac": iface.get("mac_address", ""),
                "vendor": iface.get("vendor", ""),
                "product": iface.get("product", ""),
                "link_speed": iface.get("link_speed", 0),  # Mbps
                "interface_speed": iface.get("interface_speed", 0),
                "sriov_max_vf": iface.get("sriov_max_vf", 0),
                "firmware": iface.get("firmware_version", ""),
                "numa_node": iface.get("numa_node", -1),
            })
    return nics


def extract_block_devices(machine: dict) -> list[dict]:
    """Extract storage devices from MAAS machine detail."""
    devs = []
    for bd in machine.get("physicalblockdevice_set", []):
        size_bytes = bd.get("size", 0)
        devs.append({
            "name": bd.get("name", "?"),
            "model": bd.get("model", ""),
            "serial": bd.get("serial", ""),
            "size_gb": round(size_bytes / (1000 ** 3), 1) if size_bytes else 0,
            "firmware": bd.get("firmware_version", ""),
            "numa_node": bd.get("numa_node", -1),
            "block_size": bd.get("block_size", 0),
        })
    return devs


def extract_numa_topology(machine: dict) -> list[dict]:
    """Extract NUMA node info from MAAS machine detail."""
    nodes = []
    for n in machine.get("numanode_set", []):
        nodes.append({
            "index": n.get("index", -1),
            "memory_mb": n.get("memory", 0),
            "cores": n.get("cores", []),
        })
    return sorted(nodes, key=lambda x: x["index"])


# ---------------------------------------------------------------------------
# FETCH ALL DATA FROM MAAS
# ---------------------------------------------------------------------------

def fetch_from_maas(hostname: str, maas_url: str, api_key: str) -> dict:
    """
    Connect to MAAS, resolve hostname, fetch everything needed for the report.
    Returns a dict with all data sources.
    """
    log(f"Connecting to MAAS at {maas_url}")
    client = MAASClient(maas_url, api_key)

    # Step 1: Resolve hostname
    log(f"Resolving hostname: {hostname}")
    machine = client.resolve_hostname(hostname)
    system_id = machine["system_id"]
    fqdn = machine.get("fqdn", hostname)
    log(f"  -> system_id={system_id}, fqdn={fqdn}, status={machine.get('status_name','?')}")

    # Step 2: Fetch GPU commissioning scripts
    log("Fetching GPU commissioning script outputs...")
    install_data = client.get_script_json(system_id, SCRIPT_ALIASES["install"])
    if install_data:
        log(f"  90-install: loaded ({install_data.get('verdict', {}).get('overall', '?')})")
    else:
        log("  90-install: not found or no JSON output")

    inventory_data = client.get_script_json(system_id, SCRIPT_ALIASES["inventory"])
    if inventory_data:
        log(f"  98-inventory: loaded ({inventory_data.get('verdict', {}).get('overall', '?')})")
    else:
        log("  98-inventory: not found or no JSON output")

    config_data = client.get_script_json(system_id, SCRIPT_ALIASES["config"])
    if config_data:
        log(f"  91-config: loaded ({config_data.get('verdict', {}).get('overall', '?')})")
    else:
        log("  91-config: not found (optional)")

    burnin_data = client.get_script_json(system_id, SCRIPT_ALIASES["burnin"])
    if burnin_data:
        log(f"  92-burn-in: loaded ({burnin_data.get('verdict', {}).get('overall', '?')})")
    else:
        log("  92-burn-in: not found (optional -- no sustained-load evidence)")

    stress_data = client.get_script_json(system_id, SCRIPT_ALIASES["stress"])
    if stress_data:
        log(f"  99-stress: loaded ({stress_data.get('verdict', {}).get('overall', '?')})")
    else:
        log("  99-stress: not found or no JSON output")

    # Step 3: Fetch full machine hardware details
    log("Fetching machine hardware details...")
    details = client.get_machine_details(system_id)
    hw_info = details.get("hardware_info", {})
    log(f"  Platform: {hw_info.get('system_vendor', '?')} {hw_info.get('system_product', '?')}")
    log(f"  CPU: {hw_info.get('cpu_model', '?')} ({details.get('cpu_count', '?')} cores)")
    log(f"  RAM: {details.get('memory', '?')} MiB")

    nics = extract_network_interfaces(details)
    log(f"  NICs: {len(nics)} physical devices")

    numa_nodes = extract_numa_topology(details)
    log(f"  NUMA: {len(numa_nodes)} nodes")

    # Step 4: Fetch lshw for DIMM inventory + NIC product names
    log("Fetching lshw data...")
    lshw_xml = client.get_machine_lshw(system_id)
    dimms = []
    if lshw_xml:
        log(f"  lshw XML: {len(lshw_xml)} bytes")
        dimms = parse_lshw_dimms(lshw_xml)
        if dimms:
            log(f"  DIMMs: {len(dimms)} slots populated")
            # Debug: show first DIMM speed for verification
            sample = dimms[0]
            log(f"    sample: {sample['slot']} {sample['clock_mhz']} {'MT/s' if sample['clock_mhz'] >= 1000 else 'MHz'}")
        else:
            log(f"  DIMMs: 0 (XML parsed OK but no DIMM nodes matched)")

        # Enrich NIC list with product names from lshw
        lshw_nics = parse_lshw_nics(lshw_xml)
        if lshw_nics:
            log(f"  lshw NICs: {len(lshw_nics)} network devices")
            for ln in lshw_nics[:3]:
                log(f"    {ln.get('product','?')} ({ln.get('mac','?')})")
            nics = enrich_nics_from_lshw(nics, lshw_nics)
        else:
            log("  lshw NICs: 0 network nodes found")
    else:
        log("  lshw XML: not available from any strategy")

    # Step 4b: Fetch machine-resources for additional hardware detail
    log("Fetching machine-resources data...")
    machine_resources = client.get_machine_resources(system_id)
    pci_devices = {"network": [], "storage": []}
    cpu_topology: dict = {}
    if machine_resources:
        log(f"  machine-resources: loaded ({len(machine_resources)} top-level keys)")
        # Enrich DIMM data from machine-resources if lshw failed
        if not dimms:
            dimms = parse_machine_resources_dimms(machine_resources)
            if dimms:
                log(f"  DIMMs (from machine-resources): {len(dimms)} slots")
        # Extract PCI devices
        pci_devices = extract_pci_devices(machine_resources)
        cpu_topology = parse_machine_resources_cpu(machine_resources)
        if cpu_topology:
            log(f"  CPU topology: {cpu_topology.get('sockets')} socket(s), "
                f"{cpu_topology.get('cores')} physical cores, "
                f"{cpu_topology.get('threads', '?')} threads")
        log(f"  PCI network devices: {len(pci_devices['network'])}")
        log(f"  PCI storage devices: {len(pci_devices['storage'])}")
    else:
        log("  machine-resources: not available")

    # Step 4c: Fetch dmidecode for accurate DIMM speeds
    log("Fetching dmidecode for DIMM speed data...")
    dmidecode_text = None
    # Try multiple script names for dmidecode
    for dmi_name in ["dmidecode", "00-maas-06-get-fruid-data", "maas-dmidecode",
                     "maas-get-fruid-api-data", "maas-support-info"]:
        text = client.get_commissioning_script_stdout(system_id, dmi_name)
        if text and "Memory Device" in text and "Configured" in text:
            dmidecode_text = text
            log(f"  dmidecode: found via '{dmi_name}'")
            break
    if not dmidecode_text:
        # Scan ALL commissioning results for any script containing dmidecode output
        try:
            all_results = client.get_commissioning_results(system_id)
            for result in all_results.get("results", []):
                name = result.get("name", "")
                stdout_b64 = result.get("stdout", "")
                if not stdout_b64:
                    continue
                text = base64.b64decode(stdout_b64).decode("utf-8", errors="replace")
                if "Memory Device" in text and "Configured" in text:
                    dmidecode_text = text
                    log(f"  dmidecode: found embedded in '{name}'")
                    break
        except Exception:
            pass
    
    if dmidecode_text and "Memory Device" in dmidecode_text:
        dmi_speeds = parse_dmidecode_dimm_speeds(dmidecode_text)
        if dmi_speeds:
            sample_speed = next(iter(dmi_speeds.values()))
            log(f"  dmidecode: {len(dmi_speeds)} DIMM speeds found (sample: {sample_speed} MT/s)")
            dimms = enrich_dimm_speeds(dimms, dmi_speeds)
        else:
            log("  dmidecode: parsed but no speed data found")
    else:
        log("  dmidecode: not available")

    # Step 4d: Extract storage details (including RAID member disks)
    storage_devs = extract_storage_details(details, machine_resources)
    block_devs = extract_block_devices(details)  # Legacy fallback
    log(f"  Storage: {len(storage_devs)} devices ({len(block_devs)} block devices)")
    raid_members = [d for d in storage_devs if d.get("raid_member")]
    if raid_members:
        log(f"  RAID members: {len(raid_members)}")
    # Debug: check what MAAS storage keys exist
    storage_keys = [k for k in details.keys() if any(w in k.lower() for w in 
                    ("storage", "raid", "block", "disk", "virtual", "bcache", "volume", "filesystem"))]
    if storage_keys:
        log(f"  MAAS storage-related keys: {storage_keys}")
    raid_set = details.get("raid_set", details.get("raids", []))
    vbd_set = details.get("virtualblockdevice_set", [])
    if raid_set:
        log(f"  MAAS raid_set: {len(raid_set)} entries")
        for r in raid_set[:3]:
            log(f"    {r.get('name','?')} ({r.get('level','?')}): {len(r.get('devices',[]))} members")
    else:
        log("  MAAS raid_set: empty (hardware RAID not visible to MAAS)")
    if vbd_set:
        log(f"  MAAS virtual block devices: {len(vbd_set)}")
    # Physical block device detail
    for bd in details.get("physicalblockdevice_set", [])[:5]:
        model = bd.get("model", "?")
        name = bd.get("name", "?")
        tags = bd.get("tags", [])
        log(f"    {name}: {model} tags={tags}")
    # Check machine-resources storage structure
    if machine_resources:
        stor = machine_resources.get("storage", {})
        if isinstance(stor, dict):
            log(f"  machine-resources storage keys: {list(stor.keys())[:10]}")
            disks = stor.get("disks", [])
            if disks:
                log(f"  machine-resources disks: {len(disks)}")
                for d in disks[:3]:
                    if isinstance(d, dict):
                        log(f"    {d.get('device_name', d.get('name','?'))}: {d.get('model', d.get('model_name','?'))} {d.get('type','?')}")
        elif isinstance(stor, list):
            log(f"  machine-resources storage: list of {len(stor)} items")
            for d in stor[:3]:
                if isinstance(d, dict):
                    log(f"    keys: {list(d.keys())[:8]}")

    # Step 5: List all commissioning scripts (for metadata)
    log("Fetching commissioning script list...")
    # Full per-script logs. The scripts put their JSON on stdout and every log
    # line, nvidia-smi dump and diagnostic artifact on stderr, so "all" is what
    # carries the evidence the specification asks to be retained. This is the
    # first caller of get_script_stdout_raw's output parameter.
    log("Fetching full commissioning logs for raw evidence...")
    script_logs: dict = {}
    for alias, script_name in SCRIPT_ALIASES.items():
        text = client.get_script_stdout_raw(system_id, script_name, output="all")
        if text:
            script_logs[alias] = text
            log(f"  {_strip_sh(script_name)}: {len(text):,} chars")
    if not script_logs:
        log("  no per-script logs retrievable -- embedded excerpts will be used")

    all_scripts = client.get_all_commissioning_scripts(system_id)
    log(f"  {len(all_scripts)} total commissioning scripts")

    return {
        "install": install_data,
        "inventory": inventory_data,
        "stress": stress_data,
        "config": config_data,
        "burnin": burnin_data,
        "machine": details,
        "hardware_info": hw_info,
        "nics": nics,
        "block_devices": block_devs,
        "storage_devices": storage_devs,
        "pci_devices": pci_devices,
        "numa_nodes_maas": numa_nodes,
        "dimms": dimms,
        "all_scripts": all_scripts,
        "cpu_topology": cpu_topology,
        "script_logs": script_logs,
        "system_id": system_id,
        "hostname": hostname,
        "fqdn": fqdn,
    }


# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------

_quiet = False

def log(msg: str):
    if not _quiet:
        print(f"[maas-report] {msg}", file=sys.stderr)


# ---------------------------------------------------------------------------
# SHARED HTML HELPERS (from v2.2)
# ---------------------------------------------------------------------------

def badge(verdict: str) -> str:
    vv = verdict.upper()
    cls = {"PASS": "pass", "WARN": "warn", "FAIL": "fail"}.get(vv, "na")
    return f'<span class="badge badge-{cls}">{vv}</span>'


def fmt_dur(s) -> str:
    if not s:
        return "--"
    s = int(s)
    m, sec = divmod(s, 60)
    return f"{m}m {sec}s" if m else f"{sec}s"


def v(val, unit="", na="--"):
    if val is None or val == "" or val == "null":
        return f'<span class="dim">{na}</span>'
    if isinstance(val, float):
        return f"{val:.1f}{unit}"
    return f"{val}{unit}"


def ecc_summary(ecc: dict) -> str:
    parts = []
    for key, label in [
        ("corrected_volatile", "CV"),
        ("uncorrected_volatile", "UV"),
        ("corrected_aggregate", "CA"),
        ("uncorrected_aggregate", "UA"),
        ("retired_pages_sbit", "RS"),
        ("retired_pages_dbit", "RD"),
    ]:
        val = ecc.get(key)
        if val is not None and isinstance(val, (int, float)) and val > 0:
            cls = "alert" if "uncorrected" in key or "dbit" in key else "warn"
            parts.append(f'<span class="{cls}">{label}:{val:,}</span>')
    if parts:
        return " ".join(parts)
    return '<span class="dim">Clean</span>'


def remapped_rows_summary(gpu: dict) -> str:
    rr = gpu.get("remapped_rows")
    if not rr:
        return '<span class="dim">N/A</span>'
    parts = []
    uce = rr.get("uncorrectable")
    ce = rr.get("correctable")
    if uce is not None and isinstance(uce, (int, float)) and uce > 0:
        parts.append(f'<span class="alert">UCE:{uce}</span>')
    if ce is not None and isinstance(ce, (int, float)) and ce > 0:
        parts.append(f'CE:{ce}')
    if rr.get("failure_occurred") is True:
        parts.append('<span class="alert">FAILURE</span>')
    elif rr.get("pending") is True:
        parts.append('<span class="warn">PENDING</span>')
    if not parts:
        return '<span class="dim">None</span>'
    return " ".join(parts)


def bank_availability_summary(gpu: dict) -> str:
    ba = gpu.get("bank_remap_availability")
    if not ba:
        return '<span class="dim">N/A</span>'
    total = sum(v for v in ba.values() if isinstance(v, (int, float)))
    if total == 0:
        return '<span class="dim">N/A</span>'
    parts = []
    for key, label in [("max", "Max"), ("high", "High"), ("partial", "Part"), ("low", "Low"), ("none", "None")]:
        val = ba.get(key)
        if val is not None and isinstance(val, (int, float)) and val > 0:
            cls = "alert" if key == "none" else ("warn" if key == "low" else "")
            if cls:
                parts.append(f'<span class="{cls}">{label}:{val}</span>')
            else:
                parts.append(f'{label}:{val}')
    return " ".join(parts) if parts else '<span class="dim">N/A</span>'


def pcie_str(g: dict) -> str:
    gen = g.get("pcie_gen_max", g.get("pcie_gen_current", "?"))
    w_max = g.get("pcie_width_max", g.get("pcie_width_current", "?"))
    w_cur = g.get("pcie_width_current", w_max)
    s = f"Gen{gen} x{w_max}"
    # Only flag width degradation (real hardware issue: bad slot/riser/cable).
    # Gen dropping at idle (Gen4 -> Gen2) is normal GPU power saving.
    if str(w_cur) != str(w_max):
        return f'<span class="alert">{s} (width x{w_cur})</span>'
    return s


def info_to_str(info) -> str:
    if isinstance(info, list):
        return "; ".join(str(i).strip() for i in info if str(i).strip())
    if isinstance(info, str):
        return info.strip()
    return ""


def extract_num(text: str, pattern: str) -> str | None:
    m = re.search(pattern, text)
    return m.group(1) if m else None


def _resolve_gpu_id(r: dict) -> int | None:
    """Try to determine real GPU ID from a single DCGM result entry.

    Returns the GPU index (int) if found, or None if indeterminate.
    Checks the explicit gpu_id field first, then parses the info string
    for patterns like 'GPU 3 calculated ...' or 'ECC is not enabled on GPU 7'.
    """
    gid = r.get("gpu_id")
    if gid is not None:
        try:
            return int(gid)
        except (ValueError, TypeError):
            pass
    info = info_to_str(r.get("info", ""))
    m = re.search(r'\bGPU\s+(\d+)\b', info)
    if m:
        return int(m.group(1))
    return None


def _find_remap_skipped_gpus(diag: dict, n_gpus: int) -> set[int]:
    """Identify GPU IDs skipped due to row-remapping failure.

    Scans tests that carry explicit GPU IDs in their info strings to
    determine which GPU(s) are absent when a row-remap skip is present.
    """
    all_ids = set(range(n_gpus))
    skipped: set[int] = set()
    for t in diag.get("test_results", []):
        found_ids: set[int] = set()
        has_remap_skip = False
        for r in t.get("results", []):
            gid = _resolve_gpu_id(r)
            if gid is not None and 0 <= gid < n_gpus:
                found_ids.add(gid)
            if "row remapping" in info_to_str(r.get("info", "")).lower():
                has_remap_skip = True
        if has_remap_skip and found_ids:
            skipped |= (all_ids - found_ids)
    return skipped


def _build_gpu_id_map(results_list: list[dict], n_gpus: int,
                      remap_skipped: set[int]) -> list[int]:
    """Map each result-array index to the real GPU ID.

    Uses three strategies in order:
      1. Explicit gpu_id / info-string extraction
      2. Row-remapping skip entries matched to known-skipped GPUs
      3. Remaining unknowns filled by elimination against the full 0..n-1 set
    Falls back to array-index if nothing else resolves.
    """
    n = len(results_list)
    mapping: list[int | None] = [None] * n

    # --- Pass 1: resolve from gpu_id field or info string ---
    for idx, r in enumerate(results_list):
        gid = _resolve_gpu_id(r)
        if gid is not None and 0 <= gid < n_gpus:
            mapping[idx] = gid

    # --- Pass 2: assign row-remapping skip entries ---
    remap_indices = [
        idx for idx, r in enumerate(results_list)
        if mapping[idx] is None
        and "row remapping" in info_to_str(r.get("info", "")).lower()
    ]
    unassigned_remap = sorted(remap_skipped - {g for g in mapping if g is not None})
    if remap_indices and len(remap_indices) == len(unassigned_remap):
        for idx, gid in zip(remap_indices, unassigned_remap):
            mapping[idx] = gid

    # --- Pass 3: fill remaining unknowns by elimination ---
    known_ids = {g for g in mapping if g is not None}
    missing_ids = sorted(set(range(n_gpus)) - known_ids)
    unknown_indices = [i for i in range(n) if mapping[i] is None]
    if len(missing_ids) == len(unknown_indices):
        for idx, gid in zip(unknown_indices, missing_ids):
            mapping[idx] = gid
    else:
        # Last resort: use array index
        for idx in unknown_indices:
            mapping[idx] = idx if idx < n_gpus else 0

    return mapping  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# STRESS METRIC EXTRACTION (from v2.2)
# ---------------------------------------------------------------------------

def build_stress_metrics(diag: dict, n_gpus: int) -> list[dict]:
    metrics = [{} for _ in range(n_gpus)]
    remap_skipped = _find_remap_skipped_gpus(diag, n_gpus)
    results = diag.get("test_results", [])
    for t in results:
        name = t.get("test", "")
        per_gpu = t.get("results", [])
        gpu_map = _build_gpu_id_map(per_gpu, n_gpus, remap_skipped)
        for idx, r in enumerate(per_gpu):
            gpu_id = gpu_map[idx]
            if gpu_id >= n_gpus:
                continue
            info = info_to_str(r.get("info", ""))
            if not info:
                continue
            if name == "diagnostic":
                val = extract_num(info, r'approximately\s+([\d.]+)\s+gigaflops')
                if val:
                    metrics[gpu_id]["gflops"] = val
            elif name == "pcie":
                bw = extract_num(info, r'bidirectional bandwidth[:\s]+([\d.]+)')
                lat = extract_num(info, r'GPU to Host latency[:\s]+([\d.]+)')
                if bw:
                    metrics[gpu_id]["pcie_bw"] = bw
                if lat:
                    metrics[gpu_id]["pcie_lat"] = lat
            elif name == "targeted_power":
                avg = extract_num(info, r'average power usage[:\s]+([\d.]+)')
                mx = extract_num(info, r'max power[:\s]+([\d.]+)')
                if avg:
                    metrics[gpu_id]["power_avg"] = avg
                if mx:
                    metrics[gpu_id]["power_max"] = mx
            elif name == "targeted_stress":
                lvl = extract_num(info, r'stress level\s+([\d]+)')
                if lvl:
                    metrics[gpu_id]["stress_lvl"] = lvl
            elif name == "memory":
                pct = extract_num(info, r'\(([\d.]+)%\)')
                if pct:
                    metrics[gpu_id]["mem_pct"] = pct
    return metrics


def render_test_matrix(diag: dict, n_gpus: int) -> str:
    results = diag.get("test_results", [])
    if not results:
        return ""
    remap_skipped = _find_remap_skipped_gpus(diag, n_gpus)
    gpu_headers = "".join(f"<th>{i}</th>" for i in range(n_gpus))
    rows = ""
    for t in results:
        name = t.get("test", "?")
        per_gpu = t.get("results", [])
        gpu_map = _build_gpu_id_map(per_gpu, n_gpus, remap_skipped)
        # Build cells indexed by real GPU ID
        cells_by_gpu = ['<td><span class="dot-skip">?</span></td>'] * n_gpus
        for idx, r in enumerate(per_gpu):
            gpu_id = gpu_map[idx]
            if gpu_id >= n_gpus:
                continue
            st = r.get("status", "?").lower()
            if "pass" in st:
                cell = '<td><span class="dot-pass">&#10003;</span></td>'
            elif "fail" in st:
                cell = '<td><span class="dot-fail">&#10007;</span></td>'
            elif "warn" in st:
                cell = '<td><span class="dot-warn">!</span></td>'
            elif "skip" in st:
                cell = '<td><span class="dot-skip">&mdash;</span></td>'
            else:
                cell = '<td><span class="dot-skip">?</span></td>'
            cells_by_gpu[gpu_id] = cell
        rows += f'<tr><td class="test-name">{escape(name)}</td>{"".join(cells_by_gpu)}</tr>'

    total_pass = sum(1 for t in results for r in t.get("results", []) if "pass" in r.get("status", "").lower())
    total_fail = sum(1 for t in results for r in t.get("results", []) if "fail" in r.get("status", "").lower())
    total_skip = sum(1 for t in results for r in t.get("results", []) if "skip" in r.get("status", "").lower())
    total_warn = sum(1 for t in results for r in t.get("results", []) if "warn" in r.get("status", "").lower())

    summary_parts = []
    if total_pass: summary_parts.append(f'<span class="st-pass">{total_pass} passed</span>')
    if total_fail: summary_parts.append(f'<span class="st-fail">{total_fail} failed</span>')
    if total_warn: summary_parts.append(f'<span class="st-warn">{total_warn} warnings</span>')
    if total_skip: summary_parts.append(f'<span class="st-skip">{total_skip} skipped</span>')
    summary = f'<div class="matrix-summary">{" &middot; ".join(summary_parts)}</div>'

    return f'''
    {summary}
    <table class="tbl matrix">
        <thead><tr><th>Test</th>{gpu_headers}</tr></thead>
        <tbody>{rows}</tbody>
    </table>'''


# ---------------------------------------------------------------------------
# RENDER: NEW HARDWARE SECTIONS
# ---------------------------------------------------------------------------

def render_dimm_table(dimms: list[dict]) -> str:
    """Render DIMM inventory table."""
    if not dimms:
        return '<span class="dim">DIMM inventory not available</span>'

    # Defensive filter: exclude cache, system board, and empty slots
    _exclude = ("cache", "l1 ", "l2 ", "l3 ", "system board", "motherboard")
    clean = []
    for d in dimms:
        if d["size_gb"] <= 0:
            continue
        slot_lower = (d.get("slot") or "").lower()
        desc_lower = (d.get("description") or "").lower()
        if any(kw in slot_lower or kw in desc_lower for kw in _exclude):
            continue
        clean.append(d)

    if not clean:
        return '<span class="dim">DIMM inventory not available</span>'

    total_gb = sum(d["size_gb"] for d in clean)
    populated = len(clean)

    rows = ""
    for d in clean:
        size = f'{d["size_gb"]:.0f} GB'
        clock_val = d["clock_mhz"]
        if not clock_val:
            clock = "--"
        elif clock_val >= 1000:
            clock = f'{clock_val} MT/s'
        else:
            clock = f'{clock_val} MHz'
        rows += f'''<tr>
            <td class="mono">{escape(d["slot"] or "--")}</td>
            <td>{size}</td>
            <td>{escape(d["vendor"] or "--")}</td>
            <td class="mono tiny">{escape(d["product"] or "--")}</td>
            <td class="mono tiny">{escape(d["serial"] or "--")}</td>
            <td class="mono">{clock}</td>
        </tr>'''

    return f'''
    <div class="table-note">{populated} DIMMs populated &mdash; {total_gb:.0f} GB total</div>
    <div class="tbl-wrap">
        <table class="tbl gpu">
            <thead><tr>
                <th>Slot</th><th>Size</th><th>Vendor</th>
                <th>Part Number</th><th>Serial</th><th>Speed</th>
            </tr></thead>
            <tbody>{rows}</tbody>
        </table>
    </div>'''


def render_pci_device_table(devices: list[dict], category: str) -> str:
    """Render PCI device table for network or storage devices."""
    if not devices:
        return f'<span class="dim">No {category} devices found</span>'

    rows = ""
    for d in devices:
        rows += f'''<tr>
            <td>{escape(d.get("vendor","") or "--")}</td>
            <td class="mono">{escape(str(d.get("vendor_id","") or "--"))}</td>
            <td>{escape(d.get("product","") or "--")}</td>
            <td class="mono">{escape(str(d.get("product_id","") or "--"))}</td>
            <td class="mono">{escape(d.get("driver","") or "--")}</td>
            <td>{v(d.get("numa_node", -1))}</td>
            <td class="mono tiny">{escape(d.get("pci_address","") or "--")}</td>
        </tr>'''

    return f'''
    <div class="table-note">{len(devices)} devices</div>
    <div class="tbl-wrap">
        <table class="tbl gpu">
            <thead><tr>
                <th>Vendor</th><th>Vendor ID</th><th>Product</th>
                <th>Product ID</th><th>Driver</th><th>NUMA</th><th>PCI Address</th>
            </tr></thead>
            <tbody>{rows}</tbody>
        </table>
    </div>'''


def _group_nics_by_card(nics: list[dict]) -> list[dict]:
    """Group NIC ports into physical cards.
    
    Ports on the same card share the same MAC prefix (first 5 octets)
    and typically same vendor/product/NUMA node.
    """
    if not nics:
        return []

    # Build MAC prefix groups
    groups: dict[str, list[dict]] = {}
    ungrouped = []
    for n in nics:
        mac = (n.get("mac", "") or "").lower().strip()
        if len(mac) >= 14:  # at least "aa:bb:cc:dd:ee"
            # Use first 5 octets as card identifier
            prefix = mac[:14]  # "aa:bb:cc:dd:ee"
            groups.setdefault(prefix, []).append(n)
        else:
            ungrouped.append(n)

    cards = []
    for prefix, ports in groups.items():
        # Use first port's product/vendor as the card identity
        rep = ports[0]
        product = rep.get("product", "") or ""
        vendor = rep.get("vendor", "") or ""

        if product and vendor and vendor.lower() not in product.lower():
            model_str = f'{vendor} {product}'
        elif product:
            model_str = product
        elif vendor:
            model_str = vendor
        else:
            model_str = "--"

        macs = sorted(p.get("mac", "") for p in ports)
        sriov_max = max((p.get("sriov_max_vf", 0) or 0) for p in ports)
        numa = rep.get("numa_node", -1)

        cards.append({
            "model": model_str,
            "ports": len(ports),
            "macs": macs,
            "sriov_max_vf": sriov_max,
            "numa_node": numa,
        })

    # Add ungrouped as single-port cards
    for n in ungrouped:
        product = n.get("product", "") or ""
        vendor = n.get("vendor", "") or ""
        if product and vendor and vendor.lower() not in product.lower():
            model_str = f'{vendor} {product}'
        elif product:
            model_str = product
        elif vendor:
            model_str = vendor
        else:
            model_str = "--"
        cards.append({
            "model": model_str,
            "ports": 1,
            "macs": [n.get("mac", "")],
            "sriov_max_vf": n.get("sriov_max_vf", 0) or 0,
            "numa_node": n.get("numa_node", -1),
        })

    return cards


def render_nic_table(nics: list[dict]) -> str:
    """Render network devices table, grouped by physical card."""
    if not nics:
        return '<span class="dim">No physical NICs found</span>'

    cards = _group_nics_by_card(nics)

    rows = ""
    for c in cards:
        ports_str = f'{c["ports"]}-port' if c["ports"] > 1 else "1-port"
        mac_display = c["macs"][0]
        if len(c["macs"]) > 1:
            # Show range: first...last
            last_octet_first = c["macs"][0].split(":")[-1]
            last_octet_last = c["macs"][-1].split(":")[-1]
            mac_display = f'{c["macs"][0]} &hellip; {last_octet_last}'

        sriov = f'{c["sriov_max_vf"]} VFs/port' if c["sriov_max_vf"] else "--"

        rows += f'''<tr>
            <td>{escape(c["model"])}</td>
            <td class="mono">{ports_str}</td>
            <td class="mono tiny">{mac_display}</td>
            <td>{sriov}</td>
            <td>{v(c["numa_node"])}</td>
        </tr>'''

    return f'''
    <div class="table-note">{len(cards)} physical adapters ({len(nics)} ports)</div>
    <div class="tbl-wrap">
        <table class="tbl gpu">
            <thead><tr>
                <th>Model</th><th>Ports</th><th>MAC</th>
                <th>SR-IOV</th><th>NUMA</th>
            </tr></thead>
            <tbody>{rows}</tbody>
        </table>
    </div>'''


def render_storage_table(block_devs: list[dict]) -> str:
    """Render storage device table with RAID member support."""
    if not block_devs:
        return '<span class="dim">No block devices found</span>'

    total_gb = sum(d["size_gb"] for d in block_devs)

    rows = ""
    for d in block_devs:
        name = d.get("name", "?")
        # Show RAID membership badge
        raid_badge = ""
        if d.get("raid_member"):
            raid_name = d.get("raid_name", "")
            raid_level = d.get("raid_level", "")
            label = f'{raid_name} ({raid_level})' if raid_level else raid_name
            raid_badge = f' <span class="badge badge-na">{escape(label)}</span>' if label else \
                         ' <span class="badge badge-na">RAID member</span>'
        dev_type = d.get("type", "")
        type_badge = ""
        if dev_type and dev_type not in ("block", "disk"):
            type_badge = f' <span class="dim">({escape(dev_type)})</span>'

        rows += f'''<tr>
            <td class="mono">{escape(name)}{raid_badge}{type_badge}</td>
            <td>{escape(d.get("model","") or "--")}</td>
            <td class="mono tiny">{escape(d.get("serial","") or "--")}</td>
            <td class="mono">{d["size_gb"]} GB</td>
            <td class="mono tiny">{escape(d.get("firmware","") or "--")}</td>
            <td>{v(d.get("numa_node", -1))}</td>
        </tr>'''

    return f'''
    <div class="table-note">{len(block_devs)} devices &mdash; {total_gb:.0f} GB total</div>
    <div class="tbl-wrap">
        <table class="tbl gpu">
            <thead><tr>
                <th>Device</th><th>Model</th><th>Serial</th>
                <th>Size</th><th>Firmware</th><th>NUMA</th>
            </tr></thead>
            <tbody>{rows}</tbody>
        </table>
    </div>'''


def render_commissioning_scripts_table(scripts: list[dict]) -> str:
    """Render table of all commissioning scripts that ran."""
    if not scripts:
        return '<span class="dim">No commissioning data</span>'

    rows = ""
    for s in scripts:
        status = s["status"]
        cls = ""
        if "pass" in status.lower():
            cls = "dot-pass"
        elif "fail" in status.lower():
            cls = "dot-fail"
        elif "skip" in status.lower():
            cls = "dot-skip"
        else:
            cls = "dot-warn"

        icon = {"dot-pass": "&#10003;", "dot-fail": "&#10007;", "dot-skip": "&mdash;", "dot-warn": "!"}.get(cls, "?")

        rows += f'''<tr>
            <td class="mono tiny">{escape(s["name"])}</td>
            <td><span class="{cls}">{icon}</span> {escape(status)}</td>
            <td class="mono tiny">{escape(str(s.get("runtime","--")))}</td>
        </tr>'''

    return f'''
    <div class="tbl-wrap">
        <table class="tbl gpu">
            <thead><tr><th>Script</th><th>Status</th><th>Runtime</th></tr></thead>
            <tbody>{rows}</tbody>
        </table>
    </div>'''


# ---------------------------------------------------------------------------
# MAIN REPORT GENERATOR
# ---------------------------------------------------------------------------

def acceptance_badge(verdict: str) -> str:
    cls = {"ACCEPT": "pass", "REVIEW": "warn", "REJECT": "fail"}.get(verdict, "na")
    return f'<span class="badge badge-{cls}">{escape(verdict)}</span>'


_ACC_DOT = {"PASS": ("dot-pass", "&#10003;"), "FAIL": ("dot-fail", "&#10007;"),
            "WARN": ("dot-warn", "!"), "N/A": ("dot-skip", "&mdash;")}


def render_gpu_acceptance(gpus, stress, burn, inventory=None, install=None,
                          evals=None, config=None, context=None) -> str:
    """Per-GPU hardware acceptance matrix plus per-serial evidence."""
    if not gpus:
        return '<span class="dim">No GPU inventory -- acceptance cannot be evaluated</span>'

    if evals is None:
        evals = [evaluate_gpu_acceptance(g, stress, burn, config, context)
                 for g in gpus]
    by_idx = {e["gpu_index"]: e for e in evals}

    # Union of criterion ids in first-seen order: a GPU with no stress result
    # emits fewer criteria than one that has them.
    order, labels, rejects = [], {}, {}
    for e in evals:
        for c in e["criteria"]:
            if c["id"] not in labels:
                order.append(c["id"])
                labels[c["id"]] = c["label"]
                rejects[c["id"]] = c["reject"]

    n_rej = sum(1 for e in evals if e["verdict"] == "REJECT")
    n_rev = sum(1 for e in evals if e["verdict"] == "REVIEW")
    n_acc = sum(1 for e in evals if e["verdict"] == "ACCEPT")

    idxs = sorted(by_idx)
    head = "".join(f"<th>{i}</th>" for i in idxs)
    rows = ""
    for cid in order:
        cells = ""
        for i in idxs:
            c = next((x for x in by_idx[i]["criteria"] if x["id"] == cid), None)
            if c is None:
                cells += '<td class="dot-skip">&mdash;</td>'
            else:
                cls, glyph = _ACC_DOT.get(c["status"], ("dot-skip", "?"))
                cells += f'<td class="{cls}" title="{escape(c["detail"])}">{glyph}</td>'
        rows += (f'<tr><td class="nowrap">{escape(labels[cid])}</td>'
                 f'<td class="dim tiny nowrap">#{rejects[cid]}</td>{cells}</tr>')

    verdict_row = "".join(
        f'<td>{acceptance_badge(by_idx[i]["verdict"])}</td>' for i in idxs)

    # Test age: we know when the test ran, not when the unit ships, so this is
    # reported rather than gated.
    age = acceptance_test_age_days(inventory, stress, burn)
    if age is None:
        age_note = "Test date unavailable."
    elif age > ACCEPTANCE_MAX_AGE_DAYS:
        age_note = (f'<span class="alert">Test evidence is {age:.0f} days old</span>, '
                    f"beyond the {ACCEPTANCE_MAX_AGE_DAYS}-day window. Retest before shipment.")
    else:
        age_note = f"Test evidence is {age:.1f} day(s) old."

    summary = (f'<span class="st-pass">{n_acc} accept</span> &middot; '
               f'<span class="st-warn">{n_rev} review</span> &middot; '
               f'<span class="st-fail">{n_rej} reject</span> &mdash; {age_note}')

    # Per-serial detail, collapsed.  This is what makes the report per-GPU
    # rather than lot-level: every serial carries its own evidence.
    details = ""
    for e in evals:
        g = next((x for x in gpus
                  if (int_or_none(x.get("gpu_index")) or 0) == e["gpu_index"]), {})
        ident = g.get("identity") or {}
        irom = ident.get("inforom") or {}
        pcie = g.get("pcie") or {}
        kv = [
            ("Product name", ident.get("product_name") or g.get("name")),
            ("Serial", e["serial"]),
            ("UUID", e["uuid"]),
            ("PCI device ID", ident.get("pci_device_id")),
            ("Board part number", ident.get("board_part_number")),
            ("GPU part number", ident.get("gpu_part_number")),
            ("VBIOS", g.get("vbios_version")),
            ("InfoROM image", irom.get("image")),
            ("InfoROM ECC object", irom.get("ecc")),
            ("VRAM", f'{g.get("vram_mib")} MiB' if g.get("vram_mib") else None),
            ("PCIe", _acc_pcie_str(pcie, g)),
            ("Bus ID", g.get("pci_bus_id")),
        ]
        kv_html = "".join(
            f'<tr><td class="kv-key">{escape(k)}</td>'
            f'<td class="mono">{escape(str(v))}</td></tr>'
            for k, v in kv if v not in (None, "", "None"))

        # Section 2 of the specification asks for the raw
        # `nvidia-smi -q -d ROW_REMAPPER,ECC` output per card. Script 92 embeds
        # exactly that, per GPU, and it has never been rendered until now.
        raw = (g.get("raw_evidence") or {})
        gpu_raw_html = "".join(filter(None, [
            render_log_block("nvidia-smi -q -d ROW_REMAPPER,ECC",
                             decode_b64_text(raw.get("row_remapper_ecc_b64")),
                             note="memory-health evidence for this card"),
            render_log_block("nvidia-smi -q (full device record)",
                             decode_b64_text(raw.get("smi_query_b64")),
                             note="identity, link, thermal and power state"),
        ]))

        crit_html = ""
        for c in e["criteria"]:
            cls, glyph = _ACC_DOT.get(c["status"], ("dot-skip", "?"))
            crit_html += (f'<tr><td class="{cls}">{glyph}</td>'
                          f'<td class="nowrap">{escape(c["label"])}</td>'
                          f'<td class="dim tiny">#{c["reject"]}</td>'
                          f'<td class="tiny">{escape(c["detail"])}</td></tr>')

        details += f'''
        <details class="acc-detail">
            <summary>GPU {e["gpu_index"]} &mdash; <span class="mono">{escape(str(e["serial"] or "unknown serial"))}</span> {acceptance_badge(e["verdict"])}</summary>
            <div class="acc-body">
                <table class="tbl kv">{kv_html}</table>
                <table class="tbl gpu">
                    <thead><tr><th></th><th>Criterion</th><th>Cond</th><th>Evidence</th></tr></thead>
                    <tbody>{crit_html}</tbody>
                </table>
                {gpu_raw_html}
            </div>
        </details>'''

    return f'''
    <div class="table-note">{summary}</div>
    <div class="tbl-wrap">
        <table class="tbl matrix">
            <thead><tr><th>Criterion</th><th>Cond</th>{head}</tr></thead>
            <tbody>
                {rows}
                <tr class="acc-verdict"><td><strong>Verdict</strong></td><td></td>{verdict_row}</tr>
            </tbody>
        </table>
    </div>
    <div class="table-note">Reject conditions are numbered per the acceptance specification.
    <code>sw_power_cap</code> under sustained load is reported as expected operation, not a fault;
    PCIe is judged against the maximum both card and host support rather than the idle value.</div>
    {details}'''


def _acc_pcie_str(pcie, gpu):
    dev = pcie.get("gen_device_max")
    host = pcie.get("gen_host_max")
    neg = pcie.get("gen_negotiated_max") or gpu.get("pcie_gen_max")
    w = pcie.get("width_max") or gpu.get("pcie_width_max")
    if neg is None:
        return None
    s = f"Gen{neg} x{w}" if w else f"Gen{neg}"
    if dev is not None and host is not None and host != dev:
        s += f" (card Gen{dev}, host Gen{host})"
    elif dev is not None:
        s += f" (card max Gen{dev})"
    return s


def generate_report(
    install: dict | None,
    inventory: dict | None,
    stress: dict | None,
    maas_url: str | None = None,
    system_id: str | None = None,
    # New v3 data sources
    machine: dict | None = None,
    hardware_info: dict | None = None,
    nics: list[dict] | None = None,
    block_devices: list[dict] | None = None,
    storage_devices: list[dict] | None = None,
    pci_devices: dict | None = None,
    numa_nodes_maas: list[dict] | None = None,
    dimms: list[dict] | None = None,
    all_scripts: list[dict] | None = None,
    cpu_topology: dict | None = None,
    script_logs: dict | None = None,
    hostname_override: str | None = None,
    config: dict | None = None,
    burnin: dict | None = None,
) -> str:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    # -- Data sources: GPU scripts --
    sys_info = {}
    gpus = []
    numa = {}
    if inventory:
        sys_info = inventory.get("system", {})
        gpus = inventory.get("gpus", [])
        numa = inventory.get("numa_topology", {})
    elif stress:
        sys_info = stress.get("system", {})

    # -- Hostname & platform: prefer MAAS machine data, fallback to script data --
    hw = hardware_info or {}
    mach = machine or {}

    hostname = hostname_override or mach.get("hostname") or sys_info.get("hostname", "Unknown")
    product = (
        f'{hw.get("system_vendor", "")} {hw.get("system_product", "")}'.strip()
        or sys_info.get("product_name", "")
    )
    serial_number = hw.get("system_serial", "") or sys_info.get("serial_number", "")

    gpu_count = len(gpus) if gpus else (install or {}).get("install", {}).get("gpu_count", 0)
    if not gpu_count and stress:
        gpu_count = stress.get("system", {}).get("gpu_count", 0)
    gpu_model = gpus[0].get("name", "--") if gpus else "--"
    inst = (install or {}).get("install", {})
    diag = (stress or {}).get("dcgm_diagnostics", {})

    # MAAS link
    maas_link = ""
    if maas_url and system_id:
        url = f"{maas_url.rstrip('/')}/r/machine/{system_id}/commissioning"
        # Handle double /MAAS
        if "/MAAS" in maas_url and not url.startswith(maas_url.rstrip("/")):
            base = maas_url.rstrip("/").rsplit("/MAAS", 1)[0]
            url = f"{base}/MAAS/r/machine/{system_id}/commissioning"
        maas_link = f'<a href="{url}" class="maas-link" target="_blank">View in MAAS &rarr;</a>'

    # Verdicts — collect issues first, filter false positives, then derive verdicts
    stages = [("Install", install), ("MIG/ECC", config), ("Inventory", inventory),
              ("DCGM Diagnostics", stress), ("Burn-In", burnin)]
    all_issues = []
    for label, data in stages:
        if data:
            for iss in data.get("verdict", {}).get("issues", []):
                c = dict(iss)
                c["source"] = label
                all_issues.append(c)

    # Filter redundant/false-positive issues:
    # - ECC counter query failures are irrelevant when DCGM stress test validated ECC health
    # - PCIe "link degradation" from inventory is a false positive: GPUs drop gen at idle
    #   (Gen4 -> Gen2) to save power. Only width degradation is a real hardware issue,
    #   and the inventory script (v2.0.3+) no longer flags gen-only differences.
    #   Filter it here to handle reports generated from older inventory data.
    if stress:
        all_issues = [i for i in all_issues
                      if "counters unavailable" not in i.get("issue", "").lower()]
    all_issues = [i for i in all_issues
                  if "pcie link degradation" not in i.get("issue", "").lower()]

    # - An nvbandwidth skip on a host with no NVLink is expected operation, not
    #   missing evidence: there is no peer link for it to measure, and the pcie
    #   test already covers host/device bandwidth. Reclassified to info rather
    #   than dropped, so the fact stays on the report.
    #
    #   This is adjudication, so it belongs here and not in script 98: the
    #   scripts collect evidence, this file decides what it means. Keeping it
    #   here also means a report generated from already-stored commissioning
    #   data gets the corrected reading without re-running anything, and the
    #   rule exists in exactly one place.
    #
    #   Only ever applied when NVLink was positively measured absent -- an
    #   unknown is not grounds to waive evidence -- and only when the excused
    #   tests are the ONLY ones skipped.
    if sys_info.get("nvlink_present") is False:
        skipped = skipped_test_names(diag)
        if skipped and skipped <= SKIP_EXPECTED_WITHOUT_NVLINK:
            for i in all_issues:
                if i.get("source") != "DCGM Diagnostics":
                    continue
                if "skip" not in i.get("issue", "").lower():
                    continue
                i["severity"] = "info"
                i["issue"] = (
                    "%s skipped as expected: no NVLink is fitted, so there is no "
                    "peer bandwidth to measure. Host/device bandwidth is covered "
                    "by the pcie test." % ", ".join(sorted(skipped)))

    # Per-GPU acceptance is adjudicated here rather than after rendering, so a
    # server holding a rejectable card cannot show a PASS headline.
    acc_context = build_acceptance_context(inventory, all_scripts)
    acc_evals = [evaluate_gpu_acceptance(g, stress, burnin, config, acc_context)
                 for g in gpus]
    acc_reject = [e for e in acc_evals if e["verdict"] == "REJECT"]
    acc_review = [e for e in acc_evals if e["verdict"] == "REVIEW"]
    if acc_reject:
        all_issues.append({
            "severity": "critical",
            "source": "Acceptance",
            "issue": ("%d of %d GPU(s) fail hardware acceptance: %s"
                      % (len(acc_reject), len(acc_evals),
                         ", ".join(str(e["serial"] or f"GPU {e['gpu_index']}")
                                   for e in acc_reject))),
        })
    if acc_review:
        all_issues.append({
            "severity": "warning",
            "source": "Acceptance",
            "issue": ("%d of %d GPU(s) need review before acceptance: %s"
                      % (len(acc_review), len(acc_evals),
                         ", ".join(str(e["serial"] or f"GPU {e['gpu_index']}")
                                   for e in acc_review))),
        })

    # Derive per-stage verdicts: if all issues for a stage were filtered out,
    # upgrade from WARN to PASS (FAIL stays as-is since those are real failures)
    # Only adverse findings hold a stage below PASS. An informational note --
    # "no NVLink fitted", "characterize mode" -- is not a finding, and a stage
    # whose only remaining issues are info has nothing outstanding against it.
    remaining_sources = {i["source"] for i in all_issues
                         if i.get("severity") in ("critical", "warning")}
    verdicts = []
    for label, data in stages:
        if data:
            raw = data.get("verdict", {}).get("overall", "N/A")
            if raw == "WARN" and label not in remaining_sources:
                raw = "PASS"
            verdicts.append((label, raw))
        else:
            verdicts.append((label, "N/A"))

    # Absent evidence must degrade the verdict, not be ignored.  N/A used to
    # rank *lowest* here, so a report where inventory and stress were missing
    # entirely came out PASS on the strength of the install stage alone.  A
    # report that omits required evidence is not a pass.
    pri = {"FAIL": 0, "WARN": 1, "PASS": 2, "N/A": 3}
    present = [v for v in verdicts if v[1] != "N/A"]
    missing = [label for label, vv in verdicts if vv == "N/A" and label in REQUIRED_STAGES]

    if not present:
        overall = "N/A"
    else:
        overall = min(present, key=lambda x: pri.get(x[1], 3))[1]
        if missing:
            # Downgrade at most to WARN: the evidence we do have still stands,
            # but the report is incomplete and must not read as a clean pass.
            if overall == "PASS":
                overall = "WARN"
            all_issues.append({
                "severity": "warning",
                "source": "Report",
                "issue": ("Required evidence missing: no result for "
                          + ", ".join(missing)
                          + " -- the report is incomplete"),
            })

    # A server shipping with a rejectable card is not a pass, whatever the
    # individual script stages concluded.
    if acc_reject:
        overall = "FAIL"
    elif acc_review and overall == "PASS":
        overall = "WARN"

    # Script metadata
    script_meta = []
    for label, data in stages:
        if data:
            m = data.get("report_metadata", {})
            script_meta.append(
                f'{label} v{m.get("script_version","?")} &mdash; '
                f'{m.get("generated_at","")} ({fmt_dur(m.get("duration_seconds", m.get("test_duration_seconds", 0)))})'
            )

    # Stress metrics per GPU
    stress_metrics = build_stress_metrics(diag, gpu_count) if stress and gpu_count else []
    has_stress = bool(stress_metrics and any(m for m in stress_metrics))

    # ===== BUILD HTML =====

    # Verdict cards
    verdict_cards = ""
    for label, vv in verdicts:
        meta = ""
        for l2, d in stages:
            if l2 == label and d:
                rm = d.get("report_metadata", {})
                dur = rm.get("duration_seconds", rm.get("test_duration_seconds", 0))
                if dur:
                    meta = f'<div class="card-meta">{fmt_dur(dur)}</div>'
        verdict_cards += f'''
        <div class="vcard">
            <div class="vcard-label">{label}</div>
            <div class="vcard-badge">{badge(vv)}</div>
            {meta}
        </div>'''

    # Issues
    if all_issues:
        rows = ""
        for iss in all_issues:
            sev = iss.get("severity", "info")
            cls = {"critical": "sev-crit", "warning": "sev-warn"}.get(sev, "sev-info")
            rows += f'<tr><td><span class="sev {cls}">{sev.upper()}</span></td><td class="dim">{iss.get("source","")}</td><td>{iss.get("issue","")}</td></tr>'
        issues_html = f'<table class="tbl issues"><thead><tr><th>Severity</th><th>Source</th><th>Issue</th></tr></thead><tbody>{rows}</tbody></table>'
    else:
        issues_html = '<div class="ok-msg">No issues detected across all stages</div>'

    # --- Hardware and Software tables (split into two-col layout) ---
    # Merge MAAS machine data with script data for a richer view
    cpu_str = hw.get("cpu_model", sys_info.get("cpu_model", "--"))
    total_threads = _as_int(mach.get("cpu_count", sys_info.get("cpu_total_threads", 0)))
    # Physical cores come from the inventory script (lscpu: Socket(s) x Core(s)
    # per socket). MAAS's NUMA node "cores" lists hold LOGICAL cpu ids, so
    # summing them described a 2x64C/256T machine as "256 cores / 256 threads".
    # Without a physical figure we say "logical CPUs" rather than claim cores.
    topo = cpu_topology or {}
    num_sockets = (_as_int(sys_info.get("cpu_sockets"))
                   or _as_int(topo.get("sockets"))
                   or (len(numa_nodes_maas) if numa_nodes_maas else 0))
    # Three sources, most authoritative first, so a machine commissioned before
    # script 92 started reporting cpu_total_cores still gets a correct figure:
    #   1. the inventory script (lscpu: Socket(s) x Core(s) per socket)
    #   2. machine-resources cpu.sockets[].cores[]  -- already in MAAS today
    #   3. the vendor part name ("... 64-Core Processor") x socket count
    total_cores = (_as_int(sys_info.get("cpu_total_cores"))
                   or _as_int(topo.get("cores"))
                   or cores_from_cpu_model(cpu_str, num_sockets))
    if not total_threads:
        total_threads = _as_int(topo.get("threads"))
    # Threads per core, so the NUMA blocks below can state physical cores as
    # well as the logical count MAAS shows.
    threads_per_core = 1
    if total_cores and total_threads and total_threads % total_cores == 0:
        threads_per_core = total_threads // total_cores
    prefix = f'{num_sockets}&times; ' if num_sockets > 1 else ''
    if total_cores:
        cpu_label = (f'{prefix}{escape(str(cpu_str))} &mdash; '
                     f'{total_cores} cores / {total_threads} threads')
    else:
        cpu_label = (f'{prefix}{escape(str(cpu_str))} &mdash; '
                     f'{total_threads} logical CPUs')
    ram_mb = mach.get("memory", 0)
    ram_gb = round(ram_mb / 1024, 1) if ram_mb else sys_info.get("ram_total_gb", "?")
    motherboard = hw.get("mainboard_product", sys_info.get("motherboard", "--"))
    mainboard_vendor = hw.get("mainboard_vendor", "")
    if mainboard_vendor and motherboard and motherboard != "--":
        motherboard = f"{mainboard_vendor} {motherboard}"

    bios_ver = hw.get("mainboard_firmware_version", "")
    bios_date = hw.get("mainboard_firmware_date", "")
    bios_str = f"{bios_ver} ({bios_date})" if bios_ver else "--"

    hw_fields = [
        ("Hostname", f'<span class="hl">{escape(hostname)}</span>'),
        ("Serial", escape(serial_number) or "--"),
        ("Platform", escape(product) or "--"),
        ("Motherboard", escape(motherboard)),
        ("BIOS", escape(bios_str)),
        ("CPU", cpu_label),
        ("RAM", f'{ram_gb} GB'),
    ]

    sw_fields = [
        ("Kernel", escape(sys_info.get("kernel_version", mach.get("osystem", "--")))),
        ("Architecture", escape(mach.get("architecture", "--"))),
    ]

    # Driver/CUDA/DCGM from install script
    if inst:
        sw_fields += [
            ("NVIDIA Driver", f'{escape(inst.get("driver_package",""))} ({escape(inst.get("nvidia_driver_version","?"))})'),
            ("CUDA", f'{escape(inst.get("cuda_package",""))} (reports {escape(inst.get("cuda_version","?"))})'),
            ("DCGM", f'v{escape(inst.get("dcgm_version","--"))}'),
        ]
    else:
        drv = sys_info.get("nvidia_driver_version")
        cuda = sys_info.get("cuda_version")
        if drv:
            sw_fields.append(("NVIDIA Driver", escape(drv)))
        if cuda:
            sw_fields.append(("CUDA", escape(cuda)))

    hw_rows = ""
    for k, vv in hw_fields:
        hw_rows += f'<tr><td class="kv-key">{k}</td><td>{vv}</td></tr>'

    sw_rows = ""
    for k, vv in sw_fields:
        sw_rows += f'<tr><td class="kv-key">{k}</td><td>{vv}</td></tr>'

    # --- GPU table (inventory + stress) ---
    stress_cols_hdr = ""
    if has_stress:
        stress_cols_hdr = '''
            <th title="Compute GFLOPS from DCGM diagnostic test">GFLOPS</th>
            <th title="PCIe bidirectional bandwidth">PCIe BW</th>
            <th title="Average / Max power under targeted_power test">Power Stress</th>
            <th title="DCGM targeted_stress relative level">Stress Lvl</th>
            <th title="Memory test coverage percentage">Mem Test</th>'''

    gpu_header = f'''<tr>
        <th>#</th><th>Serial</th><th>PCIe</th><th>NUMA</th>
        <th>Idle</th><th>Idle Power</th><th>ECC</th>
        <th>Remapped Rows</th><th>Banks</th>
        {stress_cols_hdr}
    </tr>'''

    gpu_rows = ""
    for g in gpus:
        idx = g.get("gpu_index", 0)
        ecc = g.get("ecc", {})
        sm = stress_metrics[idx] if idx < len(stress_metrics) else {}

        stress_cells = ""
        if has_stress:
            gflops = sm.get("gflops")
            pcie_bw = sm.get("pcie_bw")
            pcie_lat = sm.get("pcie_lat")
            p_avg = sm.get("power_avg")
            p_max = sm.get("power_max")
            s_lvl = sm.get("stress_lvl")
            mem_pct = sm.get("mem_pct")

            stress_cells = f'''
                <td class="mono">{v(gflops)}</td>
                <td class="mono nowrap">{v(pcie_bw, " GB/s")}{f' <span class="dim">({pcie_lat}us)</span>' if pcie_lat else ''}</td>
                <td class="mono nowrap">{v(p_avg)}{'W' if p_avg else ''} / {v(p_max)}{'W' if p_max else ''}</td>
                <td class="mono">{v(s_lvl)}</td>
                <td class="mono">{v(mem_pct, "%")}</td>'''

        gpu_rows += f'''<tr>
            <td>{idx}</td>
            <td class="mono">{escape(str(g.get("serial") or "--"))}</td>
            <td class="nowrap">{pcie_str(g)}</td>
            <td>{v(g.get("numa_node"))}</td>
            <td>{v(g.get("temp_idle_c"),"&deg;C")}</td>
            <td class="nowrap">{v(g.get("power_draw_w"),"W")} / {v(g.get("power_limit_w"),"W")}</td>
            <td>{ecc_summary(ecc)}</td>
            <td>{remapped_rows_summary(g)}</td>
            <td class="nowrap">{bank_availability_summary(g)}</td>
            {stress_cells}
        </tr>'''

    acceptance_html = render_gpu_acceptance(gpus, stress, burnin, inventory,
                                            install, evals=acc_evals, config=config,
                                            context=acc_context)

    # A run that had to enable ECC, or whose later stages were halted, cannot
    # produce usable evidence. Say so at the top rather than leaving it to be
    # discovered inside a per-GPU detail block.
    action_html = ""
    revalidate = bool((config or {}).get("revalidation_required"))
    skipped = [label for label, d in stages if d and d.get("skipped")]
    if revalidate or skipped:
        reasons = []
        if revalidate:
            n_ecc = (config or {}).get("ecc_enabled_this_run") or "one or more"
            reasons.append(
                f"ECC was <strong>disabled</strong> on {escape(str(n_ecc))} GPU(s) and was "
                "enabled during this run. While ECC was off the hardware was not "
                "detecting memory errors at all, and enabling it does not backfill "
                "&mdash; the aggregate counters restart from zero here. No memory "
                "evidence from this run can characterise these cards.")
        if skipped:
            reasons.append(
                "Skipped to save time: " + escape(", ".join(skipped))
                + ". These stages did not run because their output could not have been used.")
        reasons.append(
            "ECC is now enabled and persists across reboots. Commission this machine a "
            "second time; that run will produce valid evidence. <strong>This report must "
            "not be used as an acceptance certificate.</strong>")
        body = "".join(f"<p>{r}</p>" for r in reasons)
        action_html = (
            '<div class="action-required">'
            '<div class="action-title">Action required &mdash; re-run commissioning</div>'
            f"{body}</div>")

    gpu_section = ""
    if gpus:
        vram = gpus[0].get("vram_mib", "?")
        vram_type = gpus[0].get("vram_type", "?")
        gpu_info_line = f'<div class="table-note">{gpu_count}&times; {gpu_model} &mdash; {vram} MiB {vram_type}</div>'
        gpu_section = f'''{gpu_info_line}
        <div class="tbl-wrap">
            <table class="tbl gpu">
                <thead>{gpu_header}</thead>
                <tbody>{gpu_rows}</tbody>
            </table>
        </div>'''
    else:
        gpu_section = '<div class="dim">No GPU inventory data available</div>'

    # --- NUMA topology ---
    # Prefer MAAS NUMA data (richer -- includes memory + cores per node) merged with GPU mapping
    numa_html = ""
    if numa_nodes_maas and len(numa_nodes_maas) > 0:
        # Build GPU-to-NUMA mapping from our inventory data
        gpu_numa_map: dict[int, list] = {}
        if numa.get("gpu_to_numa_mapping"):
            for m in numa["gpu_to_numa_mapping"]:
                gpu_numa_map.setdefault(m.get("numa_node", -1), []).append(m)

        blocks = ""
        for n in numa_nodes_maas:
            idx = n["index"]
            mem = n.get("memory_mb", 0)
            cores = n.get("cores", [])
            mem_str = f'{mem // 1024} GB' if mem else "?"

            gpu_tags = ""
            if idx in gpu_numa_map:
                gpu_tags = " ".join(
                    f'<span class="numa-gpu">GPU {g.get("gpu_index","?")}</span>'
                    for g in gpu_numa_map[idx]
                )

            # MAAS lists LOGICAL cpu ids here. On this 2x64C/256T host that is
            # 128 per node, which is easy to misread as 128 physical cores --
            # the same number the whole machine has. Divide by threads-per-core
            # so each node reads unambiguously.
            if cores and threads_per_core > 1:
                core_str = (f'{len(cores) // threads_per_core} cores / '
                            f'{len(cores)} threads')
            elif cores:
                core_str = f'{len(cores)} cores'
            else:
                core_str = "?"

            blocks += f'''<div class="numa-node-row">
                <span class="numa-id">NODE {idx}</span>
                <span class="dim">{core_str}</span>
                <span class="dim">{mem_str}</span>
                {gpu_tags}
            </div>'''
        numa_html = blocks
    elif numa.get("numa_available"):
        # Fallback to script-only NUMA data
        nodes: dict[int, list] = {}
        for m in numa.get("gpu_to_numa_mapping", []):
            nodes.setdefault(m.get("numa_node", -1), []).append(m)
        blocks = ""
        for n in sorted(nodes):
            gpu_tags = " ".join(
                f'<span class="numa-gpu">GPU {g.get("gpu_index","?")}</span>'
                for g in nodes[n]
            )
            blocks += f'<div class="numa-node-row"><span class="numa-id">NODE {n}</span>{gpu_tags}</div>'
        numa_html = blocks
    else:
        numa_html = '<span class="dim">N/A</span>'

    # --- DIMM table ---
    dimm_html = render_dimm_table(dimms or [])

    # --- NIC table (prefer PCI devices, fallback to MAAS interface_set) ---
    pci = pci_devices or {}
    if pci.get("network"):
        nic_html = render_pci_device_table(pci["network"], "network")
    else:
        nic_html = render_nic_table(nics or [])

    # --- Storage table (prefer PCI devices, then enriched storage, then block devices) ---
    if pci.get("storage"):
        storage_html = render_pci_device_table(pci["storage"], "storage")
    elif storage_devices:
        storage_html = render_storage_table(storage_devices)
    else:
        storage_html = render_storage_table(block_devices or [])

    # --- Stress test matrix ---
    stress_section = ""
    if stress:
        level = diag.get("run_level", "?")
        dur = diag.get("duration_seconds", 0)
        exit_code = diag.get("exit_code", "?")
        stress_bar = f'''
        <div class="stress-bar">
            <span>Level <strong>{level}</strong></span>
            <span>Duration <strong>{fmt_dur(dur)}</strong></span>
            <span>Exit <strong>{exit_code}</strong></span>
        </div>'''
        matrix = render_test_matrix(diag, gpu_count)
        if matrix:
            stress_section = stress_bar + matrix
        else:
            stress_section = stress_bar + '<div class="dim">No per-test results parsed</div>'
    else:
        stress_section = '<div class="dim">Stress test data not available</div>'

    # --- Commissioning scripts table ---
    scripts_html = render_commissioning_scripts_table(all_scripts or [])
    raw_evidence_html = render_raw_evidence(inventory, stress, burnin,
                                           script_logs)

    run_info = "<br>".join(script_meta) if script_meta else "--"

    # Data source indicator
    data_source = "MAAS API" if machine else "Local files"

    # ===== ASSEMBLE =====
    html = f'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{escape(hostname)} &mdash; GPU Commissioning</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=Outfit:wght@300;400;500;600;700&display=swap" rel="stylesheet">
<style>
{CSS}
</style>
</head>
<body>
<div class="page">

<header>
    <div class="hdr-row">
        <div>
            <div class="brand">NEXGEN<span>CLOUD</span></div>
            <div class="brand-dept">DATA CENTER OPERATIONS</div>
        </div>
        <div class="hdr-right">
            {maas_link}
            <div class="overall">{badge(overall)}</div>
        </div>
    </div>
    <div class="title-block">
        <h1>GPU Commissioning Report</h1>
        <div class="subtitle">
            <span class="hl">{escape(hostname)}</span>
            <span class="sep-dot"></span>
            <span>{escape(product)}</span>
            <span class="sep-dot"></span>
            <span>{gpu_count}&times; {escape(gpu_model)}</span>
        </div>
    </div>
</header>

<section>
    <div class="section-label">Verdict</div>
    <div class="vcards">{verdict_cards}</div>
    {issues_html}
</section>

<section class="two-col">
    <div>
        <div class="section-label">Hardware</div>
        <table class="tbl kv"><tbody>{hw_rows}</tbody></table>
    </div>
    <div>
        <div class="section-label">NUMA Topology</div>
        {numa_html}
        <div class="section-label" style="margin-top:1.5rem">Software</div>
        <table class="tbl kv"><tbody>{sw_rows}</tbody></table>
    </div>
</section>

{action_html}

<section>
    <div class="section-label">Hardware Acceptance &mdash; per GPU</div>
    {acceptance_html}
</section>

<section>
    <div class="section-label">GPU Fleet</div>
    {gpu_section}
</section>

<section>
    <div class="section-label">DCGM Test Matrix</div>
    {stress_section}
</section>

<section>
    <div class="section-label">Memory (DIMMs)</div>
    {dimm_html}
</section>

<section class="two-col">
    <div>
        <div class="section-label">Network Devices</div>
        {nic_html}
    </div>
    <div>
        <div class="section-label">Storage</div>
        {storage_html}
    </div>
</section>

<section>
    <div class="section-label">All Commissioning Scripts</div>
    {scripts_html}
</section>

<section>
    <div class="section-label">Raw Evidence &mdash; full logs</div>
    {raw_evidence_html}
</section>

<footer>
    <div class="foot-left">
        <div class="foot-brand">nexgen-gpu-report v{__version__} &mdash; {data_source}</div>
        <div class="foot-ts">{now}</div>
    </div>
    <div class="foot-right">{run_info}</div>
</footer>

</div>
</body>
</html>'''
    return html


# ---------------------------------------------------------------------------
# CSS (shared with v2.2, extended for new sections)
# ---------------------------------------------------------------------------
CSS = '''
:root {
    --bg: #08090c;
    --page: #0d0f14;
    --card: #12151c;
    --card2: #181c26;
    --edge: #1f2433;
    --edge2: #2a3040;
    --txt: #c8cdd8;
    --txt2: #6b7280;
    --txt3: #3d4350;
    --bright: #eef0f6;
    --accent: #38bdf8;
    --green: #22c55e;
    --green-bg: rgba(34,197,94,.08);
    --green-bd: rgba(34,197,94,.25);
    --amber: #eab308;
    --amber-bg: rgba(234,179,8,.08);
    --amber-bd: rgba(234,179,8,.25);
    --red: #ef4444;
    --red-bg: rgba(239,68,68,.08);
    --red-bd: rgba(239,68,68,.25);
    --ff: "Outfit", system-ui, sans-serif;
    --mono: "DM Mono", "SF Mono", monospace;
}

@media print {
    :root {
        --bg:#fff; --page:#fff; --card:#f7f8fa; --card2:#eef0f3;
        --edge:#dde0e6; --edge2:#c8ccd4; --txt:#1a1c20; --txt2:#5c6070;
        --txt3:#a0a4b0; --bright:#000;
        --green-bg:rgba(34,197,94,.12); --amber-bg:rgba(234,179,8,.12);
        --red-bg:rgba(239,68,68,.12);
    }
    body { font-size: 8.5pt; }
    .page { max-width: 100%; padding: .5rem; }
    section, .two-col > div { break-inside: avoid; }
    /* Expand every collapsed evidence block when printed: a paper
       certificate must not hide the per-serial detail. */
    /* Raw logs are for the on-screen copy; a printed certificate should not
       carry megabytes of them. The adjudication and its evidence still print. */
    .ev-block { display: none !important; }
    .acc-detail { break-inside: avoid; }
    .acc-detail > summary { list-style: none; }
    details.acc-detail > .acc-body { display: grid !important; }
    .tbl-wrap { overflow: visible; }
}

* { margin:0; padding:0; box-sizing:border-box; }

body {
    font-family: var(--ff);
    background: var(--bg);
    color: var(--txt);
    line-height: 1.55;
    -webkit-font-smoothing: antialiased;
}

.page {
    max-width: 1400px;
    margin: 0 auto;
    padding: 2.5rem 3rem;
    background: var(--page);
    min-height: 100vh;
}

header {
    padding-bottom: 2rem;
    margin-bottom: 2rem;
    border-bottom: 1px solid var(--edge);
    position: relative;
}
header::after {
    content: "";
    position: absolute;
    bottom: -1px; left: 0;
    width: 100%; height: 2px;
    background: var(--accent);
}
.hdr-row {
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
    margin-bottom: 1.8rem;
}
.brand {
    font-family: var(--mono);
    font-size: .85rem;
    font-weight: 500;
    letter-spacing: .22em;
    color: var(--accent);
}
.brand span { color: var(--bright); }
.brand-dept {
    font-size: .6rem;
    letter-spacing: .18em;
    color: var(--txt3);
    margin-top: 2px;
}
.hdr-right {
    display: flex;
    align-items: center;
    gap: 1.2rem;
}
.maas-link {
    font-family: var(--mono);
    font-size: .72rem;
    color: var(--accent);
    text-decoration: none;
    padding: .35rem .8rem;
    border: 1px solid var(--edge2);
    border-radius: 6px;
    transition: all .15s;
}
.maas-link:hover {
    background: var(--card2);
    border-color: var(--accent);
}
.overall .badge { font-size: 1rem; padding: .45rem 1.4rem; }
h1 {
    font-size: 1.9rem;
    font-weight: 700;
    color: var(--bright);
    letter-spacing: -.03em;
    line-height: 1.2;
}
.subtitle {
    margin-top: .6rem;
    font-size: .85rem;
    color: var(--txt2);
    display: flex;
    align-items: center;
    gap: .6rem;
    flex-wrap: wrap;
}
.subtitle .hl {
    font-family: var(--mono);
    color: var(--accent);
    font-weight: 500;
}
.sep-dot {
    width: 3px; height: 3px;
    border-radius: 50%;
    background: var(--txt3);
    display: inline-block;
}

.badge {
    display: inline-block;
    padding: .2rem .65rem;
    border-radius: 5px;
    font-size: .72rem;
    font-weight: 600;
    font-family: var(--mono);
    letter-spacing: .06em;
}
.badge-pass { background: var(--green-bg); color: var(--green); border: 1px solid var(--green-bd); }
.badge-warn { background: var(--amber-bg); color: var(--amber); border: 1px solid var(--amber-bd); }
.badge-fail { background: var(--red-bg); color: var(--red); border: 1px solid var(--red-bd); }
.badge-na   { background: var(--card); color: var(--txt3); border: 1px solid var(--edge); }

section { margin-bottom: 2rem; }
.section-label {
    font-size: .62rem;
    font-weight: 600;
    letter-spacing: .2em;
    text-transform: uppercase;
    color: var(--txt3);
    margin-bottom: .8rem;
    padding-left: 2px;
}
.section-label::before {
    content: "";
    display: inline-block;
    width: 8px; height: 2px;
    background: var(--accent);
    margin-right: 8px;
    vertical-align: middle;
}

.action-required {
    border: 1px solid var(--red-bd);
    background: var(--red-bg);
    border-left: 3px solid var(--red);
    border-radius: 4px;
    padding: .9rem 1.1rem;
    margin: 1.25rem 0;
}
.action-required .action-title {
    color: var(--red);
    font-weight: 600;
    letter-spacing: .04em;
    text-transform: uppercase;
    font-size: .78rem;
    margin-bottom: .5rem;
}
.action-required p { margin: .35rem 0; font-size: .84rem; line-height: 1.5; }
/* Raw log blocks. Pre-formatted text scrolls inside its own box -- the page
   body must never scroll sideways. */
.ev-block {
    border: 1px solid var(--edge);
    border-radius: 6px;
    margin: 8px 0;
    background: var(--card2);
}
.ev-block > summary {
    cursor: pointer;
    padding: 8px 12px;
    font-size: 12px;
    font-weight: 600;
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    align-items: baseline;
}
.ev-block > summary:hover { background: var(--card); }
.ev-block[open] > summary { border-bottom: 1px solid var(--edge); }
.ev-meta { font-weight: 400; font-size: 11px; color: var(--txt2); }
.ev-trunc {
    font-weight: 600;
    font-size: 11px;
    color: var(--amber);
    margin-left: auto;
}
.ev-pre {
    margin: 0;
    padding: 10px 12px;
    max-height: 460px;
    overflow: auto;
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    font-size: 10.5px;
    line-height: 1.45;
    white-space: pre;
    tab-size: 8;
}

.acc-detail {
    border: 1px solid var(--edge);
    border-radius: 4px;
    margin-top: .5rem;
    background: var(--card);
}
.acc-detail > summary {
    cursor: pointer;
    padding: .5rem .75rem;
    font-size: .82rem;
    letter-spacing: .02em;
}
.acc-detail > summary:hover { background: var(--card2); }
.acc-detail[open] > summary { border-bottom: 1px solid var(--edge); }
.acc-body {
    padding: .75rem;
    display: grid;
    grid-template-columns: minmax(220px, 1fr) 2fr;
    gap: 1rem;
    align-items: start;
}
@media (max-width: 900px) { .acc-body { grid-template-columns: 1fr; } }
.acc-verdict td { border-top: 2px solid var(--edge2); padding-top: .4rem; }
.vcards {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
    gap: .75rem;
    margin-bottom: 1.2rem;
}
.vcard {
    background: var(--card);
    border: 1px solid var(--edge);
    border-radius: 10px;
    padding: .9rem 1.1rem;
    display: flex;
    flex-direction: column;
    gap: .4rem;
}
.vcard-label { font-size: .8rem; font-weight: 500; }
.card-meta { font-size: .68rem; color: var(--txt3); font-family: var(--mono); }

.ok-msg { font-size: .82rem; color: var(--green); font-weight: 500; padding: .3rem 0; }
.sev {
    display: inline-block;
    padding: .12rem .45rem;
    border-radius: 3px;
    font-size: .62rem;
    font-weight: 600;
    font-family: var(--mono);
}
.sev-crit { background: var(--red-bg); color: var(--red); }
.sev-warn { background: var(--amber-bg); color: var(--amber); }
.sev-info { background: var(--card2); color: var(--txt2); }

.two-col {
    display: grid;
    grid-template-columns: 3fr 2fr;
    gap: 2rem;
    align-items: start;
}

.tbl {
    width: 100%;
    border-collapse: collapse;
    font-size: .78rem;
}
.tbl th, .tbl td {
    padding: .4rem .55rem;
    text-align: left;
    border-bottom: 1px solid var(--edge);
}
.tbl th {
    background: var(--card2);
    color: var(--txt2);
    font-size: .6rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: .06em;
    white-space: nowrap;
}
.tbl tbody tr { transition: background .1s; }
.tbl tbody tr:hover { background: rgba(56,189,248,.03); }
.tbl tbody tr:last-child td { border-bottom: none; }

.tbl.gpu, .tbl.issues, .tbl.matrix {
    background: var(--card);
    border: 1px solid var(--edge);
    border-radius: 10px;
    overflow: hidden;
}
.tbl-wrap { overflow-x: auto; border-radius: 10px; }

.tbl.kv { background: var(--card); border: 1px solid var(--edge); border-radius: 10px; overflow: hidden; }
.kv-key {
    color: var(--txt2);
    font-weight: 500;
    font-size: .72rem;
    width: 130px;
    white-space: nowrap;
}

.table-note {
    font-size: .78rem;
    color: var(--txt2);
    margin-bottom: .6rem;
    font-family: var(--mono);
}

.mono { font-family: var(--mono); font-size: .74rem; }
.tiny { font-size: .65rem; }
.nowrap { white-space: nowrap; }
.dim { color: var(--txt2); }
.hl { color: var(--accent); font-family: var(--mono); }
.alert { color: var(--red); font-weight: 600; }
.warn { color: #FF8C00; font-weight: 600; }

.numa-node-row {
    display: flex;
    gap: .6rem;
    align-items: center;
    padding: .5rem .8rem;
    background: var(--card);
    border: 1px solid var(--edge);
    border-radius: 8px;
    margin-bottom: .4rem;
    flex-wrap: wrap;
}
.numa-id {
    font-family: var(--mono);
    font-weight: 500;
    font-size: .72rem;
    color: var(--accent);
    min-width: 60px;
}
.numa-gpu {
    font-family: var(--mono);
    font-size: .68rem;
    padding: .15rem .45rem;
    background: var(--card2);
    border: 1px solid var(--edge);
    border-radius: 4px;
    color: var(--txt);
}

.stress-bar {
    display: flex;
    gap: 2rem;
    padding: .55rem 1rem;
    background: var(--card);
    border: 1px solid var(--edge);
    border-radius: 8px;
    font-size: .82rem;
    color: var(--txt2);
    margin-bottom: .8rem;
}
.stress-bar strong { color: var(--bright); }

.matrix-summary {
    font-size: .78rem;
    margin-bottom: .6rem;
    display: flex;
    gap: .6rem;
}

.tbl.matrix th { text-align: center; min-width: 38px; }
.tbl.matrix td { text-align: center; padding: .3rem .35rem; }
.test-name {
    text-align: left !important;
    font-family: var(--mono);
    font-size: .7rem;
    font-weight: 500;
    white-space: nowrap;
}
.dot-pass { color: var(--green); font-weight: 700; font-size: .85rem; }
.dot-fail { color: var(--red); font-weight: 700; font-size: .85rem; }
.dot-warn { color: var(--amber); font-weight: 700; font-size: .85rem; }
.dot-skip { color: var(--txt3); font-size: .72rem; }

.st-pass { color: var(--green); font-weight: 600; }
.st-fail { color: var(--red); font-weight: 600; }
.st-warn { color: var(--amber); font-weight: 600; }
.st-skip { color: var(--txt3); }

footer {
    display: flex;
    justify-content: space-between;
    align-items: flex-end;
    border-top: 1px solid var(--edge);
    padding-top: 1rem;
    margin-top: 2rem;
    font-family: var(--mono);
    font-size: .65rem;
    color: var(--txt3);
    position: relative;
}
footer::before {
    content: "";
    position: absolute;
    top: -1px; left: 0;
    width: 60px; height: 2px;
    background: var(--accent);
}
.foot-right { text-align: right; line-height: 1.7; }
'''


# ---------------------------------------------------------------------------
# FILE-BASED FALLBACK (backward compat with v2.2)
# ---------------------------------------------------------------------------

def load_json_file(path: str) -> dict | None:
    if path is None:
        return None
    try:
        if path == "-":
            return json.load(sys.stdin)
        with open(path) as f:
            return json.load(f)
    except (json.JSONDecodeError, FileNotFoundError, IOError) as e:
        print(f"Warning: Could not load {path}: {e}", file=sys.stderr)
        return None


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser(
        description="NexGen GPU Commissioning Report Generator v3.2",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # MAAS API mode (recommended) -- outputs EXAMPLE-GPU-001-MAAS-validation.html
  export MAAS_URL=http://maas.example.com:5240/MAAS
  export MAAS_API_KEY=consumer:token:secret
  python3 nexgen-gpu-report.py --host EXAMPLE-GPU-001

  # Custom output name
  python3 nexgen-gpu-report.py --host EXAMPLE-GPU-001 -o custom-name.html

  # File-based mode (backward compatible with v2.2)
  python3 nexgen-gpu-report.py \\
    --install 97.json --inventory 98.json --stress 99.json \\
    -o report.html
""",
    )

    # MAAS API mode
    maas_grp = p.add_argument_group("MAAS API mode")
    maas_grp.add_argument(
        "--host", metavar="HOSTNAME",
        help="Machine hostname to look up in MAAS (e.g., EXAMPLE-GPU-001)",
    )
    maas_grp.add_argument(
        "--maas-url", metavar="URL",
        help="MAAS base URL (default: $MAAS_URL env var)",
    )
    maas_grp.add_argument(
        "--api-key", metavar="KEY",
        help="MAAS API key consumer:token:secret (default: $MAAS_API_KEY env var)",
    )

    # File-based mode (backward compat)
    file_grp = p.add_argument_group("file-based mode (backward compatible)")
    file_grp.add_argument("--install", metavar="FILE", help="90-install JSON file")
    file_grp.add_argument("--config", metavar="FILE", help="91-mig-ecc-config JSON file")
    file_grp.add_argument("--burnin", metavar="FILE", help="92-burn-in JSON file")
    file_grp.add_argument("--inventory", metavar="FILE", help="98-inventory JSON file")
    file_grp.add_argument("--stress", metavar="FILE", help="99-stress-test JSON file")

    # Output
    p.add_argument("--output", "-o", metavar="FILE", help="Output HTML file (default: stdout)")
    p.add_argument("--quiet", "-q", action="store_true", help="Suppress progress messages")
    p.add_argument("--version", action="version", version=f"%(prog)s {__version__}")

    args = p.parse_args()

    global _quiet
    _quiet = args.quiet

    if args.host:
        # === MAAS API MODE ===
        maas_url = args.maas_url or os.environ.get("MAAS_URL", "")
        api_key = args.api_key or os.environ.get("MAAS_API_KEY", "")

        if not maas_url:
            p.error(
                "--host requires MAAS URL. Set --maas-url or export MAAS_URL=..."
            )
        if not api_key:
            p.error(
                "--host requires MAAS API key. Set --api-key or export MAAS_API_KEY=..."
            )

        data = fetch_from_maas(args.host, maas_url, api_key)

        html = generate_report(
            install=data["install"],
            inventory=data["inventory"],
            stress=data["stress"],
            config=data.get("config"),
            burnin=data.get("burnin"),
            maas_url=maas_url,
            system_id=data["system_id"],
            machine=data["machine"],
            hardware_info=data["hardware_info"],
            nics=data["nics"],
            block_devices=data["block_devices"],
            storage_devices=data["storage_devices"],
            pci_devices=data["pci_devices"],
            numa_nodes_maas=data["numa_nodes_maas"],
            dimms=data["dimms"],
            all_scripts=data["all_scripts"],
            cpu_topology=data.get("cpu_topology"),
            script_logs=data.get("script_logs"),
            hostname_override=data["hostname"],
        )

    elif any([args.install, args.inventory, args.stress, args.config, args.burnin]):
        # === FILE-BASED MODE (backward compatible) ===
        log("File-based mode (no MAAS API)")
        install = load_json_file(args.install)
        inventory = load_json_file(args.inventory)
        stress = load_json_file(args.stress)
        config = load_json_file(args.config)
        burnin = load_json_file(args.burnin)

        if not any([install, inventory, stress, config, burnin]):
            print("Error: No valid JSON loaded from files", file=sys.stderr)
            sys.exit(1)

        html = generate_report(
            install=install,
            inventory=inventory,
            stress=stress,
            config=config,
            burnin=burnin,
            maas_url=args.maas_url,
        )

    else:
        p.error("Provide --host for MAAS API mode, or --install/--inventory/--stress for file mode")

    # Write output — default to reports/ directory relative to repo root
    output_path = args.output
    if not output_path and args.host:
        reports_dir = Path(__file__).resolve().parent.parent / "reports"
        reports_dir.mkdir(exist_ok=True)
        output_path = str(reports_dir / f"{args.host}-MAAS-validation.html")

    if output_path:
        out = Path(output_path)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(html, encoding="utf-8")
        log(f"Report written: {out}")
    else:
        print(html)


if __name__ == "__main__":
    main()
