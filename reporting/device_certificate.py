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

__version__ = "3.2.1"

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

GPU_SCRIPTS = [
    "97-nexgen-gpu-install-580-12.8",
    "98-nexgen-gpu-inventory",
    "99-nexgen-gpu-stress-test",
]

# Map short names used internally to the MAAS script names
SCRIPT_ALIASES = {
    "install":   "97-nexgen-gpu-install-580-12.8",
    "inventory": "98-nexgen-gpu-inventory",
    "stress":    "99-nexgen-gpu-stress-test",
}


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
        """Fetch a specific script's stdout and parse as JSON."""
        try:
            data = self.get_commissioning_results(system_id, [script_name])
            for result in data.get("results", []):
                if result.get("name") == script_name:
                    stdout_b64 = result.get("stdout", "")
                    if stdout_b64:
                        raw = base64.b64decode(stdout_b64).decode("utf-8", errors="replace")
                        # Our scripts output JSON to stdout but logs to stderr.
                        # The stdout may have leading/trailing non-JSON text if
                        # MAAS captured combined output. Try to extract JSON.
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

    def get_script_stdout_raw(self, system_id: str, script_name: str) -> str | None:
        """Download raw stdout text for a script (no base64)."""
        try:
            resp = self._get(
                f"nodes/{system_id}/results/current-commissioning/",
                {
                    "op": "download",
                    "output": "stdout",
                    "filetype": "txt",
                    "filters": script_name,
                },
            )
            return resp.text if resp.text.strip() else None
        except Exception as e:
            print(f"Warning: Could not download {script_name}: {e}", file=sys.stderr)
            return None


def _extract_json(text: str) -> dict | None:
    """Extract the first valid JSON object from text that may have non-JSON content."""
    text = text.strip()
    # Try direct parse first
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    # Find first { and try progressively larger substrings
    start = text.find("{")
    if start < 0:
        return None
    depth = 0
    for i in range(start, len(text)):
        if text[i] == "{":
            depth += 1
        elif text[i] == "}":
            depth -= 1
            if depth == 0:
                try:
                    return json.loads(text[start : i + 1])
                except json.JSONDecodeError:
                    pass
    return None


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
        log(f"  97-install: loaded ({install_data.get('verdict', {}).get('overall', '?')})")
    else:
        log("  97-install: not found or no JSON output")

    inventory_data = client.get_script_json(system_id, SCRIPT_ALIASES["inventory"])
    if inventory_data:
        log(f"  98-inventory: loaded ({inventory_data.get('verdict', {}).get('overall', '?')})")
    else:
        log("  98-inventory: not found or no JSON output")

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
    if machine_resources:
        log(f"  machine-resources: loaded ({len(machine_resources)} top-level keys)")
        # Enrich DIMM data from machine-resources if lshw failed
        if not dimms:
            dimms = parse_machine_resources_dimms(machine_resources)
            if dimms:
                log(f"  DIMMs (from machine-resources): {len(dimms)} slots")
        # Extract PCI devices
        pci_devices = extract_pci_devices(machine_resources)
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
    all_scripts = client.get_all_commissioning_scripts(system_id)
    log(f"  {len(all_scripts)} total commissioning scripts")

    return {
        "install": install_data,
        "inventory": inventory_data,
        "stress": stress_data,
        "machine": details,
        "hardware_info": hw_info,
        "nics": nics,
        "block_devices": block_devs,
        "storage_devices": storage_devs,
        "pci_devices": pci_devices,
        "numa_nodes_maas": numa_nodes,
        "dimms": dimms,
        "all_scripts": all_scripts,
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
        ("retired_pages_sbit", "RS"),
        ("retired_pages_dbit", "RD"),
    ]:
        val = ecc.get(key)
        if val is not None and isinstance(val, (int, float)) and val > 0:
            parts.append(f'<span class="alert">{label}:{val}</span>')
    if parts:
        return " ".join(parts)
    return '<span class="dim">Clean</span>'


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
    hostname_override: str | None = None,
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
    stages = [("Install", install), ("Inventory", inventory), ("Stress Test", stress)]
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

    # Derive per-stage verdicts: if all issues for a stage were filtered out,
    # upgrade from WARN to PASS (FAIL stays as-is since those are real failures)
    remaining_sources = {i["source"] for i in all_issues}
    verdicts = []
    for label, data in stages:
        if data:
            raw = data.get("verdict", {}).get("overall", "N/A")
            if raw == "WARN" and label not in remaining_sources:
                raw = "PASS"
            verdicts.append((label, raw))
        else:
            verdicts.append((label, "N/A"))

    pri = {"FAIL": 0, "WARN": 1, "PASS": 2, "N/A": 3}
    overall = min(verdicts, key=lambda x: pri.get(x[1], 3))[1]

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
    total_threads = mach.get("cpu_count", sys_info.get("cpu_total_threads", 0))
    # Derive socket count and cores from NUMA topology
    num_sockets = len(numa_nodes_maas) if numa_nodes_maas else 0
    total_cores = sum(len(n.get("cores", [])) for n in numa_nodes_maas) if numa_nodes_maas else 0
    if num_sockets > 1 and total_cores:
        cpu_label = f'{num_sockets}&times; {escape(str(cpu_str))} &mdash; {total_cores} cores / {total_threads} threads'
    elif total_cores:
        cpu_label = f'{escape(str(cpu_str))} &mdash; {total_cores} cores / {total_threads} threads'
    else:
        cpu_label = f'{escape(str(cpu_str))} &mdash; {total_threads} threads'
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
            <td class="mono">{g.get("serial","--")}</td>
            <td class="nowrap">{pcie_str(g)}</td>
            <td>{v(g.get("numa_node"))}</td>
            <td>{v(g.get("temp_idle_c"),"&deg;C")}</td>
            <td class="nowrap">{v(g.get("power_draw_w"),"W")} / {v(g.get("power_limit_w"),"W")}</td>
            <td>{ecc_summary(ecc)}</td>
            {stress_cells}
        </tr>'''

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

            core_str = f'{len(cores)} cores' if cores else "?"

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

.vcards {
    display: grid;
    grid-template-columns: repeat(3, 1fr);
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
    file_grp.add_argument("--install", metavar="FILE", help="97-install JSON file")
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
            hostname_override=data["hostname"],
        )

    elif any([args.install, args.inventory, args.stress]):
        # === FILE-BASED MODE (backward compatible) ===
        log("File-based mode (no MAAS API)")
        install = load_json_file(args.install)
        inventory = load_json_file(args.inventory)
        stress = load_json_file(args.stress)

        if not any([install, inventory, stress]):
            print("Error: No valid JSON loaded from files", file=sys.stderr)
            sys.exit(1)

        html = generate_report(
            install=install,
            inventory=inventory,
            stress=stress,
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
