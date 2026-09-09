#!/bin/bash
# Full offline test suite. No GPU required -- everything runs against fixtures
# and stub tooling.
cd "$(dirname "$0")/.." || exit 1
rc=0
hdr() { printf '\n\033[1m== %s\033[0m\n' "$1"; }

hdr "Syntax"
for f in commissioning-scripts/*.sh; do
    bash -n "$f" && printf '  ok    %s\n' "$f" || { printf '  FAIL  %s\n' "$f"; rc=1; }
done
python3 -m py_compile reporting/device_certificate.py \
    && echo "  ok    reporting/device_certificate.py" || rc=1
python3 -m py_compile reporting/sanitisation_certificate.py \
    && echo "  ok    reporting/sanitisation_certificate.py" || rc=1

hdr "Report generator runs on the oldest Python we deploy to"
python3 tests/check-python-compat.py || rc=1

hdr "MAAS metadata block is parseable by MAAS"
python3 tests/validate-maas-metadata.py || rc=1

hdr "Script stdout must be parseable JSON with nothing leaked ahead of it"
export PATH="$PWD/tests/stubs:$PATH" STUB_GPU_COUNT=2
rm -f /tmp/stub-*
for spec in "91-nexgen-gpu-mig-ecc-config.sh:" \
            "92-nexgen-gpu-inventory.sh:" \
            "98-nexgen-gpu-stress-test.sh:DCGM_FIXTURE=tests/fixtures/dcgm/all-pass.json" \
            "99-nexgen-gpu-burn-in.sh:BURN_DURATION=2 BURN_SAMPLE_INTERVAL=1 STUB_LOAD_SLEEP=1"; do
    s="${spec%%:*}"; envs="${spec#*:}"
    rm -f /tmp/stub-*
    if env $envs bash "commissioning-scripts/$s" 2>/dev/null | jq -e . >/dev/null 2>&1; then
        printf '  ok    %s\n' "$s"
    else
        printf '  FAIL  %s -- stdout is not clean JSON\n' "$s"; rc=1
    fi
done

hdr "Chassis telemetry separates a slow fan from a hot card"
# GPU telemetry says a card got hot; it cannot say why. A fan slower than its
# peers is serviceable, a warm inlet is the room's problem, and an even chassis
# with one hot card is the card. Three different conversations with a buyer,
# and none of them answerable after the run is over.
chassis_case() { # label, extra env, jq filter, expected
    local label="$1" envs="$2" filt="$3" want="$4" got
    rm -f /tmp/stub-*
    # Defaults first, caller overrides last -- otherwise a case that sets
    # CHASSIS_SAMPLE_INTERVAL=0 silently gets the default back.
    got=$(env STUB_GPU_COUNT=2 BURN_DURATION=2 BURN_SAMPLE_INTERVAL=1 \
          CHASSIS_SAMPLE_INTERVAL=1 STUB_LOAD_SLEEP=1 \
          PATH="$PWD/tests/stubs:$PWD/tests/stubs-hw:$PATH" $envs \
          bash commissioning-scripts/99-nexgen-gpu-burn-in.sh 2>/dev/null \
          | jq -r "$filt")
    if [[ "$got" == "$want" ]]; then printf '  ok    %-50s -> %s\n' "$label" "$got"
    else printf '  FAIL  %-50s -> got %s, want %s\n' "$label" "$got" "$want"; rc=1; fi
}
# Measured on a live ESC8000A-E12P: the GPU bank sits around 9880 RPM and the
# system bank around 8300. They are different fans on different duty curves, so
# pooling them gives a ~21% spread with nothing wrong. Spread is therefore per
# bank. What the check is actually for is simpler than the spread, though:
# whether every fan is turning at all, and whether it responds to load.
chassis_case "every fan is turning" "" '.burn_in.chassis.fans.all_spinning' "true"
chassis_case "a stopped fan is caught and named" "STUB_DEAD_FAN=2" \
    '.burn_in.chassis.fans.stopped | join(",")' "GPU_FAN2"
chassis_case "and stopping one fan clears all_spinning" "STUB_DEAD_FAN=2" \
    '.burn_in.chassis.fans.all_spinning' "false"
chassis_case "healthy chassis: every bank holds together" "" '[.burn_in.chassis.fans.banks[].spread_pct] | max < 10' "true"
chassis_case "banks are separated, not pooled" "" '.burn_in.chassis.fans.banks | length' "2"
chassis_case "one slow fan shows in its own bank" "STUB_SLOW_FAN=3" '.burn_in.chassis.fans.worst_spread_pct > 50' "true"
chassis_case "and the slow fan is named, not just counted" "STUB_SLOW_FAN=3" '.burn_in.chassis.fans.banks[] | select(.bank=="GPU_FAN") | .slowest_sensor' "GPU_FAN3"
chassis_case "all 11 fans counted across both banks" "" '.burn_in.chassis.fans.fan_count' "11"
chassis_case "inlet air is recorded, so ambient is evidenced" "STUB_INLET=33"     '.burn_in.chassis.inlet_c.mean == 33' "true"
chassis_case "sampling off yields null, not a fabricated zero"     "CHASSIS_SAMPLE_INTERVAL=0" '.burn_in.chassis' "null"
# The BMC is not guaranteed to answer, and chassis telemetry must never take a
# 1-hour load run down with it. Isolate the variable: fail ipmitool only, and
# leave the rest of the stubs in place.
chassis_case "an unresponsive BMC does not fail the load run" "STUB_IPMI_FAIL=1" \
    '.burn_in.tool' "gpu-burn"
chassis_case "and the chassis block is null, not a fabricated reading" "STUB_IPMI_FAIL=1" \
    '.burn_in.chassis' "null"

hdr "A sampler must not hold the script's stdout open after it exits"
# A backgrounded subshell inherits the script's stdout. Killing the subshell
# orphans the `sleep` it spawned, which keeps that descriptor open, and anything
# reading the output -- MAAS capturing the result, or $( ) here -- blocks until
# every writer closes. The run looks hung for a full sample interval after it
# has finished. Caught when the suite went from minutes to 19 of them.
for spec in "99:99-nexgen-gpu-burn-in.sh:BURN_DURATION=2 BURN_SAMPLE_INTERVAL=1 STUB_LOAD_SLEEP=1" \
            "98:98-nexgen-gpu-stress-test.sh:DCGM_FIXTURE=tests/fixtures/dcgm/all-pass.json"; do
    lbl="${spec%%:*}"; rest="${spec#*:}"; script="${rest%%:*}"; envs="${rest#*:}"
    rm -f /tmp/stub-*
    t0=$(date +%s%N)
    # Default CHASSIS_SAMPLE_INTERVAL (30s) on purpose: that is the interval a
    # leaked descriptor would make us wait for. Captured via $( ), like MAAS.
    out=$(env STUB_GPU_COUNT=2 $envs \
          PATH="$PWD/tests/stubs:$PWD/tests/stubs-hw:$PATH" \
          bash "commissioning-scripts/$script" 2>/dev/null)
    ms=$(( ($(date +%s%N) - t0) / 1000000 ))
    if [[ $ms -lt 15000 ]] && printf '%s' "$out" | jq -e . >/dev/null 2>&1; then
        printf '  ok    %s returns as soon as it is done (%sms)\n' "$lbl" "$ms"
    else
        printf '  FAIL  %s took %sms -- stdout held open by a leaked sampler\n' "$lbl" "$ms"; rc=1
    fi
done

hdr "NVLink topology comes from the peer matrix, not a port count"
# `nvidia-smi nvlink --status` enumerates every NVLink PORT the silicon has,
# connected or not. Line-counting it reported "139 active links" on a machine
# whose real topology is four 2-way bridges of 12 links -- 48 -- and that
# number reached a customer-facing report. The topo matrix states the bridges
# directly, in the same terms the specification uses.
nvcheck() { # label, STUB_NVLINK value, expect present/bridges/per/total
    local label="$1" mode="$2" want="$3" got
    rm -f /tmp/stub-*
    got=$(env STUB_NVLINK="$mode" STUB_GPU_COUNT=8 \
          PATH="$PWD/tests/stubs:$PWD/tests/stubs-hw:$PATH" \
          bash commissioning-scripts/92-nexgen-gpu-inventory.sh 2>/dev/null \
          | jq -r '.system | "\(.nvlink_present)/\(.nvlink_bridges)/\(.nvlink_links_per_bridge)/\(.nvlink_active_links)"')
    if [[ "$got" == "$want" ]]; then printf '  ok    %-52s -> %s\n' "$label" "$got"
    else printf '  FAIL  %-52s -> got %s, want %s\n' "$label" "$got" "$want"; rc=1; fi
}
nvcheck "4 x 2-way bridges reads as 4 bridges of 12, not 144" bridges "true/4/12/48"
nvcheck "no bridges reads as absent, not as idle ports"       ""        "false/0/0/0"
nvcheck "a fully-meshed baseboard still counts correctly"     1         "true/28/18/504"

hdr "Cabling check (80) survives a missing jq"
# Script 80 sorts ahead of 90, which is the only script that installs jq, and
# the MAAS image does not ship it -- so this failed preflight on every machine.
# Worse, emit_fatal was itself built on jq, so the handler meant to report the
# failure died with "command not found" and emitted nothing: MAAS recorded a
# failure with no verdict to explain it. Observed on a real commissioning run.
python3 - <<'PYEOF' || rc=1
import json, os, shutil, subprocess, sys, tempfile

rc = 0
def check(ok, label):
    global rc
    print(("  ok    " if ok else "  FAIL  ") + label)
    if not ok:
        rc = 1

# A PATH with the basics but deliberately no jq, and an apt-get that cannot
# install one -- exactly the state the ephemeral image is in.
tmp = tempfile.mkdtemp()
try:
    for b in ("bash", "date", "mkdir", "rm", "sed", "grep", "awk", "cat"):
        src = shutil.which(b)
        if src:
            os.symlink(src, os.path.join(tmp, b))
    apt = os.path.join(tmp, "apt-get")
    with open(apt, "w") as fh:
        fh.write("#!/bin/bash\nexit 1\n")
    os.chmod(apt, 0o755)

    p = subprocess.run(
        ["bash", "commissioning-scripts/80-nexgen-network-cabling-verify.sh"],
        capture_output=True, text=True, env={"PATH": tmp})

    check(p.returncode != 0, "it still fails, because without jq it cannot run")
    try:
        doc = json.loads(p.stdout)
        parsed = True
    except Exception:
        doc, parsed = None, False
    check(parsed, "but stdout is parseable JSON, not an empty stream")
    if parsed:
        check(doc["verdict"]["overall"] == "FAIL",
              "and carries a FAIL verdict MAAS can record")
        check("jq" in doc["verdict"]["issues"][0]["issue"],
              "naming the missing tool, so the cause is in the report")
    check("command not found" not in p.stdout,
          "no shell error leaks onto the JSON stream")
finally:
    shutil.rmtree(tmp, ignore_errors=True)

# And the install itself must be declared, not left to a later script.
src = open("commissioning-scripts/80-nexgen-network-cabling-verify.sh").read()
check("REQUIRED_PKGS" in src and "apt-get install" in src.split("preflight()")[1][:600],
      "preflight installs its own tools rather than depending on script 90")
sys.exit(rc)
PYEOF

hdr "Chassis inventory (89) collects what the GPU scripts cannot"
# Its own stub dir, prepended: 89 needs an lspci that emits VPD and a dmidecode
# that answers -t 39, and both names are stubbed differently next door.
python3 - <<'PYEOF' || rc=1
import json, subprocess, sys, os

env = dict(os.environ)
env["PATH"] = os.path.abspath("tests/stubs-hw") + os.pathsep + env["PATH"]
env["MEGARAID_MAX_DID"] = "4"

def run(**extra):
    e = dict(env); e.update(extra)
    p = subprocess.run(["bash", "commissioning-scripts/89-nexgen-hw-inventory.sh"],
                       capture_output=True, text=True, env=e)
    return json.loads(p.stdout)

rc = 0
def check(ok, label):
    global rc
    print(("  ok    " if ok else "  FAIL  ") + label)
    if not ok:
        rc = 1

d = run()
check(d["bmc"]["firmware_revision"] == "1.01",
      "BMC firmware revision read in-band, with no credentials")

# SMBIOS carries the BIOS release date, never when the board was built. The
# FRU area does, and on these ASUS boards it is populated -- which matters
# because this BMC exposes no power-on-hours sensor at all.
check(d["fru"]["board_mfg_date"] == "Mon Oct  2 10:19:00 2023",
      "board manufacture date recovered from FRU")
check(d["fru"]["product_serial"] == "R9S0MD0000AB",
      "FRU product serial matches the chassis label serial")

# The last dmidecode block has no trailing blank line. An awk that only
# flushes on a blank line silently drops the final PSU, which on this 4-PSU
# chassis would understate the count in the Server Report.
check(len(d["psus"]) == 4 and all(p["present"] for p in d["psus"]),
      "all four PSU bays reported, including the last block")
check([p["serial_number"] for p in d["psus"]][-1] == "G1236302NA000000004",
      "PSU serial numbers captured, final bay included")

# The whole point of the megaraid passthrough: these two drives are invisible
# to lsblk, lshw and MAAS, and they are the ones actually being sold.
mem = [x for x in d["drives"] if x["reached_via"] == "megaraid"]
check(len(mem) == 2 and {x["serial_number"] for x in mem} ==
      {"S664NE0T000002", "S664NE0T000001"},
      "drives behind the MegaRAID volume are found and identified")
# These answer with exit status 4: the controller cannot pass the SMART status
# read through, but the identity and hours come back fine. Bailing on the exit
# code would lose the only record of the drives actually being sold.
check(all(x["power_on_hours"] == 18317 for x in mem),
      "hours read despite smartctl exiting non-zero on the status read")
nvme = [x for x in d["drives"] if "MZQL" in (x["model"] or "")][0]
check(nvme["power_on_hours"] == 18318 and nvme["percentage_used"] == 0,
      "NVMe hours read from the health log, not power_on_time")

# Schedule 4 wants server power-on hours in one cell. With no BMC sensor, the
# drives are the measurement that exists -- and a spread of 1 hour across three
# drives is what shows they were fitted from new, so the figure is the
# chassis's own rather than a replacement part's.
po = d["power_on"]
check(po["drive_hours_max"] == 18318 and po["drives_reporting_hours"] == 3,
      "drive hours rolled up into power_on for the Server Report cell")
check(po["drive_hours_spread"] == 1,
      "the spread is published, so 'fitted from new' is shown not assumed")
check(d["psu_sensors"]["bays"] == 4,
      "PSU bays counted independently from the BMC sensor list")

# DMI has no add-in card serials, which is why lshw reports a MAC instead.
# A dual-port card answers on both PCI functions with ONE board serial, so
# collapsing on serial is what keeps it from being counted as two cards.
cards = d["add_in_cards"]
check(len(cards) == 1 and cards[0]["serial_number"] == "MT0000XZ0000",
      "ConnectX-7 serial recovered from PCI VPD, both functions as one card")
check(cards[0]["pci_functions"] == ["09:00.0", "09:00.1"],
      "the functions the card answered on are kept")
check(all("RAID" not in c["class"] for c in cards),
      "the MegaRAID publishes no VPD and is not logged blank")

# Measured on the real ASMB11: no power-on-hours sensor exists. Saying so,
# and giving the SEL span instead, beats inventing a number.
check(d["power_on"]["hours"] is None and d["power_on"]["note"],
      "absent power-on-hours sensor is stated, not guessed")
check(d["power_on"]["sel_span_days"] == 809,
      "SEL span gives an elapsed-service figure when no sensor exists")
check(run(STUB_POH_SENSOR="yes")["power_on"]["hours"] == 18420,
      "the sensor is used when a BMC does expose one")

# Inventory metadata must never fail a commissioning run on its own.
bare = dict(env); bare["PATH"] = "/usr/bin:/bin"
p = subprocess.run(["bash", "commissioning-scripts/89-nexgen-hw-inventory.sh"],
                   capture_output=True, text=True, env=bare)
check(p.returncode == 0, "exits 0 with no tools available")
d2 = json.loads(p.stdout)
check(d2["verdict"]["overall"] == "WARN"
      and any("tools" in i["issue"] for i in d2["verdict"]["issues"]),
      "degrades to WARN and names the tools it could not install")

sys.exit(rc)
PYEOF

hdr "Storage sanitisation (95) erases nothing it cannot evidence"
# Its own stub dir, prepended ahead of stubs-hw: 95 needs an nvme that models
# SANICAP and the sanitize log, an lsblk that answers -J with a TRAN column,
# and a findmnt that can claim root is on a drive about to be erased. storcli
# lives in a third directory, because the script probes for it with command -v
# and a stub next to the others could never be absent.
python3 - <<'PYEOF' || rc=1
import json, os, shutil, subprocess, sys, tempfile

ROOT = os.path.abspath(".")
DECOMM = os.path.join(ROOT, "tests/stubs-decomm")
HW     = os.path.join(ROOT, "tests/stubs-hw")
RAID   = os.path.join(ROOT, "tests/stubs-raidcli")
GO     = "SANITIZE ALL MEDIA"

base = dict(os.environ)
base["PATH"] = DECOMM + os.pathsep + HW + os.pathsep + base["PATH"]
base["SANITIZE_POLL_INTERVAL"] = "0"      # clamped to 1s by the script
base["MEGARAID_MAX_DID"] = "4"
# Off unless a case is about it: the stubbed download cannot match the hash
# pinned for the real Broadcom package, and that finding would otherwise
# colour every unrelated case.
base["INSTALL_STORCLI"] = "false"
# Its own state directory. The stubs record what they would have issued, and
# sharing fixed /tmp paths meant two runs at once clobbered each other -- which
# surfaced as a different set of failures on every run.
STATE = tempfile.mkdtemp(prefix="sanitize-state-")
base["STUB_STATE_DIR"] = STATE
CALLS = os.path.join(STATE, "stub-destructive-calls")

rc = 0
def check(ok, label):
    global rc
    print(("  ok    " if ok else "  FAIL  ") + label)
    if not ok:
        rc = 1

def run(raid_cli=False, **extra):
    for f in os.listdir(STATE):
        p = os.path.join(STATE, f)
        try:
            shutil.rmtree(p) if os.path.isdir(p) else os.unlink(p)
        except OSError:
            pass
    e = dict(base)
    if raid_cli:
        e["PATH"] = RAID + os.pathsep + e["PATH"]
    e.update({k: str(v) for k, v in extra.items()})
    p = subprocess.run(["bash", "commissioning-scripts/95-nexgen-storage-sanitize.sh"],
                       capture_output=True, text=True, env=e)
    return p, json.loads(p.stdout)

def erased():
    """Destructive commands the run would have issued, in order."""
    try:
        with open(CALLS) as fh:
            return [l.strip() for l in fh if l.strip()]
    except IOError:
        return []

def media(d, **match):
    return [m for m in d["media"] if all(m.get(k) == v for k, v in match.items())]

def nvme(d):
    return media(d, device_type="nvme")[0]

# --- The default must be inert -------------------------------------------
# MAAS injects no environment, so the confirmation is a constant in the file.
# An unedited upload sitting in a commissioning set must inventory and report
# and touch nothing.
p, d = run(STUB_SG_PASSTHROUGH=1)
check(p.stdout.lstrip().startswith("{") and d["report_metadata"]["script_name"] == "storage-sanitize",
      "stdout is clean JSON with nothing leaked ahead of it")
check(d["report_metadata"]["mode"] == "audit" and erased() == [],
      "audit mode is the default and issues no destructive command")
check(nvme(d)["method"] == "nvme-sanitize-crypto" and nvme(d)["result"] == "not attempted",
      "audit mode still plans a method per device, without running it")

# A confirmation phrase that nearly matches is not a confirmation. This is the
# difference between a typo and an erase.
_, d = run(STUB_SG_PASSTHROUGH=1, SANITIZE_CONFIRM="sanitize all media")
check(d["report_metadata"]["mode"] == "audit" and erased() == [],
      "a near-miss confirmation phrase stays in audit mode")

# --- Section 6: the environment gates the erase ---------------------------
_, d = run(STUB_SG_PASSTHROUGH=1, SANITIZE_CONFIRM=GO, STUB_ROOT_FSTYPE="ext4")
check(d["report_metadata"]["mode"] == "audit" and erased() == []
      and d["environment"]["is_ephemeral"] is False,
      "refuses when root is on persistent storage (section 6)")

_, d = run(STUB_SG_PASSTHROUGH=1, SANITIZE_CONFIRM=GO, STUB_MOUNTED_TARGET="nvme0n1")
check(d["report_metadata"]["mode"] == "audit" and erased() == [],
      "refuses when a device targeted for sanitisation is mounted")

_, d = run(STUB_SG_PASSTHROUGH=1)
check("ephemeral root" in (d["environment"]["attestation"] or ""),
      "the trusted-environment attestation is recorded, not merely checked")

# --- Section 7: an absent device is not an empty one ----------------------
_, d = run(STUB_SG_PASSTHROUGH=1, SANITIZE_CONFIRM=GO, STUB_NS_MISMATCH=1)
check(d["report_metadata"]["mode"] == "audit" and erased() == [],
      "refuses when nvme list and lsblk disagree on the namespace count")

_, d = run(STUB_SG_PASSTHROUGH=1, SANITIZE_CONFIRM=GO, STUB_NVME_NO_ID=1)
check(d["report_metadata"]["mode"] == "audit" and erased() == [],
      "refuses when a controller returns no identity")

_, d = run(STUB_SG_PASSTHROUGH=1, SANITIZE_CONFIRM=GO,
           STUB_DIRECT_SATA=1, STUB_NO_SERIAL=1)
check(d["report_metadata"]["mode"] == "audit" and erased() == [],
      "refuses when a physical device has no readable serial number")

# The drives a RAID controller hides are the ones most easily left out of an
# evidence register, because nothing the OS presents mentions them.
_, d = run(STUB_SG_PASSTHROUGH=1)
members = media(d, device_type="raid-member")
check(len(members) == 2 and all(m["serial_number"] for m in members),
      "drives behind the RAID controller are inventoried by serial")
check(len(media(d, device_type="raid-volume")) == 1
      and media(d, device_type="raid-volume")[0]["physical_media"] is False,
      "the RAID volume itself is recorded but not counted as physical media")

# util-linux before 2.37 has no MOUNTPOINTS column.
_, d = run(STUB_SG_PASSTHROUGH=1, STUB_NO_MOUNTPOINTS=1)
check(d["inventory"]["counts"]["physical_media"] == 3,
      "falls back to MOUNTPOINT on lsblk versions with no MOUNTPOINTS")

# --- Section 8: the method order, strongest first -------------------------
for caps, fna, want, level in (
        ("crypto", 0, "nvme-sanitize-crypto", "purge"),
        ("block",  0, "nvme-sanitize-block",  "purge"),
        ("none",   4, "nvme-format-ses2",     "purge"),
        ("none",   0, "none",                 "none")):
    _, d = run(STUB_SG_PASSTHROUGH=1, STUB_SANICAP=caps, STUB_FNA=fna)
    got = nvme(d)
    check(got["method"] == want and got["method_level"] == level,
          "sanicap=%-6s fna=%s -> %s" % (caps, fna, want))

# sg3-utils is in the Ubuntu archive and storcli is not, so the SCSI route is
# tried first. Both are purges; the point is which dependency is required.
_, d = run(STUB_SG_PASSTHROUGH=1)
check(all(m["method"] == "scsi-sanitize-crypto" for m in media(d, device_type="raid-member")),
      "a RAID member reachable on its own sg node is purged without a vendor tool")
_, d = run(raid_cli=True, STUB_SG_PASSTHROUGH=0)
check(d["controller_survey"]["raid_cli"] and d["controller_survey"]["commands_run"],
      "a staged RAID CLI is surveyed, read-only, when SANITIZE does not pass through")

# The survey output is parsed, not just dumped: Sanitize Support is the field
# that decides which verb reaches a Purge, and reading it by eye is how the
# wrong verb gets chosen. Format captured from the MR9560-16i in 183.
sv = d["controller_survey"]["drives"]
check(len(sv) == 2 and all(x["slot"].startswith("/c0/e252/s") for x in sv),
      "the per-slot survey is parsed out of the storcli text")
check(all(x["sed_capable"] == "Yes" and x["sanitize_crypto"] and x["sanitize_block"]
          for x in sv),
      "SED capability and both sanitize methods are decoded per drive")
check("cryptoerase" in d["controller_survey"]["purge_verb"]
      and "blockErase if the firmware refuses" in d["controller_survey"]["purge_verb"],
      "with CryptoErase reported, that verb is recommended -- with the FDE caveat")
_, d = run(raid_cli=True, STUB_SG_PASSTHROUGH=0, STUB_RAIDCLI_SAN="BlockErase")
check("blockErase" in d["controller_survey"]["purge_verb"]
      and "cryptoerase" not in d["controller_survey"]["purge_verb"],
      "a drive reporting only BlockErase gets BlockErase recommended")
_, d = run(raid_cli=True, STUB_SG_PASSTHROUGH=0, STUB_RAIDCLI_SAN="")
check("no drive-level sanitize" in d["controller_survey"]["purge_verb"],
      "a drive reporting no sanitize support is not offered a verb it lacks")
check(d["controller_survey"]["raw_survey_b64"],
      "and the raw survey text is kept as evidence alongside the parse")

# The buyer may want the mirror back, so the array is rebuilt rather than left
# deleted -- which means the geometry has to be captured BEFORE the volume goes,
# because deleting it destroys the configuration.
_, d = run(raid_cli=True, STUB_SG_PASSTHROUGH=0)
sv = d["controller_survey"]
check([v["vd"] for v in sv["virtual_disks"]] == [239],
      "the virtual-disk id is read from the controller, not assumed to be 0")
check(sv["virtual_disk_config_b64"] and sv["drive_list"] == "252:0,252:1",
      "the array geometry and its drive slots are captured before anything is deleted")
seq = d["remediation"][0]["routes"][0]["sequence"]
check("/c0/v239 del force" in " ".join(seq),
      "the sequence deletes the volume that actually exists")
check(any("add vd type=r1 drives=252:0,252:1" in x for x in seq),
      "and ends by recreating the same RAID 1 on the same slots")
check(seq.index([x for x in seq if "del force" in x][0])
      < seq.index([x for x in seq if "start sanitize" in x][0])
      < seq.index([x for x in seq if "add vd" in x][0]),
      "in the only order that works: delete, purge, then rebuild")
_, d = run(raid_cli=True, STUB_SG_PASSTHROUGH=0, STUB_RAIDCLI_VD=7)
check("/c0/v7 del force" in " ".join(d["remediation"][0]["routes"][0]["sequence"]),
      "a different controller reports a different id, and the sequence follows it")
_, d = run(raid_cli=True, STUB_SG_PASSTHROUGH=0, STUB_RAIDCLI_SLOT=0)
check(all(m["method"] == "none" and "could not resolve" in m["method_reason"]
          for m in media(d, device_type="raid-member")),
      "a RAID CLI that cannot resolve the serial addresses no drive at all")

# The fleet as it actually is: no sg passthrough, no vendor binary. Those two
# boot SSDs block release, and the audit says so before anything is destroyed.
_, d = run(STUB_SG_PASSTHROUGH=0)
rem = {x["serial_number"]: x for x in d["remediation"]}
check(d["verdict"]["overall"] == "FAIL"
      and set(rem) == {"S664NE0R123456", "S664NE0R123457"},
      "RAID members that cannot be purged in place reach the remediation register")
check(all(r["nist_level_reached"] == "none" for r in rem.values()),
      "and the NIST level they reached is stated, not implied")
# Never a destruction list: the drives are purge-capable, the controller is the
# obstacle, so what belongs here is the route to a Purge.
# Scanned per field, not over the whole blob: a route note may legitimately say
# a drive is kept rather than destroyed, and a substring match cannot tell the
# difference between proposing destruction and ruling it out.
proposals = [r["blocker"] for r in rem.values()] + \
            [rt["action"] for r in rem.values() for rt in r["routes"]]
check(not any("destro" in t.lower() or "quarantin" in t.lower() for t in proposals),
      "no blocker or route proposes destroying a drive")
check(all(any(rt["reaches"] == "purge" for rt in r["routes"]) for r in rem.values()),
      "every entry offers at least one route that reaches NIST Purge")
check(all(r["routes"][0]["order"] == 1 and "controller" in r["routes"][0]["action"].lower()
          for r in rem.values()),
      "routes are ordered, strongest first")
# The evidence that the drive itself is fine: IDENTIFY traverses the passthrough
# even though SANITIZE does not.
check(all(r["ata_security_supported"] is True for r in rem.values()),
      "ATA Security support is read through the megaraid passthrough")
_, d = run(STUB_SG_PASSTHROUGH=0, STUB_NO_ATA_SECURITY=1)
check(all(x["ata_security_supported"] is None for x in d["remediation"]),
      "a controller that will not pass it through leaves the capability unproven, not false")

# NIST levels, named as such on every device.
_, d = run(STUB_SG_PASSTHROUGH=1)
check(nvme(d)["nist_level"] == "purge"
      and d["report_metadata"]["required_level"] == "purge"
      and "800-88r1" in d["report_metadata"]["standard"],
      "the standard and the required level are recorded on the run")

# A Clear is not a Purge, and does not quietly become one.
_, d = run(STUB_SG_PASSTHROUGH=0, STUB_DIRECT_SATA=1, STUB_ATA_CAPS="none",
           STUB_SCSI_SANITIZE=0, SANITIZE_CONFIRM=GO, ALLOW_OVERWRITE_FALLBACK="true")
check(all(m["nist_level"] == "clear" for m in media(d, method="overwrite"))
      and d["verdict"]["overall"] == "FAIL",
      "a verified NIST Clear still fails the run, because the host leaves custody")
_, d = run(STUB_SG_PASSTHROUGH=0, STUB_DIRECT_SATA=1, STUB_ATA_CAPS="none",
           STUB_SCSI_SANITIZE=0, SANITIZE_CONFIRM=GO, ALLOW_OVERWRITE_FALLBACK="true",
           ACCEPT_NIST_CLEAR="true")
check(any(i["severity"] == "warning" and "Clear, not Purge" in i["issue"]
          for i in d["verdict"]["issues"]),
      "accepting Clear deliberately downgrades it to a warning, and says so")

# The controller survey: read-only, and it names what would finish the picture.
_, d = run(STUB_SG_PASSTHROUGH=0)
sv = d["controller_survey"]
check(sv["members_seen"] == 2 and sv["raid_cli"] is None
      and any("show all" in c for c in sv["to_complete_the_picture"]),
      "with no vendor CLI, the survey names the read-only commands that would answer it")
_, d = run(raid_cli=True, STUB_SG_PASSTHROUGH=0)
check(d["controller_survey"]["raid_cli"] and d["controller_survey"]["commands_run"],
      "with one staged, the survey runs it -- read-only")

# --- Section 9: completion is a number, not a sentence --------------------
# This is the flaw the runbook warns about in its own script: the wording of
# the sanitize log varies by nvme-cli version and by vendor.
_, d = run(STUB_SG_PASSTHROUGH=1, SANITIZE_CONFIRM=GO, STUB_SANITIZE_JSON=0,
           STUB_SANITIZE_LIES=1, STUB_SANITIZE_RESULT="fail")
check(nvme(d)["result"] == "failed" and nvme(d)["status_code"] == "3"
      and d["verdict"]["overall"] == "FAIL",
      "a log claiming success with SSTAT=3 is a failure, not a pass")

_, d = run(STUB_SG_PASSTHROUGH=1, SANITIZE_CONFIRM=GO, STUB_SANITIZE_JSON=0)
check(nvme(d)["result"] == "success",
      "an nvme-cli with no JSON sanitize-log still adjudicates from the text")
check(any("cannot be read as JSON" in i["issue"] for i in d["verdict"]["issues"]),
      "and says that it had to")

_, d = run(STUB_SG_PASSTHROUGH=1, SANITIZE_CONFIRM=GO, STUB_SANITIZE_TICKS=2,
           SANITIZE_POLL_INTERVAL=1)
check(nvme(d)["result"] == "success",
      "a sanitise that reports progress before completing is waited out")

_, d = run(STUB_SG_PASSTHROUGH=1, SANITIZE_CONFIRM=GO,
           STUB_SANITIZE_RESULT="timeout", SANITIZE_MAX_WAIT=2)
check(nvme(d)["result"] == "timeout" and d["verdict"]["overall"] == "FAIL",
      "a sanitise that never completes times out and fails, rather than hanging")

# Section 14 wants a record for every drive, which aborting on the first
# failure guarantees some drives will not have.
_, d = run(STUB_SG_PASSTHROUGH=1, SANITIZE_CONFIRM=GO, STUB_NVME_CTRLS=2,
           STUB_SANITIZE_RESULT="fail")
ctrls = media(d, device_type="nvme")
check(len(ctrls) == 2 and all(c["started_at"] and c["evidence_b64"] for c in ctrls),
      "one device failing does not stop the sweep; every device keeps a record")

# --- An overwrite is a clear. It is never allowed to read as a purge ------
_, d = run(STUB_SG_PASSTHROUGH=0, STUB_DIRECT_SATA=1, STUB_ATA_CAPS="none",
           STUB_SCSI_SANITIZE=0, SANITIZE_CONFIRM=GO, ALLOW_OVERWRITE_FALLBACK="true")
check(all(m["method_level"] == "clear" for m in media(d, method="overwrite")),
      "a full-device overwrite is recorded as a clear, not a purge")
check(d["verdict"]["overall"] == "FAIL",
      "and a cleared drive still blocks release")
check(not any("destro" in json.dumps(i).lower() for i in d["verdict"]["issues"]),
      "no finding proposes destroying anything")

# --- Verification is identity, and says so --------------------------------
_, d = run(STUB_SG_PASSTHROUGH=1, STUB_DIRECT_SATA=1, SANITIZE_CONFIRM=GO,
           EXPECT_SYSTEM_SERIAL="R9S0MD0000AB")
check(d["verdict"]["overall"] == "PASS" and not d["remediation"]
      and d["inventory"]["counts"]["purged"] == d["inventory"]["counts"]["purge_planned"],
      "a run that purged and verified every device passes with nothing outstanding")
check(all((m.get("verification") or {}).get("serial_matches") is True
          for m in d["media"] if m["physical_media"]),
      "every device was re-identified after the erase")
check("corroborate" in nvme(d)["verification"]["note"],
      "the read-back is recorded as corroborating, not as the proof")

_, d = run(STUB_SG_PASSTHROUGH=1, SANITIZE_CONFIRM=GO, EXPECT_SYSTEM_SERIAL="WRONG-SERIAL")
check(d["report_metadata"]["mode"] == "audit" and erased() == [],
      "a pinned serial that does not match the chassis refuses the run")

# The pin is a list, so one script can be armed for a batch. A single serial is
# a list of one, and membership -- not equality -- is what decides.
for pin, want in (("R9S0MD0000AB", "sanitize"),
                  ("AAA R9S0MD0000AB BBB", "sanitize"),
                  ("AAA,R9S0MD0000AB", "sanitize"),
                  ("AAA BBB", "audit")):
    _, d = run(STUB_SG_PASSTHROUGH=1, SANITIZE_CONFIRM=GO, EXPECT_SYSTEM_SERIAL=pin)
    check(d["report_metadata"]["mode"] == want,
          "pin %-24s -> %s" % ("'" + pin + "'", want))
check(not erased(), "and the chassis outside the list is left untouched")

# --- The evidence has to outlive the maintenance environment --------------
_, d = run(STUB_SG_PASSTHROUGH=1, SANITIZE_CONFIRM=GO, EXPECT_SYSTEM_SERIAL="R9S0MD0000AB")
check(d["audit_log_b64"] and d["inventory"]["lsblk_b64"]
      and d["inventory"]["nvme_id_ctrl_b64"],
      "the audit log and raw inventory are embedded, not left in a RAM /root")
check(d["attestation"]["reviewer"] is None and len(d["attestation"]["out_of_scope"]) == 2,
      "the reviewer is left for a human, and the gaps are named")
check(not any("openstack" in str(x).lower() or "nova" in str(x).lower()
              for x in d["attestation"]["out_of_scope"]),
      "the evidence is about media, and says nothing about platform teardown")

# --- What the first real run on an ESC8000A-E12P node exposed -------------
# The BMC emulates a USB disk and a USB CDROM whether or not anything is
# attached. Listing one for physical destruction makes the register wrong:
# there is nothing in the chassis to pull out.
_, d = run(STUB_SG_PASSTHROUGH=1)
vm = media(d, device_type="bmc-virtual-media")
check(len(vm) == 1 and vm[0]["physical_media"] is False,
      "BMC virtual media is recorded but is not physical media in the chassis")
check(not any(q.get("serial_number") == vm[0]["serial_number"] for q in d["remediation"]),
      "and it is kept out of the remediation register")
_, d = run(STUB_VIRTUAL_MEDIA=0, STUB_SG_PASSTHROUGH=1)
check(not media(d, device_type="bmc-virtual-media"),
      "a machine with no virtual media simply has none recorded")

# An image mounted over Redfish is not local storage, cannot be erased here, and
# its provenance is unknown. That is a stop condition, not something to ignore.
_, d = run(STUB_VM_ATTACHED=1, STUB_SG_PASSTHROUGH=1, SANITIZE_CONFIRM=GO)
check(d["report_metadata"]["mode"] == "audit" and erased() == []
      and any("virtual media" in i["issue"] for i in d["verdict"]["issues"]),
      "an image attached to the BMC's virtual media refuses the run")

# nvme-cli 1.16 on the real ephemeral image takes -o json for id-ctrl but does
# not advertise it for sanitize-log. A capability read off a usage string is a
# guess, so it is settled by asking a controller for one.
_, d = run(STUB_SG_PASSTHROUGH=1)
check(d["environment"]["nvme_sanitize_log_json"] is True,
      "JSON sanitise-log support is probed by asking a controller, not by parsing help")
_, d = run(STUB_SG_PASSTHROUGH=1, STUB_SANITIZE_JSON=0)
check(d["environment"]["nvme_sanitize_log_json"] is False
      and any("cannot be read as JSON" in i["issue"] for i in d["verdict"]["issues"]),
      "an image that cannot return one says so, and names the version")

# An audit run that leaves no log is not much of a record.
_, d = run(STUB_SG_PASSTHROUGH=1)
import base64
alog = base64.b64decode(d["audit_log_b64"] or "").decode("utf-8", "replace")
check(d["audit_log_b64"] and "AUDIT MODE" in alog,
      "an audit run still produces an audit log")
check("plan: /dev/nvme0" in alog and "nvme-sanitize-crypto" in alog,
      "and the log carries the method chosen for each device")

# --- Fetching the vendor CLI at run time ----------------------------------
# The commissioning environment is a RAM image, so a storcli installed on the
# deployed OS is not there. It is fetched instead -- and verified against a
# pinned hash first, because it is a binary that erases disks.
FAKE_ZIP = os.path.join(ROOT, "tests/fixtures/storcli-fake.zip")
FAKE_SHA = "241d4d213a326f6dd008f1fc7ef5f9773992c2cf92f20ce49c665073d2c4711e"
OPT = os.path.join(STATE, "fake-opt", "storcli64")

def run_fetch(**extra):
    e = {"INSTALL_STORCLI": "true", "STUB_CURL_ZIP": FAKE_ZIP,
         "STORCLI_INSTALL_PATH": OPT, "STUB_SG_PASSTHROUGH": 0}
    e.update(extra)
    return run(**e)

_, d = run_fetch(STORCLI_SHA256=FAKE_SHA)
prov = d["controller_survey"]["raid_cli_provenance"]
check(prov["installed"] is True and prov["verified"] is True
      and d["controller_survey"]["commands_run"],
      "a package matching the pinned hash is installed, and the survey then runs")
check(d["controller_survey"]["raid_cli"] == OPT,
      "and the freshly installed binary is found where the package put it")

_, d = run_fetch(STORCLI_SHA256="0" * 64)
prov = d["controller_survey"]["raid_cli_provenance"]
check(prov["installed"] is False and prov["verified"] is False
      and any("pinned hash" in i["issue"] for i in d["verdict"]["issues"]),
      "a package that does not match the pinned hash is refused, not installed")
check(d["report_metadata"]["mode"] == "audit" or erased(),
      "and that refusal is not itself a reason to abandon the reachable drives")

p, d = run_fetch(STUB_CURL="fail", STORCLI_SHA256=FAKE_SHA)
check(d["controller_survey"]["raid_cli_provenance"]["attempted"] is True
      and d["controller_survey"]["raid_cli_provenance"]["installed"] is False
      and any("could not be fetched" in i["issue"] for i in d["verdict"]["issues"]),
      "a blocked download warns and degrades rather than crashing")

_, d = run(INSTALL_STORCLI="true", STUB_VIRTUAL_MEDIA=0, STUB_SG_PASSTHROUGH=1,
           STUB_NVME_CTRLS=1)
check(d["controller_survey"]["raid_cli_provenance"]["attempted"] in (True, False),
      "the fetch decision is always recorded, either way")

# --- Finding a vendor CLI that is not on PATH -----------------------------
# The Broadcom package installs to /opt/MegaRAID/storcli/storcli64 and puts
# nothing on PATH, so a probe built on `command -v` alone reports "no CLI
# staged" on a machine where it is installed -- and silently takes the weaker
# route as a result.
CLI = os.path.join(RAID, "storcli64")
_, d = run(STUB_SG_PASSTHROUGH=0)
check(d["environment"]["raid_cli"] is None
      and all(m["method"] == "none" for m in media(d, device_type="raid-member")),
      "with no CLI anywhere, the members have no route from here")
_, d = run(STUB_SG_PASSTHROUGH=0, RAID_CLI=CLI)
check(d["environment"]["raid_cli"] == CLI,
      "a CLI off PATH is found via RAID_CLI, as an /opt install would be")
# A staged CLI makes a Purge reachable but does not make it issuable here: the
# drive is still a virtual-disk member, and a controller sanitise needs it
# Unconfigured Good. Deleting the VD is a controller reconfiguration, so the
# script names the sequence rather than doing it.
mem = media(d, device_type="raid-member")
check(all(m["method"] == "none" and "ALLOW_RAID_RECONFIG is not set" in m["method_reason"]
          for m in mem),
      "and it does not silently reconfigure the controller to get at the drive")
rt = {x["serial_number"]: x["routes"] for x in d["remediation"]}
check(all("start sanitize" in r[0]["command"] and "ause" in r[0]["command"] for r in rt.values()),
      "the route issues a firmware sanitize, with ause so a failure leaves the drive usable")
check(all("secureerase force" in r[1]["command"] and "SED" in r[1]["prerequisite"] for r in rt.values()),
      "and secureerase is offered only as the SED-specific route")
cmds = [x["command"] for r in rt.values() for x in r if x.get("command")]
check(cmds and not any("start erase" in c for c in cmds),
      "no route commands start erase -- it is an overwrite, so a Clear on flash")
p, d = run(STUB_SG_PASSTHROUGH=0, RAID_CLI="/nope/storcli64")
check(d["environment"]["raid_cli"] is None and "not executable" in p.stderr,
      "a RAID_CLI that does not exist warns and degrades, rather than crashing")

# --- Purging a drive inside a virtual disk --------------------------------
# A controller-issued sanitise needs the drive Unconfigured Good, so the volume
# has to go first -- and the buyer expects the array back, so it is rebuilt from
# the configuration captured beforehand. That is a controller reconfiguration,
# not a media erase, so it has its own consent separate from SANITIZE_CONFIRM.
def run_raid(**extra):
    e = {"INSTALL_STORCLI": "true", "STUB_CURL_ZIP": FAKE_ZIP,
         "STORCLI_SHA256": FAKE_SHA, "STORCLI_INSTALL_PATH": OPT,
         "STUB_SG_PASSTHROUGH": 0, "SANITIZE_POLL_INTERVAL": 1,
         "SANITIZE_CONFIRM": GO, "EXPECT_SYSTEM_SERIAL": "R9S0MD0000AB"}
    e.update(extra)
    return run(**e)

def members(d):
    return media(d, device_type="raid-member")

_, d = run_raid()
check(all(m["method"] == "none" for m in members(d))
      and d["raid_reconfiguration"]["performed"] is False
      and not any("del force" in c for c in erased()),
      "without ALLOW_RAID_RECONFIG the volume is never touched")
check(all("ALLOW_RAID_RECONFIG is not set" in m["method_reason"] for m in members(d)),
      "and the reason names the gate that would permit it")

_, d = run_raid(ALLOW_RAID_RECONFIG="true")
calls = erased()
check(d["verdict"]["overall"] == "PASS" and not d["remediation"],
      "with both gates set, every drive reaches a verified Purge")
check(all(m["result"] == "success" and m["nist_level"] == "purge" for m in members(d)),
      "including the two the controller was hiding")
check(all((m.get("verification") or {}).get("serial_matches") is True for m in members(d)),
      "each re-identified by serial through the controller, the volume now gone")
i_del = [n for n, c in enumerate(calls) if "del force" in c]
i_san = [n for n, c in enumerate(calls) if "start sanitize" in c]
i_add = [n for n, c in enumerate(calls) if "add vd" in c]
check(i_del and i_san and i_add and max(i_del) < min(i_san) < max(i_san) < min(i_add),
      "in the only workable order: delete, purge both, then rebuild")
check(d["raid_reconfiguration"]["restored"] is True
      and "type=r1" in d["raid_reconfiguration"]["restored_as"],
      "and the array is put back as the RAID 1 it was")
check([x["step"] for x in d["raid_reconfiguration"]["steps"]][0] == "delete virtual disk"
      and any(x["step"] == "recreate virtual disk" for x in d["raid_reconfiguration"]["steps"]),
      "every controller step is recorded with its exit status and output")

# The guard that matters most: never put an array back over a drive that was not
# actually erased, because that hides the very thing this run establishes.
_, d = run_raid(ALLOW_RAID_RECONFIG="true", STUB_RAIDCLI_ERASE="fail")
check(all(m["result"] == "failed" for m in members(d))
      and d["raid_reconfiguration"]["restored"] is False
      and not any("add vd" in c for c in erased()),
      "a member that failed to purge gets no array rebuilt over it")
check(any("was not rebuilt" in i["issue"] for i in d["verdict"]["issues"]),
      "and the report says the array was left unconfigured, and why")

# Completion is adjudicated on the strongest evidence available.
_, d = run_raid(ALLOW_RAID_RECONFIG="true", STUB_RAIDCLI_TICKS=2)
check(all(m["status_code"] == "100%" for m in members(d)),
      "a reported percentage decides completion, and 100% is what it waits for")
_, d = run_raid(ALLOW_RAID_RECONFIG="true", STUB_RAIDCLI_NOPCT=1, STUB_RAIDCLI_TICKS=2)
check(all(m["result"] == "success" and m["status_code"].startswith("state:")
          for m in members(d)),
      "a controller reporting no percentage falls back to the drive leaving Sntze")

_, d = run_raid(ALLOW_RAID_RECONFIG="true", RAID_RESTORE_ARRAY="false")
check(d["raid_reconfiguration"]["restored"] is False
      and "RAID_RESTORE_ARRAY" in d["raid_reconfiguration"]["restore_skipped"]
      and d["verdict"]["overall"] == "PASS",
      "leaving the drives unconfigured is a choice, and does not fail the run")

# --- Purging through a JBOD window ----------------------------------------
# Established on the hardware: the controller can issue neither a sanitise nor
# an instant secure erase, but in JBOD it presents each drive as its own SCSI
# target and ATA SANITIZE survives the translation. So the erase is addressed to
# the drive, inside a window the script opens and closes.
def run_jbod(**extra):
    e = {"RAID_CLI": CLI, "STUB_SG_PASSTHROUGH": 0, "STUB_RAIDCLI_CTLSAN": "no",
         "STUB_ATA_CAPS": "crypto", "ALLOW_RAID_RECONFIG": "true",
         "SANITIZE_CONFIRM": GO, "EXPECT_SYSTEM_SERIAL": "R9S0MD0000AB",
         "SANITIZE_POLL_INTERVAL": 1}
    e.update(extra)
    return run(**e)

_, d = run_jbod()
calls = erased()
mem = media(d, device_type="raid-member")
check(d["verdict"]["overall"] == "PASS" and not d["remediation"],
      "all three drives purged in one run, nothing outstanding")
check(all(m["method"] == "ata-sanitize-via-jbod" and m["result"] == "success"
          and m["nist_level"] == "purge" for m in mem),
      "the members are purged by their own ATA SANITIZE, through the window")
check(all((m.get("verification") or {}).get("serial_matches") is True for m in mem),
      "and each re-identified by serial on the node JBOD exposed it as")
# The node is resolved at execution time: names are recycled the moment the
# volume is deleted, and the register still carries the volume's path.
check(all(m["device"] == "/dev/sda" for m in mem)
      and any("/dev/sdj" in c for c in calls),
      "the device is resolved by serial at execution time, not by the recorded name")
i_del = [n for n, c in enumerate(calls) if "del force" in c]
i_hd  = [n for n, c in enumerate(calls) if "hdparm" in c]
i_add = [n for n, c in enumerate(calls) if "add vd" in c]
check(i_del and len(i_hd) == 2 and i_add
      and max(i_del) < min(i_hd) and max(i_hd) < min(i_add),
      "in order: delete the volume, purge both drives, rebuild the array")
steps = [x["step"] for x in d["raid_reconfiguration"]["steps"]]
check(d["raid_reconfiguration"]["jbod_window"] is True
      and any("set drive JBOD" == x for x in steps)
      and steps.index("recreate virtual disk") > max(
          i for i, x in enumerate(steps) if "Unconfigured Good" in x),
      "the window is closed and the drives returned before the array is rebuilt")
check(d["raid_reconfiguration"]["restored"] is True,
      "and the array comes back as the RAID 1 it was")

# A controller that accepts the JBOD command but never presents the drive: the
# erase cannot be addressed, and nothing is rebuilt over an unpurged drive.
_, d = run_jbod(STUB_RAIDCLI_JBOD_HIDES=1)
check(all(m["result"] == "failed" and "not exposed" in (m["status_code"] or "")
          for m in media(d, device_type="raid-member")),
      "a drive that never appears in the window is a failure, not a silent pass")
check(not any("hdparm" in c for c in erased())
      and d["raid_reconfiguration"]["restored"] is False,
      "no erase is issued blind, and no array is rebuilt over it")

# The drive is exposed but rejects the command.
_, d = run_jbod(STUB_ATA_REJECT=1)
check(all(m["result"] == "failed" for m in media(d, device_type="raid-member"))
      and d["verdict"]["overall"] == "FAIL",
      "a drive that rejects the sanitise fails the run")

# --- The JBOD probe: reconfigure and look, erase nothing -------------------
# Neither controller route exists on an MR9560-16i (Support Sanitize = No,
# Allow Ctrl Encryption = No), while the drives themselves report CryptoErase
# and BlockErase. Whether ATA SANITIZE survives the translation the controller
# applies in JBOD is firmware-dependent, so it is established by looking rather
# than assumed either way -- and looking must not erase anything.
def run_probe(**extra):
    e = {"RAID_CLI": CLI, "STUB_SG_PASSTHROUGH": 0, "STUB_RAIDCLI_CTLSAN": "no",
         "ALLOW_RAID_RECONFIG": "true", "SANITIZE_POLL_INTERVAL": 1}
    e.update(extra)
    return run(**e)

# A run pinned to one chassis must not touch a different one -- not even to
# look. The probe reconfigures the controller, and refusing the erase is no
# comfort if the array was deleted and rebuilt on a machine the operator
# deliberately pinned away from.
_, d = run_probe(EXPECT_SYSTEM_SERIAL="SOME-OTHER-CHASSIS")
check(d["jbod_probe"]["attempted"] is False
      and d["raid_reconfiguration"]["performed"] is False
      and erased() == [],
      "a chassis the run is pinned away from is not probed, let alone erased")
check(any("pinned to" in i["issue"] for i in d["verdict"]["issues"]),
      "and the refusal names the pin")

_, d = run_probe(STUB_ATA_CAPS="crypto")
pr = d["jbod_probe"]
check(d["report_metadata"]["mode"] == "audit" and pr["attempted"] is True,
      "the probe runs in audit mode, so looking is never confused with wiping")
check(not any(("sanitize" in c or "secureerase" in c) and "nvme" not in c for c in erased()),
      "and it issues no erase to any drive it exposed")
check(len(pr["drives"]) == 2
      and all(x["exposed_as"] and x["purge_reachable"] for x in pr["drives"]),
      "each hidden drive is matched by serial to the device JBOD exposed it as")
check(all(x["would_use"] == "hdparm --sanitize-crypto-scramble" for x in pr["drives"]),
      "and the command that would purge it is named")
check("does survive the translation" in pr["conclusion"],
      "the conclusion answers the question the probe exists for")
check(d["raid_reconfiguration"]["restored"] is True,
      "the array it borrowed is put back, nothing having been erased")
steps = [x["step"] for x in d["raid_reconfiguration"]["steps"]]
check(steps[0].startswith("delete virtual disk")
      and "enable controller JBOD" in steps
      and "disable controller JBOD" in steps
      and steps[-1] == "recreate virtual disk",
      "every controller command is recorded, ending with the array restored")

# Two ways the route can be closed, and both must be reported as closed rather
# than worked around.
_, d = run_probe(STUB_RAIDCLI_JBOD_HIDES=1)
check(all(x["exposed_as"] is None for x in d["jbod_probe"]["drives"])
      and "route is closed" in d["jbod_probe"]["conclusion"]
      and any("no purge-capable command" in i["issue"] for i in d["verdict"]["issues"]),
      "a controller that accepts JBOD but never presents the drives is a closed route")
_, d = run_probe(STUB_ATA_CAPS="none", STUB_SCSI_SANITIZE=0)
check(not any(x["purge_reachable"] for x in d["jbod_probe"]["drives"])
      and "does not survive" in d["jbod_probe"]["conclusion"],
      "exposed but with no purge-capable command is also a closed route")

# And the gate holds: no consent, no reconfiguration.
_, d = run(RAID_CLI=CLI, STUB_SG_PASSTHROUGH=0, STUB_RAIDCLI_CTLSAN="no")
check(d["jbod_probe"]["attempted"] is False
      and d["raid_reconfiguration"]["performed"] is False
      and not any("del force" in c for c in erased()),
      "without ALLOW_RAID_RECONFIG the probe does not touch the controller")

# --- Filesystem signatures, because "OS only" is a claim worth checking ---
# The OS volume on these hosts is asserted to hold no user data. Swap is what
# most often contradicts that: a hypervisor that swapped wrote pages from any
# process on the host, guests included. Recorded either way -- every device here
# has to reach Purge regardless, so this is context for the reviewer, not a gate.
_, d = run(STUB_SG_PASSTHROUGH=1)
vol = media(d, device_type="raid-volume")[0]
check(sorted(vol["capabilities"]["filesystem_signatures"]) == ["ext4", "vfat"],
      "filesystem signatures on a volume are recorded without mounting it")
_, d = run(STUB_SG_PASSTHROUGH=1, STUB_SWAP=1)
check(any(i["severity"] == "info" and "swap signature" in i["issue"]
          for i in d["verdict"]["issues"]),
      "a swap signature is surfaced, since it contradicts a no-user-data claim")
check(d["report_metadata"]["required_level"] == "purge",
      "and it does not change the required level, which is Purge for every device")

# --- Nulls, never blanks. A blank serial is a claim we cannot make --------
_, d = run(STUB_SG_PASSTHROUGH=1)
blanks = [(m["device"], k) for m in d["media"] for k, v in m.items() if v == ""]
check(not blanks, "absent values are null, never the empty string")

sys.exit(rc)
PYEOF

hdr "Stress-test verdict matrix"
./tests/run-stress-verdicts.sh || rc=1

hdr "Burn-in verdict matrix"
./tests/run-burnin-verdicts.sh || rc=1

hdr "Acceptance adjudication matrix"
python3 tests/run-acceptance.py || rc=1

hdr "End-to-end report rendering"
F=tests/fixtures/reports
check_report() { # label, expected headline, extra args...
    local label="$1" want="$2"; shift 2
    local out=/tmp/rep-test.html got
    python3 reporting/device_certificate.py "$@" -o "$out" --quiet 2>/dev/null
    got=$(grep -oE 'class="badge badge-[a-z]+">[A-Z/]+' "$out" | head -1 | grep -oE '[A-Z/]+$')
    if [[ "$got" == "$want" ]]; then printf '  ok    %-36s -> %s\n' "$label" "$got"
    else printf '  FAIL  %-36s -> got %s, want %s\n' "$label" "${got:-none}" "$want"; rc=1; fi
}
check_report "healthy, all stages"    PASS --install $F/install-pass.json --config $F/config-ecc-already-on.json --inventory $F/inventory-healthy.json --stress $F/stress-pass.json --burnin $F/burnin-full.json
# Without script 91's result the card's prior ECC state is unknown, so a zero
# counter cannot be trusted -- that must not read as a clean pass.
check_report "no 91 result: prior ECC unknown" WARN --install $F/install-pass.json --inventory $F/inventory-healthy.json --stress $F/stress-pass.json --burnin $F/burnin-full.json
check_report "ECC enabled this run"  FAIL --install $F/install-pass.json --config $F/config-ecc-enabled.json --inventory $F/inventory-skipped.json --stress $F/stress-skipped.json
check_report "two rejectable cards"   FAIL --install $F/install-pass.json --inventory $F/inventory-mixed.json   --stress $F/stress-pass.json --burnin $F/burnin-full.json
check_report "stress returned CONFIG" FAIL --install $F/install-pass.json --inventory $F/inventory-healthy.json --stress $F/stress-config.json --burnin $F/burnin-full.json
check_report "evidence missing"       WARN --install $F/install-pass.json
check_report "no burn-in evidence"    WARN --install $F/install-pass.json --inventory $F/inventory-healthy.json --stress $F/stress-pass.json

hdr "Post-test counter check across the whole sequence"
BL=$(mktemp); MK=/tmp/nexgen-baseline-written
rm -f "$BL" "$MK" /tmp/stub-*
NEXGEN_BASELINE_FILE="$BL" bash commissioning-scripts/92-nexgen-gpu-inventory.sh >/dev/null 2>&1
n=$(jq 'length' "$BL" 2>/dev/null || echo 0)
if [[ "$n" == "$STUB_GPU_COUNT" ]]; then echo "  ok    92 writes a baseline for $n GPU(s)"
else echo "  FAIL  92 baseline has $n entries, expected $STUB_GPU_COUNT"; rc=1; fi
touch "$MK"
for spec in "clean:PASS:" "fault after baseline:FAIL:STUB_ECC_UCE_AFTER=6"; do
    lbl="${spec%%:*}"; rest="${spec#*:}"; want="${rest%%:*}"; envs="${rest#*:}"
    rm -f /tmp/stub-*
    got=$(env NEXGEN_BASELINE_FILE="$BL" $envs DCGM_FIXTURE=tests/fixtures/dcgm/all-pass.json \
          bash commissioning-scripts/98-nexgen-gpu-stress-test.sh 2>/dev/null | jq -r '.verdict.overall')
    if [[ "$got" == "$want" ]]; then printf '  ok    98 %-24s -> %s\n' "$lbl" "$got"
    else printf '  FAIL  98 %-24s -> got %s, want %s\n' "$lbl" "$got" "$want"; rc=1; fi
done
rm -f /tmp/stub-*
got=$(env NEXGEN_BASELINE_FILE=/tmp/definitely-absent.json DCGM_FIXTURE=tests/fixtures/dcgm/all-pass.json \
      bash commissioning-scripts/98-nexgen-gpu-stress-test.sh 2>/dev/null \
      | jq -r '.counters_since_baseline.baseline_available')
if [[ "$got" == "false" ]]; then echo "  ok    a missing baseline is reported, not assumed clean"
else echo "  FAIL  missing baseline reported as $got"; rc=1; fi
rm -f "$BL" "$MK"

hdr "ECC revalidation gate (run 1 halts, run 2 proceeds)"
export NEXGEN_HALT_FILE=/tmp/nexgen-halt-test.$$
rm -f "$NEXGEN_HALT_FILE" /tmp/stub-*
o=$(STUB_ECC_MODE=Disabled bash commissioning-scripts/91-nexgen-gpu-mig-ecc-config.sh 2>/dev/null)
v=$(printf '%s' "$o" | jq -r '.verdict.overall'); r=$(printf '%s' "$o" | jq -r '.revalidation_required')
if [[ "$v" == "FAIL" && "$r" == "true" && -f "$NEXGEN_HALT_FILE" ]]; then
    echo "  ok    run 1: ECC disabled -> 91 FAIL, halt marker written"
else
    echo "  FAIL  run 1: verdict=$v revalidation=$r marker=$([[ -f $NEXGEN_HALT_FILE ]] && echo yes || echo no)"; rc=1
fi
for s2 in 92-nexgen-gpu-inventory 98-nexgen-gpu-stress-test 99-nexgen-gpu-burn-in; do
    rm -f /tmp/stub-*
    t0=$(date +%s%N)
    o=$(DCGM_FIXTURE=tests/fixtures/dcgm/all-pass.json BURN_DURATION=1800 \
        bash "commissioning-scripts/$s2.sh" 2>/dev/null)
    ms=$(( ($(date +%s%N) - t0) / 1000000 ))
    if [[ "$(printf '%s' "$o" | jq -r '.skipped // false')" == "true" && $ms -lt 5000 ]]; then
        printf '  ok    run 1: %s skipped in %sms\n' "$s2" "$ms"
    else
        printf '  FAIL  run 1: %s did not skip (%sms)\n' "$s2" "$ms"; rc=1
    fi
done
rm -f "$NEXGEN_HALT_FILE" /tmp/stub-*
o=$(bash commissioning-scripts/91-nexgen-gpu-mig-ecc-config.sh 2>/dev/null)
if [[ "$(printf '%s' "$o" | jq -r '.verdict.overall')" == "PASS" && ! -f "$NEXGEN_HALT_FILE" ]]; then
    echo "  ok    run 2: ECC already on -> 91 PASS, no halt marker"
else
    echo "  FAIL  run 2: 91 did not pass cleanly with ECC already enabled"; rc=1
fi
rm -f /tmp/stub-*
if [[ "$(bash commissioning-scripts/92-nexgen-gpu-inventory.sh 2>/dev/null | jq -r '.skipped // false')" == "false" ]]; then
    echo "  ok    run 2: later scripts run normally"
else
    echo "  FAIL  run 2: later scripts still skipping"; rc=1
fi
unset NEXGEN_HALT_FILE

hdr "Action-required banner"
F=tests/fixtures/reports
python3 reporting/device_certificate.py --install $F/install-pass.json \
    --config $F/config-ecc-enabled.json --inventory $F/inventory-skipped.json \
    --stress $F/stress-skipped.json -o /tmp/rep-run1.html --quiet 2>/dev/null
if grep -q '<div class="action-required">' /tmp/rep-run1.html; then
    echo "  ok    shown when ECC was enabled this run"
else echo "  FAIL  banner missing on a revalidation run"; rc=1; fi
python3 reporting/device_certificate.py --install $F/install-pass.json \
    --config $F/config-ecc-already-on.json --inventory $F/inventory-healthy.json \
    --stress $F/stress-pass.json --burnin $F/burnin-full.json -o /tmp/rep-test.html --quiet 2>/dev/null
if grep -q '<div class="action-required">' /tmp/rep-test.html; then
    echo "  FAIL  banner shown on a clean run"; rc=1
else echo "  ok    absent on a clean run"; fi

hdr "CPU cores are physical cores, not logical CPUs"
# MAAS NUMA "cores" lists hold logical cpu ids. Summing them described a real
# 2x64C/256T machine on the certificate as "256 cores / 256 threads".
jq '.system += {cpu_model:"AMD EPYC 9554 64-Core Processor", cpu_sockets:2,
                cpu_cores_per_socket:64, cpu_total_cores:128, cpu_total_threads:256}' \
   $F/inventory-healthy.json > /tmp/inv-cores.json 2>/dev/null
python3 reporting/device_certificate.py --inventory /tmp/inv-cores.json \
    --stress $F/stress-pass.json -o /tmp/rep-cores.html --quiet 2>/dev/null
if grep -q '128 cores / 256 threads' /tmp/rep-cores.html; then
    echo "  ok    reports 128 cores / 256 threads for 2x64C"
else
    echo "  FAIL  wrong CPU topology: $(grep -o '[0-9]* cores / [0-9]* threads' /tmp/rep-cores.html | head -1)"; rc=1
fi
# With no physical figure available it must not invent one.
python3 reporting/device_certificate.py --inventory $F/inventory-healthy.json \
    --stress $F/stress-pass.json -o /tmp/rep-nocores.html --quiet 2>/dev/null
if grep -q 'logical CPUs' /tmp/rep-nocores.html; then
    echo "  ok    says 'logical CPUs' when the physical count is unknown"
else
    echo "  FAIL  claimed a core count with no physical figure available"; rc=1
fi
rm -f /tmp/inv-cores.json /tmp/rep-cores.html /tmp/rep-nocores.html
# MAAS lists logical cpu ids per NUMA node, so this host shows "128" per node
# -- the same number as its physical core TOTAL. Both must be readable without
# having to reconcile them.
python3 - <<'PYEOF' || rc=1
import sys, json, re
sys.path.insert(0, "reporting")
import device_certificate as dc

inv = json.load(open("tests/fixtures/reports/inventory-healthy.json"))
inv["system"].update(dict(cpu_model="AMD EPYC 9554 64-Core Processor", cpu_sockets=2,
                          cpu_cores_per_socket=64, cpu_total_cores=128,
                          cpu_total_threads=256))
stress = json.load(open("tests/fixtures/reports/stress-pass.json"))
# Exactly the layout MAAS reports for a 2-socket, 8-GPU H100 PCIe node.
numa = [{"index": 0, "memory_mb": 788480,
         "cores": list(range(0, 64)) + list(range(128, 192))},
        {"index": 1, "memory_mb": 788480,
         "cores": list(range(64, 128)) + list(range(192, 256))}]
html = dc.generate_report(None, inv, stress, numa_nodes_maas=numa,
                          machine={"cpu_count": 256})
labels = re.findall(r"(\d+) cores / (\d+) threads", html)
want = [("128", "256"), ("64", "128"), ("64", "128")]
if labels == want:
    print("  ok    128c/256t total, 64c/128t per node (MAAS shows 256 and 128)")
    sys.exit(0)
print(f"  FAIL  CPU labels {labels}, want {want}")
sys.exit(1)
PYEOF

hdr "CPU core count survives a machine commissioned before script 92 changed"
python3 - <<'PYEOF' || rc=1
import sys, json, re
sys.path.insert(0, "reporting")
import device_certificate as dc

PAT = re.compile(r"(\d+) cores / (\d+) threads")

def resources(nested=True):
    socks = [{"socket": i, "name": "AMD EPYC 9554 64-Core Processor",
              "cores": [{"core": c, "threads": [{"id": c}, {"id": c + 128}]}
                        for c in range(64)]} for i in range(2)]
    cpu = {"architecture": "x86_64", "sockets": socks, "total": 256}
    return {"resources": {"cpu": cpu}} if nested else {"cpu": cpu}

rc = 0
# Parser handles both shapes MAAS emits, and refuses to guess when absent.
for nested in (True, False):
    got = dc.parse_machine_resources_cpu(resources(nested))
    if got != {"sockets": 2, "cores": 128, "threads": 256}:
        print(f"  FAIL  machine-resources cpu parse (nested={nested}): {got}"); rc = 1
for bad in (None, {}, {"cpu": {}}, {"cpu": {"sockets": "nope"}}):
    if dc.parse_machine_resources_cpu(bad) != {}:
        print(f"  FAIL  should not claim a topology from {bad!r}"); rc = 1
if not rc:
    print("  ok    machine-resources cpu.sockets[].cores[] parsed, junk declined")

# The vendor part name is a sound last resort, but only when it states cores.
cases = [("AMD EPYC 9554 64-Core Processor", 2, 128),
         ("AMD EPYC 9754 128-Core Processor", 2, 256),
         ("AMD EPYC 9554 64-Core Processor", 1, 64),
         ("Intel(R) Xeon(R) Gold 6338 CPU @ 2.00GHz", 2, 0),
         ("", 2, 0)]
bad = [(m, n, dc.cores_from_cpu_model(m, n)) for m, n, w in cases
       if dc.cores_from_cpu_model(m, n) != w]
if bad:
    print(f"  FAIL  part-name core inference: {bad}"); rc = 1
else:
    print("  ok    part-name inference, and no claim when the name is silent")

# End to end: inventory WITHOUT cpu_total_cores must still report 128, not 256.
inv = json.load(open("tests/fixtures/reports/inventory-healthy.json"))
inv["system"].update(dict(cpu_model="AMD EPYC 9554 64-Core Processor",
                          cpu_sockets=2, cpu_total_threads=256))
inv["system"].pop("cpu_total_cores", None)
stress = json.load(open("tests/fixtures/reports/stress-pass.json"))
numa = [{"index": 0, "memory_mb": 788480,
         "cores": list(range(0, 64)) + list(range(128, 192))},
        {"index": 1, "memory_mb": 788480,
         "cores": list(range(64, 128)) + list(range(192, 256))}]
for label, topo in [("via machine-resources", dc.parse_machine_resources_cpu(resources())),
                    ("via part name only", None)]:
    html = dc.generate_report(None, inv, stress, numa_nodes_maas=numa,
                              machine={"cpu_count": 256}, cpu_topology=topo)
    if PAT.findall(html)[:3] == [("128", "256"), ("64", "128"), ("64", "128")]:
        print(f"  ok    128c/256t {label}, no cpu_total_cores needed")
    else:
        print(f"  FAIL  {label}: {PAT.findall(html)[:3]}"); rc = 1
sys.exit(rc)
PYEOF

hdr "Stress stage is named after the tool it runs"
python3 - <<'PYEOF' || rc=1
import sys
sys.path.insert(0, "reporting")
import device_certificate as dc
if "DCGM Diagnostics" in dc.REQUIRED_STAGES and "Stress Test" not in dc.REQUIRED_STAGES:
    print("  ok    REQUIRED_STAGES names the DCGM stage")
    sys.exit(0)
print(f"  FAIL  REQUIRED_STAGES = {dc.REQUIRED_STAGES}")
sys.exit(1)
PYEOF

hdr "Burn-in mode commentary is kept out of the findings table"
python3 - <<'PYEOF' || rc=1
import sys, json, re, copy
sys.path.insert(0, "reporting")
import device_certificate as dc

inv = json.load(open("tests/fixtures/reports/inventory-healthy.json"))
stress = json.load(open("tests/fixtures/reports/stress-pass.json"))
burn0 = json.load(open("tests/fixtures/reports/burnin-full.json"))
for t in burn0["burn_in"]["telemetry"]:
    t.update(loaded_samples=356,
             throttle_samples={"sw_power_cap": 356, "hw_thermal_slowdown": 0,
                               "hw_power_brake_slowdown": 0, "sw_thermal_slowdown": 0})

def burn_rows(issues, mode="characterize", hwth=0):
    b = copy.deepcopy(burn0)
    b["burn_in"]["mode"] = mode
    for t in b["burn_in"]["telemetry"]:
        t["throttle_samples"]["hw_thermal_slowdown"] = hwth
    b["verdict"] = {"overall": "FAIL" if hwth else "PASS", "issues": issues}
    h = dc.generate_report(None, inv, stress, burnin=b)
    return [r for r in re.findall(
        r'sev-(\w+)">\w+</span></td><td class="dim">([^<]*)</td>', h)
        if r[1] == "Burn-In"], b

rc = 0
# The note stored from the run they already have.
rows, _ = burn_rows([{"issue": "BURN_MODE=characterize -- telemetry recorded, "
                               "no thresholds enforced", "severity": "info"}])
if not rows:
    print("  ok    the stored BURN_MODE note is not shown as a finding")
else:
    print("  FAIL  BURN_MODE note still in the findings table: %s" % rows); rc = 1

# The replacement notes the newer script writes.
rows, _ = burn_rows([
    {"issue": "No thermal or power-brake slowdown on any of 8 GPU(s) across "
              "2849 loaded sample(s)", "severity": "info"},
    {"issue": "BURN_MODE=characterize: the per-GPU load verdict...", "severity": "info"}])
if not rows:
    print("  ok    the newer mode and throttle notes are not shown either")
else:
    print("  FAIL  newer notes still shown: %s" % rows); rc = 1

# A genuine throttle finding must never be filtered.
rows, b = burn_rows([{"issue": "8 GPU(s) hit thermal or power-brake slowdown "
                               "under sustained load", "severity": "critical"}],
                    mode="enforce", hwth=41)
if rows and rows[0][0] == "crit":
    print("  ok    a real throttle finding survives as critical")
else:
    print("  FAIL  real throttle finding was filtered: %s" % rows); rc = 1

# Criterion #6 carries the answer either way, independent of BURN_MODE.
# Thermal throttling is WARN, not FAIL: a card throttling because its airflow
# is restricted or a fan is slow is serviceable, and it is not a Schedule 4
# para 4 criterion either. Grading it REJECT overstated what the contract
# measures and pre-empted a repair decision. It is still surfaced, and still
# disclosed under limb 1(g) in the Server Report.
c6 = [c for c in dc.evaluate_gpu_acceptance(inv["gpus"][0], stress, b)["criteria"]
      if c["reject"] == 6][0]
if c6["status"] == "WARN" and "hw_thermal_slowdown in 41" in c6["detail"]:
    print("  ok    criterion #6 warns on thermal throttling, does not reject")
else:
    print("  FAIL  #6 thermal grading: %s %s" % (c6["status"], c6["detail"])); rc = 1
# Recorded as a measurement, not a diagnosis. The cause of throttling is not
# evidenced -- only that clocks dropped under load -- and the column it feeds is
# warranted under Clause 10.1(i), so the detail must not assert a cause. What it
# must carry is the functional result, which is that nothing was found wrong.
if ("no fault was found" in c6["detail"]
        and "gpu-burn returned OK" in c6["detail"]
        and "Sch 4 para 4" in c6["detail"]):
    print("  ok    and states no fault was found, without asserting a cause")
else:
    print("  FAIL  #6 detail wording: %s" % c6["detail"]); rc = 1
for word in ("serviceable", "airflow", "seating", "recommend"):
    if word in c6["detail"].lower():
        print("  FAIL  #6 detail speculates on cause (%r)" % word); rc = 1
        break
else:
    print("  ok    and does not speculate on the cause")

# A power brake is asserted by power delivery, not temperature, so no amount of
# airflow work addresses it. It is a different fault and stays FAIL.
bb = copy.deepcopy(burn0)
for t in bb["burn_in"]["telemetry"]:
    t["throttle_samples"]["hw_power_brake_slowdown"] = 7
bb["verdict"] = {"overall": "FAIL", "issues": []}
c6b = [c for c in dc.evaluate_gpu_acceptance(inv["gpus"][0], stress, bb)["criteria"]
       if c["reject"] == 6][0]
if c6b["status"] == "FAIL" and "hw_power_brake_slowdown in 7" in c6b["detail"]:
    print("  ok    a power brake is still FAIL -- not a thermal problem")
else:
    print("  FAIL  power brake grading: %s %s" % (c6b["status"], c6b["detail"])); rc = 1

# The whole point of the regrade: those GPUs must land in review, not rejected.
ev = dc.evaluate_gpu_acceptance(inv["gpus"][0], stress, b)
if ev["verdict"] == "REVIEW":
    print("  ok    a thermally throttling GPU is REVIEW, not REJECT")
else:
    print("  FAIL  thermal GPU verdict is %s" % ev["verdict"]); rc = 1

b2 = copy.deepcopy(burn0); b2["verdict"] = {"overall": "PASS", "issues": []}
c6 = [c for c in dc.evaluate_gpu_acceptance(inv["gpus"][0], stress, b2)["criteria"]
      if c["reject"] == 6][0]
if c6["status"] == "PASS" and "no hardware thermal or power-brake slowdown" in c6["detail"]:
    print("  ok    criterion #6 states the negative outright when clean")
else:
    print("  FAIL  #6 clean wording: %s" % c6["detail"]); rc = 1

sys.exit(rc)
PYEOF

hdr "nvbandwidth skip is adjudicated by SKU, not by NVLink presence"
python3 - <<'PYEOF' || rc=1
import sys, json, re, copy
sys.path.insert(0, "reporting")
import device_certificate as dc

# Established from DCGM 4.6.1's source and the SKU table compiled into its nvvs
# binary: the plugin's only Skip path is `is_allowed` false, that parameter
# defaults false, and PCI device 0x2331 (H100 PCIe) has no nvbandwidth key in
# the table. So it skips on EVERY H100 PCIe regardless of bridges.
#
# The old rule keyed off NVLink presence. It gave the right answer on a
# bridgeless host for the wrong reason and the WRONG answer on a bridged one:
# EXAMPLE-NODE-03 has four working NV12 bridges and still skipped, which put all
# eight of its GPUs into review over a skip that was by design. Keyed on SKU,
# with an allow-list -- an unknown SKU keeps the warning rather than waiving
# evidence on a guess.
inv = json.load(open("tests/fixtures/reports/inventory-healthy.json"))
base = json.load(open("tests/fixtures/reports/stress-pass.json"))

def stress_with(skipped):
    st = copy.deepcopy(base)
    for name in skipped:
        st["dcgm_diagnostics"]["test_results"].append(
            {"test": name,
             "results": [{"gpu_id": None, "status": "Skip", "info": "", "warnings": []}
                         for _ in range(8)]})
    st["verdict"] = {"overall": "WARN", "issues": [{
        "issue": "%d test(s) skipped -- they did not run, so they evidence nothing"
                 % (8 * len(skipped)),
        "severity": "warning", "details": ", ".join(skipped)}]}
    return st

def run(dev_id, nvlink, skipped):
    i = copy.deepcopy(inv)
    i["system"]["nvlink_present"] = nvlink
    for g in i["gpus"]:
        ident = g.setdefault("identity", {})
        if dev_id is None:
            ident.pop("pci_device_id", None)
        else:
            ident["pci_device_id"] = dev_id
    h = dc.generate_report(None, i, stress_with(skipped))
    m = re.search(r"DCGM Diagnostics.{0,400}?badge-(pass|warn|fail|na)", h, re.S)
    sev = re.findall(r"sev-(crit|warn|info)\">(\w+)</span></td><td class=\"dim\">DCGM", h)
    return (m.group(1) if m else "?"), (sev[0][1] if sev else "none")

rc = 0
cases = [
    ("0x2331", False, ["nvbandwidth"], "pass", "INFO",
     "H100 PCIe, no bridges: skipped by design"),
    ("0x2331", True,  ["nvbandwidth"], "pass", "INFO",
     "H100 PCIe WITH bridges: still by design, not a review item"),
    ("0x2330", True,  ["nvbandwidth"], "warn", "WARNING",
     "H100 SXM5, where DCGM does qualify it: a skip is unexplained"),
    (None,     True,  ["nvbandwidth"], "warn", "WARNING",
     "unknown SKU: evidence is not waived on a guess"),
    ("0x2331", False, ["nvbandwidth", "memory_bandwidth"], "warn", "WARNING",
     "another test also skipped: not excused by the SKU rule"),
]
for dev_id, nvlink, skipped, want_stage, want_sev, label in cases:
    stage, sev = run(dev_id, nvlink, skipped)
    ok = (stage == want_stage and sev == want_sev)
    print(("  ok    " if ok else "  FAIL  ") + label
          + ("" if ok else "  -> stage=%s sev=%s want %s/%s"
             % (stage, sev, want_stage, want_sev)))
    if not ok:
        rc = 1

# An informational note must not hold a stage below PASS.
st = copy.deepcopy(base)
st["verdict"] = {"overall": "WARN",
                 "issues": [{"issue": "just a note", "severity": "info"}]}
h = dc.generate_report(None, inv, st)
m = re.search(r"DCGM Diagnostics.{0,400}?badge-(pass|warn|fail|na)", h, re.S)
if m and m.group(1) == "pass":
    print("  ok    an info-only stage is not held at WARN")
else:
    print("  FAIL  info-only issue held the stage below PASS"); rc = 1

sys.exit(rc)
PYEOF

hdr "Raw logs are embedded in full, condensed not truncated"
python3 - <<'PYEOF' || rc=1
import sys, json
sys.path.insert(0, "reporting")
import device_certificate as dc

rc = 0

# A gpu-burn-shaped log. The real tool repaints its progress line in place
# roughly 70 times per 0.1% step it reports, which is what turns a 3600 s run
# into ~22 MB. Those repaints are terminal artifacts, not distinct evidence.
step = "%.1f%%  proc'd: %d (35942 Gflop/s)   errors: 0   temps: 71 C"
raw = ""
for pct in range(100):
    for rep in range(70):
        raw += (step % (pct / 10.0, pct * 1000 + rep)) + "\r"
raw += "\nTested 8 GPUs:\n" + "".join("\tGPU %d: OK\n" % g for g in range(8))

out, note = dc.condense_terminal_log(raw)
checks = [
    (len(out) < len(raw) / 20, "condensing cuts the log by more than 20x"),
    (out.count("%") == 100, "exactly one line survives per reported step"),
    ("GPU 7: OK" in out, "the per-GPU verdict survives condensing"),
    ("69069" in out, "the latest counters of each step are the ones kept"),
    ("repaints collapsed" in note, "the note states what was collapsed"),
]
for ok, label in checks:
    print(("  ok    " if ok else "  FAIL  ") + label)
    if not ok:
        rc = 1

# A log with no repaints must pass through untouched.
plain = "line one\nline two\n"
if dc.condense_terminal_log(plain) == (plain, ""):
    print("  ok    a log without repaints is passed through unchanged")
else:
    print("  FAIL  condensing altered a log with no carriage returns"); rc = 1

# The whole thing is embedded: no cap engaged at the real default, no link out.
h = dc.render_log_block("gpu-burn output", raw)
if "ev-trunc" not in h and "repaints collapsed" in h:
    print("  ok    embedded in full at the default cap, stating the condensing")
else:
    print("  FAIL  block truncated at the default cap"); rc = 1
if "ev-link" not in h and "MAAS" not in h:
    print("  ok    nothing links out -- the reader may have no MAAS access")
else:
    print("  FAIL  block links to MAAS"); rc = 1

# The cap is a backstop; if it ever engages it must be visible, and tail mode
# must keep the end, where the verdict is.
h = dc.render_log_block("x", raw, max_chars=3000, keep="tail")
if "ev-trunc" in h and "omitted" in h and "GPU 7: OK" in h:
    print("  ok    backstop cap is flagged and tail mode keeps the verdict")
else:
    print("  FAIL  backstop truncation silent or lost the tail"); rc = 1
if "0.0%" in dc.render_log_block("x", raw, max_chars=3000, keep="head"):
    print("  ok    head mode keeps the start")
else:
    print("  FAIL  head mode wrong"); rc = 1

# Absent evidence renders nothing; log text is untrusted and must be escaped.
if all(dc.render_log_block("x", v) == "" for v in ("", None, "   \n")):
    print("  ok    absent evidence renders no block")
else:
    print("  FAIL  empty input produced a block"); rc = 1
h = dc.render_log_block("x", "a<script>alert(1)</script>")
if "&lt;script&gt;" in h and "<script>alert" not in h:
    print("  ok    log text is HTML-escaped")
else:
    print("  FAIL  log text not escaped"); rc = 1
if dc.decode_b64_text("!!!bad!!!") == "" and dc.decode_b64_text(None) == "":
    print("  ok    unusable base64 degrades to empty rather than raising")
else:
    print("  FAIL  base64 decode not defensive"); rc = 1

# End to end: the artifact the specification names, per card.
inv = json.load(open("tests/fixtures/reports/inventory-healthy.json"))
stress = json.load(open("tests/fixtures/reports/stress-pass.json"))
burn = json.load(open("tests/fixtures/reports/burnin-full.json"))
html = dc.generate_report(None, inv, stress, burnin=burn)
want = "nvidia-smi -q -d ROW_REMAPPER,ECC"
n = len(inv["gpus"])
if html.count(want) == n:
    print("  ok    %s attached for each of %d cards" % (want, n))
else:
    print("  FAIL  %s appears %d times, want %d" % (want, html.count(want), n)); rc = 1
if "Raw Evidence" in html and 'class="ev-block"' in html:
    print("  ok    raw evidence section rendered, collapsed by default")
else:
    print("  FAIL  raw evidence section missing"); rc = 1
if ".ev-block { display: none !important; }" in html:
    print("  ok    print stylesheet omits the raw logs")
else:
    print("  FAIL  raw logs would print"); rc = 1

# When MAAS returned a full log for a stage, the JSON excerpt of the same stage
# must not be shown as well.
html2 = dc.generate_report(None, inv, stress, burnin=burn,
                           script_logs={"burnin": "FULL BURN LOG\nTested 8 GPUs:\n"})
if "FULL BURN LOG" in html2 and "output (sustained load)" not in html2:
    print("  ok    full stage log supersedes the embedded excerpt")
else:
    print("  FAIL  excerpt and full log both shown for the same stage"); rc = 1

sys.exit(rc)
PYEOF

hdr "Certificate renders script 89's chassis inventory"
python3 - <<'PYEOF' || rc=1
import sys, json, os
sys.path.insert(0, "reporting")
import device_certificate as dc

F = "tests/fixtures/reports"
inv = json.load(open(f"{F}/inventory-healthy.json"))
stress = json.load(open(f"{F}/stress-pass.json"))
hw = json.load(open("tests/fixtures/hwinv-183.json"))

rc = 0
def check(ok, label):
    global rc
    print(("  ok    " if ok else "  FAIL  ") + label)
    if not ok:
        rc = 1

html = dc.generate_report(None, inv, stress, hwinv=hw)
check("Chassis &amp; Components" in html, "the chassis section is rendered")
check("1.01" in html, "BMC firmware revision reaches the report -- limb 1(d)")
check("Mon Oct  2 10:19:00 2023" in html, "FRU board manufacture date is shown")

# The whole point of limb 1(f): one usable figure, with what makes it usable
# -- the agreement between drives -- visible rather than asserted.
check("18,318 h" in html, "power-on hours shown as a single server figure")
check("fitted from new" in html and "spread 1 h" in html,
      "the spread is shown, so the figure is justified not asserted")
check("94% powered" in html, "SEL span corroborates the drive hours")
check("all drives agree, so fitted from new" in html,
      "agreement between drives is what justifies using the highest")

# max() is the tightest lower bound on server hours, but a drive moved from
# another machine reads high and would inflate it. Powered time cannot exceed
# elapsed time, so that contradiction is the detector.
import copy
moved = copy.deepcopy(hw)
moved["power_on"].update(drive_hours_max=40000, drive_hours_min=18317,
                         drive_hours_spread=21683)
h4 = dc.generate_report(None, inv, stress, hwinv=moved)
check("another machine" in h4 and "Do not use this as the server figure" in h4,
      "hours beyond the chassis age AND a wide spread: a drive carried a "
      "previous life, so the figure is withheld")

# The same contradiction with a ZERO spread means the opposite thing, and a
# real machine showed it: 995 powered days against an 880-day event log, three
# drives agreeing to the hour. Drives that agree cannot each have been swapped
# in separately, so the log is what is short -- a cleared SEL. Reading this as
# a transplant would have withheld a sound figure.
cleared = json.load(open("tests/fixtures/hwinv-182-cleared-sel.json"))
h6 = dc.generate_report(None, inv, stress, hwinv=cleared)
check("cleared SEL looks like" in h6 and "drive figure stands" in h6,
      "the same contradiction with zero spread is read as a cleared log")
check("Do not use this as the server figure" not in h6,
      "and the figure is kept, not withheld")
check("23,890 h" in h6, "so the Server Report cell still gets its number")

spread = copy.deepcopy(hw)
spread["power_on"].update(drive_hours_max=18318, drive_hours_min=9000,
                          drive_hours_spread=9318)
h5 = dc.generate_report(None, inv, stress, hwinv=spread)
check("not fitted with the chassis" in h5 and "remains a lower bound" in h5,
      "drives that disagree are flagged, and the bound is stated honestly")

check("4 of 4 bays populated" in html, "every PSU bay is accounted for")

# Confirmed on real hardware: a chassis with four PSUs fitted recorded only
# two SMBIOS type-39 entries. Reporting the DMI count alone would understate
# what is being sold, so the BMC's sensor count is the presence evidence.
under = json.load(open("tests/fixtures/hwinv-psu-underreport.json"))
h3 = dc.generate_report(None, inv, stress, hwinv=under)
check("4 bays fitted" in h3 and "records only 2" in h3,
      "BMC bay count overrides an under-reporting SMBIOS table")
# The shortfall is a BIOS limitation, established by measurement across six
# machines: SMBIOS type 39 lists every bay only from BIOS 1003 onward. Saying
# so is the difference between a report that explains itself and one that
# leaves the Buyer to wonder whether two supplies are missing.
check("BIOS 1003" in h3 and "not a missing supply" in h3,
      "the shortfall is attributed to BIOS, not to absent hardware")
# The unlisted bays are fitted and drawing power, so they appear as rows with
# their identity fields blank -- counted only, they would be invisible.
check(h3.count("not in SMBIOS") == 2 and h3.count("Present per BMC sensor") == 2,
      "the bays SMBIOS omits are still listed, one row each")
check("G1236302NA000000004" in html, "PSU serials reach the report -- limb 1(c)")

# These two drives are invisible to lsblk, lshw and MAAS. If the report does
# not carry them, nothing the Buyer receives describes the drives being sold.
check("S664NE0T000002" in html and "S664NE0T000001" in html,
      "drives behind the RAID controller appear in the report")
check("behind RAID" in html, "and are marked as reached through the controller")
check("MT0000XZ0000" in html, "ConnectX-7 board serial from VPD -- limb 1(c)")

# Absence must read as absence, not as a clean result.
html2 = dc.generate_report(None, inv, stress)
check("script 89 did not run" in html2,
      "a machine without script 89 says so rather than showing blanks")
check("Chassis &amp; Components" in html2,
      "the section is still present, so the gap is visible")

sys.exit(rc)
PYEOF

hdr "Certificate of Sanitisation carries the evidence, not a summary of it"
python3 - <<'PYEOF' || rc=1
import json, os, sys
from html.parser import HTMLParser

sys.path.insert(0, "reporting")
import sanitisation_certificate as sc

rc = 0
def check(ok, label):
    global rc
    print(("  ok    " if ok else "  FAIL  ") + label)
    if not ok:
        rc = 1

F = "tests/fixtures"
def load(name):
    with open(os.path.join(F, name)) as fh:
        return json.load(fh)

def render(name):
    return sc.generate_certificate(load(name))

complete = render("sanitize-complete.json")
audit    = render("sanitize-audit.json")
nopurge  = render("sanitize-no-purge-path.json")
failed   = render("sanitize-failed-drive.json")
block    = render("sanitize-block-fallback.json")

# Every serial in the evidence has to reach the page. A certificate that
# summarises rather than lists is not evidence of anything in particular.
serials = [m["serial_number"] for m in load("sanitize-complete.json")["media"]
           if m.get("physical_media") and m.get("serial_number")]
check(serials and all(s in complete for s in serials),
      "every physical device's serial number appears on the certificate")
check(all(m["command"] in complete
          for m in load("sanitize-complete.json")["media"] if m.get("command")),
      "the exact command issued to each device appears verbatim")
check("PURGED" in complete and "CLEARED ONLY" not in complete,
      "a purged drive is shown as purged")

# The distinction the whole document exists to preserve.
check("CLEARED ONLY" in sc.generate_certificate(
          {"report_metadata": {"mode": "sanitize"},
           "verdict": {"overall": "FAIL"},
           "media": [{"physical_media": True, "device": "/dev/sdb",
                      "device_type": "sata", "method": "overwrite",
                      "method_level": "clear", "result": "success"}]}),
      "an overwrite is shown as a clear, never as a purge")
check("NIST purge" in complete,
      "and a purge is labelled with the standard it was measured against")

# Audit mode is not a sanitisation record, however clean its verdict.
check("Audit only" in audit and "not a sanitisation record" in audit,
      "an audit run says on its face that it erased nothing")

# The drives that block release must be impossible to miss.
check("NOT PURGED" in nopurge and "not yet purged" in nopurge
      and "reaches NIST purge" in nopurge,
      "media that could not be purged are shown as such, with the routes to a Purge")
# Phrase-level, not substring: a note may legitimately say that deleting a
# virtual disk loses its geometry, and a bare "destro" cannot tell that apart
# from proposing that a drive be destroyed.
low = nopurge.lower()
check(not any(x in low for x in ("physically destro", "destruction certificate",
                                 "quarantin", "removed from the host and destro")),
      "the certificate never proposes destroying a drive")
check("800-88r1" in nopurge and "user-addressable" in nopurge,
      "the Clear/Purge distinction is stated on the face of the document")
check("ATA Security feature set" in nopurge,
      "and the evidence that the drive itself is purge-capable is shown")
check("not cleared for release" in nopurge,
      "and the certificate says the host cannot be released")
check("FAILED" in failed and "remediation" in failed.lower(),
      "a failed erase reaches the remediation register")

# The block-erase fallback is a purge, and is named as the method it was.
check("block erase" in block,
      "the fallback method is named, not folded into 'sanitised'")

# Section 14 is a human's job and the document does not pretend otherwise.
# There is no sign-off block: a checklist for someone to tick is a
# release-approval artefact, and this document's job is to show what was
# destroyed. Section 14's review is recorded in the ticket instead.
check("&#9744;" not in complete and "Sign-off" not in complete,
      "the certificate carries no checklist for a person to tick")

# The page opens with the answer and nothing else; the reasoning is one click
# away rather than in the reader's face.
opening = complete.split("<details")[0]
check("What was destroyed" in opening and "PASS" in opening,
      "the verdict and the table of what was destroyed are what a reader meets")
check("Technical detail" in complete and "800-88r1" in complete,
      "the Clear/Purge reasoning is present, behind an expander")
check("user-addressable" not in opening,
      "and it is not in the opening, which states the outcome instead")

# The evidence has to survive without the MAAS that produced it.
check("ev-pre" in complete and "nvme sanitize" in complete,
      "the raw audit log is embedded in the page")
check("href=" not in complete and "<a " not in complete,
      "the certificate links nowhere -- the evidence is in the file or it is not evidence")

# Absence reads as absence.
sparse = sc.generate_certificate({"report_metadata": {"mode": "audit"},
                                  "verdict": {"overall": "N/A"}})
check("not recorded" in sparse and "cannot be shown" in sparse,
      "a certificate with no evidence says so rather than showing blanks")

class P(HTMLParser):
    def __init__(self):
        HTMLParser.__init__(self)
        self.stack = []; self.err = 0
        self.void = {"br", "img", "hr", "meta", "link", "input", "col"}
    def handle_starttag(self, t, a):
        if t not in self.void: self.stack.append(t)
    def handle_endtag(self, t):
        if t in self.void: return
        if self.stack and self.stack[-1] == t: self.stack.pop()
        elif t in self.stack:
            while self.stack and self.stack.pop() != t: self.err += 1
        else: self.err += 1

for name, html in (("complete", complete), ("audit", audit), ("no-purge", nopurge),
                   ("failed", failed), ("sparse", sparse)):
    p = P(); p.feed(html)
    check(not p.err and not p.stack,
          "%s certificate is well-formed HTML" % name)

sys.exit(rc)
PYEOF

hdr "Rendered HTML is well-formed"
python3 - <<'PY' || rc=1
from html.parser import HTMLParser
import sys
class P(HTMLParser):
    def __init__(s):
        super().__init__(); s.stack=[]; s.err=0
        s.void={'br','img','hr','meta','link','input','col'}
    def handle_starttag(s,t,a):
        if t not in s.void: s.stack.append(t)
    def handle_endtag(s,t):
        if t in s.void: return
        if s.stack and s.stack[-1]==t: s.stack.pop()
        elif t in s.stack:
            while s.stack and s.stack.pop()!=t: s.err+=1
        else: s.err+=1
p=P(); p.feed(open('/tmp/rep-test.html').read())
bad = p.err or p.stack
print(f"  {'FAIL' if bad else 'ok  '}  unbalanced={p.err} unclosed={len(p.stack)}")
sys.exit(1 if bad else 0)
PY

printf '\n%s\n' "$([[ $rc -eq 0 ]] && echo 'ALL TESTS PASSED' || echo 'FAILURES PRESENT')"
exit $rc
