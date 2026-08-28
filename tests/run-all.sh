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
# Exactly the layout MAAS reports for CA1-ESC812-182.
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
