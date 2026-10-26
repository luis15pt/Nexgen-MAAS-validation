#!/bin/bash
# Drives the DCGM stress-test script against DCGM fixtures using stub tools,
# asserting the verdict each one must produce.  No GPU required.
cd "$(dirname "$0")/.." || exit 1
export PATH="$PWD/tests/stubs:$PATH"
SCRIPT=commissioning-scripts/95-nexgen-dcgm-level3.sh
rc=0
# Every run below writes a ladder record; keep them out of /run and out of
# each other's way.
export NEXGEN_DCGM_LADDER_FILE=/tmp/nexgen-dcgm-ladder-test.$$.jsonl
rm -f "$NEXGEN_DCGM_LADDER_FILE"

check() { # $1=fixture  $2=expected verdict  $3=exit code to simulate
    local out verdict
    out=$(DCGM_FIXTURE="tests/fixtures/dcgm/$1.json" DCGM_FIXTURE_EXIT="${3:-0}" \
          bash "$SCRIPT" 2>/dev/null)
    verdict=$(printf '%s' "$out" | jq -r '.verdict.overall' 2>/dev/null)
    if [[ "$verdict" == "$2" ]]; then
        printf '  ok    %-16s exit=%-3s -> %s\n' "$1" "${3:-0}" "$verdict"
    else
        printf '  FAIL  %-16s exit=%-3s -> got %s, want %s\n' "$1" "${3:-0}" "${verdict:-<no json>}" "$2"
        printf '%s' "$out" | jq -r '.verdict.issues[]?.issue' 2>/dev/null | sed 's/^/          /'
        rc=1
    fi
}

echo "Stress-test verdict matrix:"
check all-pass        PASS
check one-fail        FAIL
check all-config      FAIL     # REJECT #5: config log is not a hardware pass
check all-retest      FAIL     # REJECT #5
check all-skip        FAIL     # nothing passed: enumeration is not a test
check unknown-status  FAIL     # unrecognised status is not a pass
check empty-results   FAIL     # no test executed
check unparseable     FAIL     # no test evidence
check all-pass        WARN 1   # exit code and results disagree
check all-pass        FAIL 124 # timeout kill: run incomplete

# dcgmi says "1 GPU found" on a single-GPU host. Matching only "GPUs found"
# counted it as 0 and failed the run as a DCGM/driver incompatibility.
out=$(STUB_GPU_COUNT=1 DCGM_FIXTURE="tests/fixtures/dcgm/all-pass.json" \
      bash "$SCRIPT" 2>&1)
if grep -q "DCGM sees 1 GPU(s)" <<<"$out"; then
    echo "  ok    single-GPU host: discovery counts 1 GPU"
else
    echo "  FAIL  single-GPU host: discovery miscounted"
    grep -E "DCGM (sees|discovery)" <<<"$out" | sed 's/^/          /'
    rc=1
fi


echo "DCGM ladder:"
# Level comes from the file name, so one body serves four scripts.
for spec in "93-nexgen-dcgm-level1.sh:1" "94-nexgen-dcgm-level2.sh:2" \
            "95-nexgen-dcgm-level3.sh:3" "96-nexgen-dcgm-level4.sh:4"; do
    f="${spec%%:*}"; want="${spec#*:}"; rm -f "$NEXGEN_DCGM_LADDER_FILE"
    got=$(DCGM_FIXTURE=tests/fixtures/dcgm/all-pass.json bash "commissioning-scripts/$f" 2>/dev/null \
          | jq -r '.dcgm_diagnostics.run_level')
    if [[ "$got" == "$want" ]]; then printf '  ok    %s runs level %s\n' "$f" "$got"
    else printf '  FAIL  %s ran level %s, want %s\n' "$f" "$got" "$want"; rc=1; fi
done
# An explicit level still wins over the file name (named test lists go this way).
got=$(DCGM_DIAG_LEVEL=pcie,memory DCGM_FIXTURE=tests/fixtures/dcgm/all-pass.json \
      bash commissioning-scripts/93-nexgen-dcgm-level1.sh 2>/dev/null | jq -r '.dcgm_diagnostics.run_level')
if [[ "$got" == "pcie,memory" ]]; then echo "  ok    DCGM_DIAG_LEVEL overrides the file name"
else echo "  FAIL  DCGM_DIAG_LEVEL override gave $got"; rc=1; fi

# A rung records its verdict, and a failed lower rung stops the higher ones.
rm -f "$NEXGEN_DCGM_LADDER_FILE"
DCGM_FIXTURE=tests/fixtures/dcgm/one-fail.json bash commissioning-scripts/94-nexgen-dcgm-level2.sh >/dev/null 2>&1
rec=$(jq -c 'select(.level==2) | .verdict' "$NEXGEN_DCGM_LADDER_FILE" 2>/dev/null)
if [[ "$rec" == '"FAIL"' ]]; then echo "  ok    level 2 FAIL recorded in the ladder file"
else echo "  FAIL  ladder record after a level-2 failure: ${rec:-none}"; rc=1; fi
out=$(DCGM_FIXTURE=tests/fixtures/dcgm/all-pass.json bash commissioning-scripts/95-nexgen-dcgm-level3.sh 2>/dev/null)
sk=$(printf '%s' "$out" | jq -r '.skipped // false'); v=$(printf '%s' "$out" | jq -r '.verdict.overall')
if [[ "$sk" == "true" && "$v" == "FAIL" ]]; then echo "  ok    level 3 skips (FAIL) after level 2 failed"
else echo "  FAIL  level 3 after a level-2 failure: skipped=$sk verdict=$v"; rc=1; fi
n=$(jq -c 'select(.level==3)' "$NEXGEN_DCGM_LADDER_FILE" 2>/dev/null | wc -l)
if [[ "$n" == 0 ]]; then echo "  ok    a skipped rung leaves no record of its own"
else echo "  FAIL  skipped level 3 wrote $n record(s)"; rc=1; fi
v=$(DCGM_LADDER_IGNORE_PREVIOUS=1 DCGM_FIXTURE=tests/fixtures/dcgm/all-pass.json \
    bash commissioning-scripts/95-nexgen-dcgm-level3.sh 2>/dev/null | jq -r '.verdict.overall')
if [[ "$v" == "PASS" ]]; then echo "  ok    DCGM_LADDER_IGNORE_PREVIOUS=1 runs it anyway"
else echo "  FAIL  override gave $v"; rc=1; fi
# A lower rung that was not uploaded is not consulted; WARN does not gate.
rm -f "$NEXGEN_DCGM_LADDER_FILE"
DCGM_FIXTURE=tests/fixtures/dcgm/all-pass.json DCGM_FIXTURE_EXIT=1 bash commissioning-scripts/93-nexgen-dcgm-level1.sh >/dev/null 2>&1
v=$(DCGM_FIXTURE=tests/fixtures/dcgm/all-pass.json bash commissioning-scripts/96-nexgen-dcgm-level4.sh 2>/dev/null | jq -r '.verdict.overall')
if [[ "$v" == "PASS" ]]; then echo "  ok    level 4 runs after level 1 WARN with levels 2-3 absent"
else echo "  FAIL  level 4 after L1 WARN gave $v"; rc=1; fi
# A preflight failure is recorded too, so the rungs above it skip.
rm -f "$NEXGEN_DCGM_LADDER_FILE"
STUB_GPU_COUNT=0 DCGM_FIXTURE=tests/fixtures/dcgm/all-pass.json bash commissioning-scripts/93-nexgen-dcgm-level1.sh >/dev/null 2>&1 || true
rec=$(jq -c 'select(.level==1) | .verdict' "$NEXGEN_DCGM_LADDER_FILE" 2>/dev/null)
if [[ "$rec" == '"FAIL"' ]]; then echo "  ok    a preflight failure is recorded as a level FAIL"
else echo "  FAIL  preflight failure record: ${rec:-none}"; rc=1; fi
rm -f "$NEXGEN_DCGM_LADDER_FILE"

exit $rc
