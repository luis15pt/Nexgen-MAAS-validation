#!/bin/bash
# Drives the DCGM stress-test script against DCGM fixtures using stub tools,
# asserting the verdict each one must produce.  No GPU required.
cd "$(dirname "$0")/.." || exit 1
export PATH="$PWD/tests/stubs:$PATH"
SCRIPT=commissioning-scripts/98-nexgen-gpu-stress-test.sh
rc=0

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

# nvbandwidth measures peer bandwidth over NVLink. With no NVLink fitted DCGM
# skips it, which is expected operation rather than missing evidence -- and it
# was putting a WARNING on every report for this fleet.
echo "nvbandwidth skip vs NVLink presence:"
nvbw() { # $1=fixture $2=expected verdict $3..=env
    local out verdict label="$1"; local want="$2"; shift 2
    out=$(env DCGM_FIXTURE="tests/fixtures/dcgm/$label.json" DCGM_FIXTURE_EXIT=0 "$@" \
          bash "$SCRIPT" 2>/dev/null)
    verdict=$(printf '%s' "$out" | jq -r '.verdict.overall' 2>/dev/null)
    local sev
    sev=$(printf '%s' "$out" | jq -r '[.verdict.issues[]?|select(.issue|test("skip"))|.severity]|join(",")' 2>/dev/null)
    if [[ "$verdict" == "$want" ]]; then
        printf '  ok    %-34s -> %-4s (skip severity: %s)\n' "$*" "$verdict" "${sev:-none}"
    else
        printf '  FAIL  %-34s -> got %s, want %s\n' "$*" "${verdict:-<no json>}" "$want"
        printf '%s' "$out" | jq -r '.verdict.issues[]?.issue' | sed 's/^/          /'
        rc=1
    fi
}
# nvbandwidth-skip: every test passes except nvbandwidth, which is exactly what
# an H100 PCIe host with no bridges reports.
nvbw nvbandwidth-skip PASS                        # measured no NVLink -> excused
nvbw nvbandwidth-skip WARN STUB_NVLINK=1          # NVLink fitted -> unexplained
nvbw nvbandwidth-skip WARN STUB_TOPO_FAIL=1       # presence unknown -> not waived
# Nothing passed at all, so this is a FAIL regardless of NVLink -- enumeration
# is not a test.
nvbw all-skip FAIL

o=$(env DCGM_FIXTURE="tests/fixtures/dcgm/all-skip.json" DCGM_FIXTURE_EXIT=0 \
        bash "$SCRIPT" 2>/dev/null)
nv=$(printf '%s' "$o" | jq -r '.system.nvlink_present')
if [[ "$nv" == "false" || "$nv" == "true" || "$nv" == "null" ]]; then
    printf '  ok    nvlink_present recorded in the report (%s)\n' "$nv"
else
    printf '  FAIL  nvlink_present missing from the report\n'; rc=1
fi

exit $rc
