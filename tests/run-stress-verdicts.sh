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
exit $rc
