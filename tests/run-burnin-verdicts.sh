#!/bin/bash
# Drives the burn-in script against stubbed telemetry, asserting the
# verdict for each scenario. No GPU required.
cd "$(dirname "$0")/.." || exit 1
export PATH="$PWD/tests/stubs:$PATH" STUB_GPU_COUNT=2
SCRIPT=commissioning-scripts/99-nexgen-gpu-burn-in.sh
rc=0

check() { # $1=label  $2=expected verdict  rest=env assignments
    local label="$1" want="$2"; shift 2
    rm -f /tmp/stub-dmesg-called.* /tmp/stub-snap-called.*
    local out got
    out=$(env BURN_DURATION=2 BURN_SAMPLE_INTERVAL=1 STUB_LOAD_SLEEP=1 "$@" \
          bash "$SCRIPT" 2>/dev/null)
    got=$(printf '%s' "$out" | jq -r '.verdict.overall' 2>/dev/null)
    if [[ "$got" == "$want" ]]; then
        printf '  ok    %-42s -> %s\n' "$label" "$got"
    else
        printf '  FAIL  %-42s -> got %s, want %s\n' "$label" "${got:-<no json>}" "$want"
        printf '%s' "$out" | jq -r '.verdict.issues[]?.issue' 2>/dev/null | sed 's/^/          /'
        rc=1
    fi
}

echo "Burn-in verdict matrix:"
check "healthy, power-capped, characterize"  PASS
check "healthy, power-capped, enforce"       WARN  BURN_MODE=enforce
check "power cap + clock floor met"          PASS  BURN_MODE=enforce BURN_MIN_SM_CLOCK_MHZ=1500
check "sustained clock below floor"          FAIL  BURN_MODE=enforce BURN_MIN_SM_CLOCK_MHZ=1800
check "hw thermal slowdown, characterize"    PASS  STUB_TH_HWTH=Active
check "hw thermal slowdown, enforce"         FAIL  BURN_MODE=enforce BURN_MIN_SM_CLOCK_MHZ=1500 STUB_TH_HWTH=Active
check "power brake slowdown, enforce"        FAIL  BURN_MODE=enforce BURN_MIN_SM_CLOCK_MHZ=1500 STUB_TH_HWPB=Active
check "load never reached power floor"       FAIL  BURN_MODE=enforce BURN_MIN_SM_CLOCK_MHZ=1500 BURN_MIN_POWER_W=300
check "disqualifying Xid 79"                 FAIL  STUB_DMESG_XID=79
check "disqualifying Xid 94"                 FAIL  STUB_DMESG_XID=94
check "non-disqualifying Xid 13"             WARN  STUB_DMESG_XID=13
check "new uncorrectable ECC under load"     FAIL  STUB_ECC_UCE_AGG_POST=2
check "new uncorrectable remapped row"       FAIL  STUB_REMAP_UCE_POST=1
check "no load generator available"          FAIL  BURN_TOOL=gpu-burn
check "load generator killed by timeout"     FAIL  STUB_LOAD_EXIT=124
exit $rc
