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
    out=$(env BURN_DURATION=2 BURN_SAMPLE_INTERVAL=1 STUB_LOAD_SLEEP=1 \
              STUB_LOADED_GPUS=0,1 STUB_POWER_LOADED=340.00 "$@" \
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
check "load present but below power floor"    FAIL  BURN_MODE=enforce BURN_MIN_SM_CLOCK_MHZ=1500 BURN_MIN_POWER_W=300 STUB_POWER_LOADED=200.00
check "disqualifying Xid 79"                 FAIL  STUB_DMESG_XID=79
check "disqualifying Xid 94"                 FAIL  STUB_DMESG_XID=94
check "non-disqualifying Xid 13"             WARN  STUB_DMESG_XID=13
check "new uncorrectable ECC under load"     FAIL  STUB_ECC_UCE_AGG_POST=2
check "new uncorrectable remapped row"       FAIL  STUB_REMAP_UCE_POST=1
check "no load generator available"          FAIL  BURN_TOOL=gpu-burn
check "killed short of the required window"   FAIL  STUB_LOAD_EXIT=124
# Regressions from a real run: a tool that overruns its own -d but delivered the
# full window is a tool quirk, and a load that covered only some GPUs means the
# uncovered ones were never tested.
check "overran -d but window complete"       WARN  STUB_LOAD_EXIT=124 STUB_LOAD_SLEEP=3 BURN_DURATION=2
check "load covered only half the GPUs"      FAIL  STUB_LOADED_GPUS=0 STUB_POWER=71.22
check "load covered every GPU"               PASS  STUB_LOADED_GPUS=0,1 STUB_POWER_LOADED=340.00

# Structural: one load process per GPU, launched concurrently. Handed a
# multi-GPU id list, dcgmproftester batches instead -- a real 8-GPU run loaded
# 4 cards for 1780s and the other 4 for 280s.
echo "Load tool selection:"
rm -f /tmp/stub-*
lf=$(mktemp)
o=$(env STUB_GPU_COUNT=8 STUB_LOADED_FILE="$lf" BURN_DURATION=2 \
        BURN_SAMPLE_INTERVAL=1 STUB_LOAD_SLEEP=1 bash "$SCRIPT" 2>/dev/null)
mode=$(printf '%s' "$o" | jq -r '.burn_in.load_mode')
covered=$(printf '%s' "$o" | jq '[.burn_in.telemetry[]|select(.loaded_seconds>0)]|length')
if [[ "$mode" == "dcgmi-diag" && "$covered" == "8" ]]; then
    printf '  ok    default is dcgmi diag targeted_stress, all 8 GPUs covered\n'
else
    printf '  FAIL  default mode=%s covered=%s\n' "$mode" "$covered"; rc=1
fi
# NVIDIA's own load test failing is a hardware verdict, not a tool quirk
rm -f /tmp/stub-*
o=$(env STUB_GPU_COUNT=8 STUB_LOADED_FILE="$(mktemp)" STUB_LOAD_EXIT=1 BURN_DURATION=2 \
        BURN_SAMPLE_INTERVAL=1 STUB_LOAD_SLEEP=1 bash "$SCRIPT" 2>/dev/null)
if [[ "$(printf '%s' "$o" | jq -r '.verdict.overall')" == "FAIL" ]]; then
    printf '  ok    a targeted_stress failure fails the run\n'
else
    printf '  FAIL  targeted_stress failure did not fail the run\n'; rc=1
fi
rm -f "$lf"

echo "Load launch topology (dcgmproftester fallback):"
llog=$(mktemp)
rm -f /tmp/stub-*
env STUB_GPU_COUNT=8 STUB_LOAD_LOG="$llog" STUB_LOADED_FILE="$(mktemp)" \
    BURN_TOOL=dcgmproftester BURN_DURATION=2 BURN_SAMPLE_INTERVAL=1 STUB_LOAD_SLEEP=1 \
    bash "$SCRIPT" >/dev/null 2>&1
got=$(sort -n "$llog" | tr '\n' ' ' | sed 's/ $//')
if [[ "$got" == "0 1 2 3 4 5 6 7" ]]; then
    printf '  ok    one process per GPU: %s\n' "$got"
else
    printf '  FAIL  expected one process per GPU 0-7, got: %s\n' "$got"; rc=1
fi
rm -f "$llog"

exit $rc
