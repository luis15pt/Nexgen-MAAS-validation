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
noload=$(mktemp -d)
for t in nvidia-smi dmesg systemctl lspci; do ln -sf "$PWD/tests/stubs/$t" "$noload/$t"; done
check "no load generator available"          FAIL  PATH="$noload:/usr/bin:/bin"
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
echo "gpu-burn (the load source the specification names):"
check "gpu-burn all OK"                      PASS
check "gpu-burn reports a FAULTY GPU"        FAIL  STUB_GPU_BURN_FAULTY=1
check "gpu-burn counts computation errors"   FAIL  STUB_GPU_BURN_ERRORS=17
check "gpu-burn covers only half the GPUs"   FAIL  STUB_GPU_BURN_PARTIAL=1 STUB_GPU_COUNT=2

echo "Load tool selection:"
rm -f /tmp/stub-*
lf=$(mktemp)
o=$(env STUB_GPU_COUNT=8 STUB_LOADED_FILE="$lf" BURN_TOOL=dcgmi-diag BURN_DURATION=2 \
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
o=$(env STUB_GPU_COUNT=8 STUB_LOADED_FILE="$(mktemp)" BURN_TOOL=dcgmi-diag STUB_LOAD_EXIT=1 \
        BURN_DURATION=2 BURN_SAMPLE_INTERVAL=1 STUB_LOAD_SLEEP=1 bash "$SCRIPT" 2>/dev/null)
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


# Both of these are regressions from a real 8-GPU H100 run on driver 595.71.05.
echo "Report assembly under a large artifact (MAX_ARG_STRLEN):"
rm -f /tmp/stub-*
o=$(env STUB_GPU_COUNT=2 STUB_GPU_BURN_BLOAT=1 BURN_TOOL=gpu-burn BURN_DURATION=2 \
        BURN_SAMPLE_INTERVAL=1 STUB_LOAD_SLEEP=1 STUB_LOADED_GPUS=0,1 \
        STUB_POWER_LOADED=340.00 bash "$SCRIPT" 2>/dev/null)
# The real failure: base64 of gpu-burn's log exceeded a single argv entry's
# 131072-byte cap, jq never ran, and the script still printed "Verdict: PASS"
# with no JSON at all.
if printf '%s' "$o" | jq empty 2>/dev/null; then
    v=$(printf '%s' "$o" | jq -r '.verdict.overall')
    n=$(printf '%s' "$o" | jq '.burn_in.gpu_burn_results | length')
    b=$(printf '%s' "$o" | jq -r '.burn_in.gpu_burn_output_b64 | length')
    if [[ "$v" == "PASS" && "$n" == "2" && "$b" -gt 0 ]]; then
        printf '  ok    oversized gpu-burn log still yields valid JSON (verdict=%s, %s results, %s b64 bytes)\n' "$v" "$n" "$b"
    else
        printf '  FAIL  verdict=%s results=%s b64=%s\n' "$v" "$n" "$b"; rc=1
    fi
else
    printf '  FAIL  no valid JSON emitted for an oversized gpu-burn log\n'; rc=1
fi

# Same oversized log, but with a real error count buried in it. Guards the
# single-pass max: the old `... | sort -rn | head -1 || echo "0"` raced under
# pipefail and returned "0\n0", which is invalid JSON and killed the report.
rm -f /tmp/stub-*
o=$(env STUB_GPU_COUNT=2 STUB_GPU_BURN_BLOAT=1 STUB_GPU_BURN_ERRORS=17 \
        BURN_TOOL=gpu-burn BURN_DURATION=2 BURN_SAMPLE_INTERVAL=1 STUB_LOAD_SLEEP=1 \
        STUB_LOADED_GPUS=0,1 STUB_POWER_LOADED=340.00 \
        bash "$SCRIPT" 2>/dev/null)
v=$(printf '%s' "$o" | jq -r '.verdict.overall' 2>/dev/null)
e=$(printf '%s' "$o" | jq -r '.burn_in.gpu_burn_error_count' 2>/dev/null)
if [[ "$v" == "FAIL" && "$e" == "17" ]]; then
    printf '  ok    error count survives a 250KB log of "errors: 0" (verdict=%s, count=%s)\n' "$v" "$e"
else
    printf '  FAIL  oversized log with errors: verdict=%s count=%s (want FAIL/17)\n' "$v" "$e"; rc=1
fi

echo "Counter columns resolved by name, not position:"
# Driver 595 rejects pcie.replay_counter, so it is dropped from the query and
# every later column shifts. Reading $4..$6 blind put remapped-row values under
# the wrong keys and left remapped_rows_uncorrectable_delta permanently null,
# which silently disabled the FAIL gate that depends on it.
rm -f /tmp/stub-*
o=$(env STUB_GPU_COUNT=2 STUB_NO_REPLAY_FIELD=1 BURN_DURATION=2 \
        BURN_SAMPLE_INTERVAL=1 STUB_LOAD_SLEEP=1 STUB_LOADED_GPUS=0,1 \
        STUB_POWER_LOADED=340.00 STUB_REMAP_UCE_POST=1 bash "$SCRIPT" 2>/dev/null)
v=$(printf '%s' "$o" | jq -r '.verdict.overall' 2>/dev/null)
ru=$(printf '%s' "$o" | jq -r '.burn_in.counter_deltas[0].remapped_rows_uncorrectable_delta' 2>/dev/null)
rp=$(printf '%s' "$o" | jq -r '.burn_in.counter_deltas[0].pcie_replay_delta' 2>/dev/null)
if [[ "$v" == "FAIL" && "$ru" == "1" && "$rp" == "null" ]]; then
    printf '  ok    replay field absent: remap gate still fires (verdict=%s, remapUCE delta=%s, replay=%s)\n' "$v" "$ru" "$rp"
else
    printf '  FAIL  verdict=%s remapUCE=%s replay=%s (want FAIL/1/null)\n' "$v" "$ru" "$rp"; rc=1
fi

rm -f /tmp/stub-*
o=$(env STUB_GPU_COUNT=2 STUB_NO_REPLAY_FIELD=1 BURN_DURATION=2 \
        BURN_SAMPLE_INTERVAL=1 STUB_LOAD_SLEEP=1 STUB_LOADED_GPUS=0,1 \
        STUB_POWER_LOADED=340.00 bash "$SCRIPT" 2>/dev/null)
v=$(printf '%s' "$o" | jq -r '.verdict.overall' 2>/dev/null)
# A missing replay counter is not a gate input, so it must not degrade a clean run.
if [[ "$v" == "PASS" ]]; then
    printf '  ok    replay field absent on a clean run does not degrade the verdict\n'
else
    printf '  FAIL  clean run with no replay field -> %s, want PASS\n' "$v"
    printf '%s' "$o" | jq -r '.verdict.issues[]?.issue' | sed 's/^/          /'; rc=1
fi

exit $rc
