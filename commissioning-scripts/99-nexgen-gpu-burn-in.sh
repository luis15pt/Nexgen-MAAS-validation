#!/bin/bash
# --- Start MAAS 1.0 script metadata ---
# title: NexGen GPU Sustained Burn-In (optional)
# description: >-
#   Applies a sustained full-power load and records what the GPUs
#   actually did under it: power, temperature, SM clock, throttle reasons, ECC
#   and remapped-row deltas, PCIe replay deltas, and any Xid events raised
#   during the window.  A DCGM diagnostic is a diagnostic, not a burn-in --
#   reballed and reflowed cards pass cold and fail hot, so only sustained load
#   exposes them.  Optional: simply do not upload this script to skip it.
#   Runs last: it is the longest phase, so a failure earlier in the pipeline
#   costs the least time. Requires 90-nexgen-gpu-install to have run first.
#   Override: BURN_DURATION=1800 BURN_MODE=characterize|enforce BURN_TOOL=auto|dcgmproftester|gpu-burn
# script_type: commissioning
# parallel: disabled
# hardware_type: gpu
# timeout: 01:30:00
# destructive: false
# may_reboot: false
# --- End MAAS 1.0 script metadata ---

set -o pipefail
trap 'warn "Command failed at line $LINENO (exit code $?)"' ERR

###############################################################################
# CONFIG
###############################################################################
# Seconds of sustained load. Set to 5 minutes to validate the pipeline end to
# end; a real acceptance run needs >= 1800s, and the report deliberately marks
# anything shorter for review rather than accepting it. Raise this once the
# steps are confirmed -- the declared MAAS timeout (01:30:00) leaves room up to
# roughly 80 minutes without touching the metadata.
BURN_DURATION="${BURN_DURATION:-300}"
BURN_SAMPLE_INTERVAL="${BURN_SAMPLE_INTERVAL:-10}"
# A GPU counts as under load when it draws at least this fraction of its own
# power limit. Used to prove the load actually reached every card.
BURN_LOADED_POWER_FRAC="${BURN_LOADED_POWER_FRAC:-0.5}"
# Fraction of BURN_DURATION each GPU must actually spend under load.
BURN_COVERAGE_FRAC="${BURN_COVERAGE_FRAC:-0.9}"
BURN_TOOL="${BURN_TOOL:-auto}"   # auto | gpu-burn | dcgmi-diag | dcgmproftester
# Extra gpu-burn arguments. "-tc" uses the tensor cores, "-d" double precision.
# Plain SGEMM is the default because it is the most portable and already pulls an
# H100 PCIe to its power limit.
BURN_GPU_BURN_ARGS="${BURN_GPU_BURN_ARGS:-}"
# characterize: measure and report, gate on nothing.  Run this first on
# known-good, properly-cooled cards to find out whether they ever reach thermal
# slowdown and what their sustained clock floor actually is -- the enforce
# thresholds should come from that data, not from a guess.
BURN_MODE="${BURN_MODE:-characterize}"
# Only consulted when BURN_MODE=enforce.  Left unset by default precisely
# because it must be derived from a characterization run.
BURN_MIN_SM_CLOCK_MHZ="${BURN_MIN_SM_CLOCK_MHZ:-}"
BURN_MIN_POWER_W="${BURN_MIN_POWER_W:-}"
GPU_BURN_REPO="${GPU_BURN_REPO:-https://github.com/wilicc/gpu-burn}"
WORK_DIR="/tmp/gpu-burnin-$$"
SCRIPT_VERSION="1.5.0"

# Xid events the acceptance spec treats as disqualifying.  Others (13, 31, 43)
# are typically application faults raised by the load itself, not hardware.
XID_CRITICAL="${XID_CRITICAL:-48 63 64 79 92 94 95}"

###############################################################################
# LOGGING
###############################################################################
log()  { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [INFO]  $*" >&2; }
warn() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [WARN]  $*" >&2; }
err()  { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [ERROR] $*" >&2; }

mkdir -p "$WORK_DIR"
trap 'rm -rf "$WORK_DIR"' EXIT

get_smi_header_info() {
    local header
    header=$(nvidia-smi 2>/dev/null | head -5)
    SMI_DRIVER=$(echo "$header" | grep -oP 'Driver Version:\s*\K[0-9.]+' || echo "unknown")
    SMI_CUDA=$(echo "$header" | grep -oP 'CUDA Version:\s*\K[0-9.]+' || echo "unknown")
    SMI_GPU_COUNT=$(nvidia-smi --query-gpu=count --format=csv,noheader,nounits 2>/dev/null | head -1 || echo "0")
}

fail_json() {
    local msg="$1"
    err "$msg"
    get_smi_header_info
    jq -n --arg v "$SCRIPT_VERSION" --arg m "$msg" \
        --arg drv "${SMI_DRIVER:-unknown}" --arg cuda "${SMI_CUDA:-unknown}" \
        --argjson gpus "${SMI_GPU_COUNT:-0}" \
        '{
            report_metadata:{script_version:$v, script_name:"gpu-burn-in"},
            verdict:{overall:"FAIL", issues:[{"issue":$m,"severity":"critical"}]},
            system:{nvidia_driver_version:$drv, cuda_version:$cuda, gpu_count:$gpus},
            burn_in:{tool:"none", mode:"'"$BURN_MODE"'", duration_seconds:0, gpus:[], xid:{critical:[],other:[]}}
        }'
    exit 1
}

###############################################################################
# THROTTLE-REASON FIELD NAMING
###############################################################################
# Renamed from clocks_throttle_reasons.* to clocks_event_reasons.* in newer
# drivers.  Probe once rather than assuming, and degrade to no throttle data
# rather than losing the whole sample query.
detect_throttle_fields() {
    local base f
    for base in clocks_event_reasons clocks_throttle_reasons; do
        if nvidia-smi --query-gpu="${base}.sw_power_cap" \
                --format=csv,noheader,nounits &>/dev/null; then
            THROTTLE_BASE="$base"
            THROTTLE_FIELDS=""
            for f in sw_power_cap hw_thermal_slowdown hw_power_brake_slowdown sw_thermal_slowdown; do
                THROTTLE_FIELDS+=",${base}.${f}"
            done
            log "Throttle reasons via ${base}.*"
            return 0
        fi
    done
    THROTTLE_BASE=""
    THROTTLE_FIELDS=""
    warn "Throttle reason fields unavailable on this driver -- throttling will not be characterised"
}

###############################################################################
# PRE/POST COUNTER SNAPSHOT
###############################################################################
# Probe each counter field once and keep only those this driver accepts.  A
# single unsupported field makes the whole --query-gpu call fail, and these
# field names do get removed and renamed between driver branches, so the set is
# discovered rather than assumed.  SNAP_FIELDS is the surviving list.
SNAP_FIELDS=""
SNAP_MISSING=""
probe_counter_fields() {
    local f
    SNAP_FIELDS="index"
    for f in ecc.errors.corrected.aggregate.total \
             ecc.errors.uncorrected.aggregate.total \
             pcie.replay_counter \
             remapped_rows.correctable \
             remapped_rows.uncorrectable; do
        if nvidia-smi --query-gpu="index,$f" --format=csv,noheader,nounits &>/dev/null; then
            SNAP_FIELDS+=",$f"
        else
            SNAP_MISSING+="${SNAP_MISSING:+,}$f"
        fi
    done
    if [[ -n "$SNAP_MISSING" ]]; then
        warn "Counter fields unsupported on driver ${SMI_DRIVER}: $SNAP_MISSING"
        warn "  Their deltas will be reported as null rather than losing the whole snapshot."
    fi
    log "Counter snapshot fields: $SNAP_FIELDS"
}

# Absolute counters are not the interesting quantity: a replay count can carry
# boot-time link-training events that are not defects.  What matters is the
# delta across the load window, so each counter is snapshotted either side.
snapshot_counters() {
    local out="$1" err="$1.err"
    : > "$out"
    if ! nvidia-smi --query-gpu="$SNAP_FIELDS" --format=csv,noheader,nounits \
            > "$out" 2>"$err"; then
        warn "Counter snapshot failed even with the probed field set"
        [[ -s "$err" ]] && { warn "  nvidia-smi said:"; head -3 "$err" >&2; }
        : > "$out"
    fi
    # Header order for the delta pass, so it does not assume fixed columns.
    printf '%s\n' "$SNAP_FIELDS" > "${out}.fields"
}

###############################################################################
# LOAD TOOL SELECTION
###############################################################################
# dcgmproftester ships with DCGM, which script 90 already installs, so it needs
# no compilation and no network.  gpu-burn is offered as a genuinely
# independent load source but has to be built from source on the node, which
# needs outbound network and adds a failure mode -- hence not the default.
# LOAD_MODE=per-gpu  one process per GPU, launched concurrently
# LOAD_MODE=single    one process that loads every GPU itself
# LOAD_MODE=gpu-burn     one process, loads every GPU itself, per-GPU verdict
# LOAD_MODE=dcgmi-diag   NVIDIA's targeted_stress plugin, one invocation
# LOAD_MODE=per-gpu      one process per GPU, launched concurrently
select_load_tool() {
    LOAD_TOOL="" LOAD_BIN="" LOAD_MODE="" LOAD_DIR=""

    # gpu-burn first: it is what the acceptance specification names for the
    # sustained phase, it loads every GPU in one process by design, and it
    # reports a per-GPU OK/FAULTY verdict plus a computation error count -- which
    # is stronger evidence than telemetry alone.
    if [[ "$BURN_TOOL" == "auto" || "$BURN_TOOL" == "gpu-burn" ]]; then
        if command -v gpu_burn &>/dev/null; then
            LOAD_TOOL="gpu-burn"; LOAD_BIN="$(command -v gpu_burn)"
            LOAD_DIR="$(dirname "$LOAD_BIN")"; LOAD_MODE="gpu-burn"
            log "Load tool: gpu-burn (preinstalled)"
            return 0
        fi
        if build_gpu_burn; then
            LOAD_TOOL="gpu-burn"; LOAD_BIN="$WORK_DIR/gpu-burn/gpu_burn"
            LOAD_DIR="$WORK_DIR/gpu-burn"; LOAD_MODE="gpu-burn"
            log "Load tool: gpu-burn (built from source)"
            return 0
        fi
        if [[ "$BURN_TOOL" == "gpu-burn" ]]; then
            warn "gpu-burn requested but unavailable and could not be built"
            return 1
        fi
        warn "gpu-burn unavailable -- falling back to DCGM targeted_stress"
    fi

    # NVIDIA's own sustained-load plugin: level-3 diagnostic, every GPU in one
    # invocation, duration via a documented parameter.
    if [[ "$BURN_TOOL" == "auto" || "$BURN_TOOL" == "dcgmi-diag" ]]; then
        if command -v dcgmi &>/dev/null; then
            LOAD_TOOL="dcgmi-diag"; LOAD_BIN="dcgmi"; LOAD_MODE="dcgmi-diag"
            log "Load tool: dcgmi diag targeted_stress (all GPUs, one invocation)"
            return 0
        fi
        [[ "$BURN_TOOL" == "dcgmi-diag" ]] && \
            { warn "dcgmi requested but not found"; return 1; }
    fi

    # Last resort. NVIDIA documents dcgmproftester as a load generator for
    # validating DCGM's *measurement* path, not as a stress tool, and handed a
    # multi-GPU id list it loads the cards in sequential BATCHES: a real 8-GPU
    # run drove 4 cards for 1780s, re-initialised, then started the other 4 and
    # got 280s into them. -t 1004 is DCGM_FI_PROF_PIPE_TENSOR_ACTIVE, a
    # half-precision matrix-multiply-accumulate on the tensor cores. Launched as
    # one process per GPU so every card is loaded for the whole window.
    if [[ "$BURN_TOOL" == "auto" || "$BURN_TOOL" == "dcgmproftester" ]]; then
        local cand
        for cand in dcgmproftester13 dcgmproftester12 dcgmproftester11 dcgmproftester; do
            if command -v "$cand" &>/dev/null; then
                LOAD_TOOL="$cand"; LOAD_BIN="$cand"; LOAD_MODE="per-gpu"
                log "Load tool: $cand (one process per GPU)"
                return 0
            fi
        done
        [[ "$BURN_TOOL" == "dcgmproftester" ]] && \
            { warn "dcgmproftester requested but not found"; return 1; }
    fi
    return 1
}

# Build gpu-burn from source. Needs a compiler, make, git and outbound network;
# script 90 has already installed the CUDA toolkit that provides nvcc. Every
# failure path is logged, because a silent build failure here would downgrade
# the run to a weaker load source without anyone noticing.
build_gpu_burn() {
    log "Building gpu-burn from source..."
    local cudapath=""
    local c
    for c in /usr/local/cuda /usr/local/cuda-13.0 /usr/local/cuda-12.8 /usr; do
        [[ -x "$c/bin/nvcc" ]] && { cudapath="$c"; break; }
    done
    if [[ -z "$cudapath" ]] && command -v nvcc &>/dev/null; then
        cudapath="$(dirname "$(dirname "$(command -v nvcc)")")"
    fi
    if [[ -z "$cudapath" ]]; then
        warn "  nvcc not found -- cannot build gpu-burn (is cuda-toolkit installed by script 90?)"
        return 1
    fi
    log "  CUDA at $cudapath"

    local missing=""
    command -v git  &>/dev/null || missing+=" git"
    command -v make &>/dev/null || missing+=" make"
    command -v g++  &>/dev/null || missing+=" g++"
    if [[ -n "$missing" ]]; then
        log "  Installing build prerequisites:$missing"
        DEBIAN_FRONTEND=noninteractive apt-get install -y -qq $missing 2>&1 | tail -3 >&2 \
            || warn "  apt-get failed for:$missing"
    fi
    for c in git make g++; do
        command -v "$c" &>/dev/null || { warn "  $c still unavailable -- cannot build gpu-burn"; return 1; }
    done

    if ! git clone --depth 1 "$GPU_BURN_REPO" "$WORK_DIR/gpu-burn" >&2 2>&1; then
        warn "  git clone failed (no outbound network to ${GPU_BURN_REPO}?)"
        return 1
    fi
    if ! make -C "$WORK_DIR/gpu-burn" CUDAPATH="$cudapath" >&2 2>&1; then
        warn "  make failed"
        return 1
    fi
    [[ -x "$WORK_DIR/gpu-burn/gpu_burn" ]] || { warn "  gpu_burn binary not produced"; return 1; }
    log "  gpu-burn built"
    return 0
}

###############################################################################
# APPLY LOAD
###############################################################################
# Returns the worst exit status seen.  For per-GPU mode that means a single
# wedged GPU is visible rather than averaged away.
apply_load() {
    local ceiling=$(( BURN_DURATION + 300 ))
    local rc=0

    if [[ "$LOAD_MODE" == "gpu-burn" ]]; then
        # gpu-burn loads every GPU itself and prints a per-GPU OK/FAULTY summary.
        # Run from its own directory: it loads compare.ptx relative to the binary.
        log "  gpu_burn ${BURN_GPU_BURN_ARGS} ${BURN_DURATION}"
        ( cd "$LOAD_DIR" && timeout "$ceiling" "$LOAD_BIN" $BURN_GPU_BURN_ARGS "$BURN_DURATION" ) \
            > "$WORK_DIR/gpu-burn.out" 2>&1 || rc=$?
        log "  ---------- begin gpu-burn output ----------"
        head -c 500000 "$WORK_DIR/gpu-burn.out" >&2 2>/dev/null || true
        log "  ---------- end gpu-burn output ----------"
        return $rc
    fi

    if [[ "$LOAD_MODE" == "dcgmi-diag" ]]; then
        # One invocation, every GPU, duration set through the documented
        # parameter. Output is captured rather than left on stdout, which
        # carries this script's own JSON report.
        log "  dcgmi diag -r targeted_stress -p targeted_stress.test_duration=${BURN_DURATION}"
        timeout "$ceiling" dcgmi diag -r targeted_stress \
            -p "targeted_stress.test_duration=${BURN_DURATION}" -j \
            > "$WORK_DIR/diag.json" 2>"$WORK_DIR/diag.err" || rc=$?
        # Retain both streams in the MAAS commissioning log.
        log "  ---------- begin dcgmi diag output ----------"
        head -c 500000 "$WORK_DIR/diag.json" >&2 2>/dev/null || true
        [[ -s "$WORK_DIR/diag.err" ]] && head -c 100000 "$WORK_DIR/diag.err" >&2
        log "  ---------- end dcgmi diag output ----------"
        return $rc
    fi

    if [[ "$LOAD_MODE" == "single" ]]; then
        timeout "$ceiling" "$LOAD_BIN" "$BURN_DURATION" >&2 2>&1 || rc=$?
        return $rc
    fi

    # One process per GPU, all started before any is waited on.
    local g pid e
    local -a pids=() failed=()
    for (( g = 0; g < ${SMI_GPU_COUNT:-1}; g++ )); do
        timeout "$ceiling" "$LOAD_BIN" --no-dcgm-validation -t 1004 \
            -d "$BURN_DURATION" -i "$g" >&2 2>&1 &
        pids+=("$!")
    done
    log "  Launched ${#pids[@]} concurrent load process(es), one per GPU"

    for g in "${!pids[@]}"; do
        pid="${pids[$g]}"
        e=0
        wait "$pid" || e=$?
        if [[ "$e" -ne 0 ]]; then
            failed+=("GPU${g}:${e}")
            [[ "$e" -gt "$rc" ]] && rc="$e"
        fi
    done
    [[ ${#failed[@]} -gt 0 ]] && warn "  Load process exits: ${failed[*]}"
    return $rc
}

###############################################################################
# GPU-BURN RESULT PARSING
###############################################################################
# gpu-burn ends with, e.g.:
#     Tested 8 GPUs:
#         GPU 0: OK
#         GPU 1: FAULTY
# and reports a running "errors: N" per GPU. A FAULTY card produced wrong
# arithmetic under load, which is a hardware verdict and not something the
# telemetry can show.
parse_gpu_burn_results() {
    local out="$WORK_DIR/gpu-burn.out"
    [[ -s "$out" ]] || { echo "[]"; return; }
    sed -nE 's/^[[:space:]]*GPU[[:space:]]+([0-9]+):[[:space:]]*(OK|FAULTY).*/\1 \2/p' "$out" \
    | awk '{ printf "%s{\"gpu_index\":%s,\"status\":\"%s\"}", (n++?",":"["), $1, $2 }
            END { printf "%s", (n?"]":"[]") }'
}

# Highest "errors: N" seen in the run, which is non-zero only on a real fault.
gpu_burn_error_total() {
    local out="$WORK_DIR/gpu-burn.out"
    [[ -s "$out" ]] || { echo "0"; return; }
    grep -oE 'errors:[[:space:]]*[0-9]+' "$out" 2>/dev/null \
        | grep -oE '[0-9]+' | sort -rn | head -1 || echo "0"
}

###############################################################################
# SAMPLER
###############################################################################
start_sampler() {
    local out="$1"
    (
        while :; do
            nvidia-smi --query-gpu="index,power.draw,power.limit,temperature.gpu,temperature.memory,clocks.sm${THROTTLE_FIELDS}" \
                --format=csv,noheader,nounits 2>/dev/null \
                | sed "s/^/$(date +%s), /" >> "$out" || true
            sleep "$BURN_SAMPLE_INTERVAL"
        done
    ) &
    SAMPLER_PID=$!
}

stop_sampler() {
    [[ -n "${SAMPLER_PID:-}" ]] && kill "$SAMPLER_PID" 2>/dev/null || true
    wait "${SAMPLER_PID:-}" 2>/dev/null || true
}

# Aggregate samples per GPU.
#
# awk is used ONLY to extract fields and coerce them to JSON literals; every
# comparison and aggregation happens in jq, where types are explicit.
#
# An earlier version compared values inside awk and reported min > max on real
# hardware -- e.g. clocks "min":1005,"max":990 and power "min":334.44,"max":49.14.
# Both are exactly the *lexicographic* extremes of the sampled values, i.e. awk
# compared them as text. Whether awk compares numerically depends on how the
# value was produced and on the awk implementation, so the comparison has been
# moved somewhere it cannot be ambiguous rather than patched in place.
aggregate_samples() {
    local csv="$1" have_throttle="$2"
    [[ ! -s "$csv" ]] && { echo "[]"; return; }
    awk -F',' -v ht="$have_throttle" '
        function num(s) {
            gsub(/^[ \t]+|[ \t]+$/, "", s)
            return (s ~ /^-?[0-9]+([.][0-9]+)?$/) ? s + 0 : "null"
        }
        function act(s) {
            gsub(/^[ \t]+|[ \t]+$/, "", s)
            return (s == "Active") ? "true" : "false"
        }
        {
            gi = num($2); if (gi == "null") next
            printf "{\"gpu_index\":%s,\"power_w\":%s,\"power_limit_w\":%s", gi, num($3), num($4)
            printf ",\"temp_gpu_c\":%s,\"temp_mem_c\":%s,\"clocks_sm_mhz\":%s", num($5), num($6), num($7)
            if (ht == "1")
                printf ",\"swpc\":%s,\"hwth\":%s,\"hwpb\":%s,\"swth\":%s", \
                       act($8), act($9), act($10), act($11)
            printf "}\n"
        }
    ' "$csv" \
    | jq -s \
        --argjson ht "${have_throttle:-0}" \
        --argjson interval "${BURN_SAMPLE_INTERVAL:-10}" \
        --argjson frac "${BURN_LOADED_POWER_FRAC:-0.5}" '
        def r2: . * 100 | round / 100;
        def stats(f):
            (map(f) | map(select(. != null))) as $v
            | if ($v | length) == 0 then null
              else {min: ($v | min), max: ($v | max), mean: (($v | add) / ($v | length) | r2)}
              end;
        group_by(.gpu_index)
        | map(. as $s
            # "Loaded" = drawing at least $frac of its own power limit. Counting
            # this per GPU is what reveals a load generator that only covered
            # some of the cards -- the run looks fine in aggregate otherwise.
            | ([$s[] | select(.power_w != null and .power_limit_w != null
                              and .power_w >= (.power_limit_w * $frac))] | length) as $loaded
            | {
                gpu_index:     $s[0].gpu_index,
                samples:       ($s | length),
                loaded_samples: $loaded,
                loaded_seconds: ($loaded * $interval),
                power_w:       ($s | stats(.power_w)),
                power_limit_w: ($s | stats(.power_limit_w) | if . then .max else null end),
                temp_gpu_c:    ($s | stats(.temp_gpu_c)),
                temp_memory_c: ($s | stats(.temp_mem_c)),
                clocks_sm_mhz: ($s | stats(.clocks_sm_mhz)),
                throttle_samples: (if $ht == 1 then {
                    sw_power_cap:            ([$s[] | select(.swpc)] | length),
                    hw_thermal_slowdown:     ([$s[] | select(.hwth)] | length),
                    hw_power_brake_slowdown: ([$s[] | select(.hwpb)] | length),
                    sw_thermal_slowdown:     ([$s[] | select(.swth)] | length)
                } else null end)
              })'
}

###############################################################################
# CUMULATIVE COUNTER CHECK
###############################################################################
# Counters are read from `nvidia-smi -q` text rather than --query-gpu, because
# several counter fields are absent on current drivers (remapped_rows.
# uncorrectable is unsupported on 595) and one missing field takes the whole
# --query-gpu call with it.
#
# Script 92 writes a baseline BEFORE any load runs; every later phase compares
# against it. That attributes a fault to the phase that induced it, and means
# the check still happens when the optional burn-in is not uploaded. A delta
# measured only across the burn-in window cannot see an error raised by the
# DCGM diagnostic, because that error is already in the window's own baseline.
NEXGEN_BASELINE_FILE="${NEXGEN_BASELINE_FILE:-/run/nexgen-gpu-counter-baseline.json}"

smi_section() {
    awk -v want="$1" '
        !found {
            h = $0
            gsub(/^[[:space:]]+|[[:space:]]+$/, "", h)
            if (h == want) { match($0, /^ */); ind = RLENGTH; found = 1 }
            next
        }
        found {
            if ($0 ~ /^[[:space:]]*$/) next
            match($0, /^ */)
            if (RLENGTH <= ind) exit
            print
        }
    '
}

smi_field() {
    awk -v want="$1" '
        {
            p = index($0, ":")
            if (p == 0) next
            k = substr($0, 1, p - 1); v = substr($0, p + 1)
            gsub(/^[[:space:]]+|[[:space:]]+$/, "", k)
            gsub(/^[[:space:]]+|[[:space:]]+$/, "", v)
            if (k == want) { print v; exit }
        }
    '
}

to_json_num() {
    local v="${1//,/}" n
    case "$v" in
        ""|*N/A*|*"Not Supported"*|*"Not Available"*|*Unknown*|*Disabled*) echo "null"; return ;;
    esac
    n=$(printf '%s' "$v" | grep -oE '^-?[0-9]+(\.[0-9]+)?' | head -1)
    [[ -z "$n" ]] && { echo "null"; return; }
    echo "$n"
}

to_json_bool() {
    case "$1" in
        Yes|yes|YES|True|true) echo "true"  ;;
        No|no|NO|False|false)  echo "false" ;;
        *)                     echo "null"  ;;
    esac
}

sum_or_empty() {
    local a b
    a=$(printf '%s' "${1:-}" | grep -oE '[0-9]+' | head -1)
    b=$(printf '%s' "${2:-}" | grep -oE '[0-9]+' | head -1)
    if [[ -z "$a" && -z "$b" ]]; then echo ""; else echo $(( ${a:-0} + ${b:-0} )); fi
}

# Per-GPU aggregate error counters, as a JSON array.
snapshot_gpu_counters() {
    local n gi out agg remap pci first=1
    n="${SMI_GPU_COUNT:-0}"
    # Do not depend on a caller having populated SMI_GPU_COUNT: an empty count
    # silently yields "[]", which looks like a valid baseline and makes every
    # later delta null.
    if [[ -z "$n" || "$n" == "0" ]]; then
        n=$(nvidia-smi --query-gpu=count --format=csv,noheader,nounits 2>/dev/null | head -1)
        n="${n//[^0-9]/}"
        n="${n:-0}"
    fi
    printf '['
    for (( gi = 0; gi < n; gi++ )); do
        out=$(nvidia-smi -i "$gi" -q 2>/dev/null || true)
        [[ -z "$out" ]] && continue
        agg=$(printf   '%s\n' "$out" | smi_section "ECC Errors" | smi_section "Aggregate")
        remap=$(printf '%s\n' "$out" | smi_section "Remapped Rows")
        pci=$(printf   '%s\n' "$out" | smi_section "PCI")
        [[ $first -eq 0 ]] && printf ','
        first=0
        printf '{"gpu_index":%d' "$gi"
        printf ',"ecc_corrected_aggregate":%s' \
            "$(to_json_num "$(sum_or_empty \
                "$(printf '%s\n' "$agg" | smi_field 'DRAM Correctable')" \
                "$(printf '%s\n' "$agg" | smi_field 'SRAM Correctable')")")"
        printf ',"ecc_uncorrected_aggregate":%s' \
            "$(to_json_num "$(sum_or_empty \
                "$(printf '%s\n' "$agg" | smi_field 'DRAM Uncorrectable')" \
                "$(printf '%s\n' "$agg" | smi_field 'SRAM Uncorrectable')")")"
        printf ',"remapped_rows_correctable":%s'   "$(to_json_num  "$(printf '%s\n' "$remap" | smi_field 'Correctable Error')")"
        printf ',"remapped_rows_uncorrectable":%s' "$(to_json_num  "$(printf '%s\n' "$remap" | smi_field 'Uncorrectable Error')")"
        printf ',"remapped_rows_pending":%s'       "$(to_json_bool "$(printf '%s\n' "$remap" | smi_field 'Pending')")"
        printf ',"remapped_rows_failure":%s'       "$(to_json_bool "$(printf '%s\n' "$remap" | smi_field 'Remapping Failure Occurred')")"
        printf ',"replays_since_reset":%s'         "$(to_json_num  "$(printf '%s\n' "$pci"   | smi_field 'Replays Since Reset')")"
        printf '}'
    done
    printf ']'
}

# Compare a fresh snapshot against the pre-load baseline.
counters_since_baseline() {
    local now="$1"
    if [[ ! -s "$NEXGEN_BASELINE_FILE" ]]; then
        jq -n --argjson now "$now" \
            '{baseline_available:false, deltas:[], new_faults:0, final:$now}'
        return
    fi
    jq -n --slurpfile base "$NEXGEN_BASELINE_FILE" --argjson now "$now" '
        def d($a; $b): if ($a != null and $b != null) then $a - $b else null end;
        (($base[0] // []) | map({key:(.gpu_index|tostring), value:.}) | from_entries) as $bm
        | [ $now[] | . as $n | ($bm[($n.gpu_index|tostring)] // null) as $p | {
              gpu_index: $n.gpu_index,
              ecc_corrected_delta:               d($n.ecc_corrected_aggregate;   ($p.ecc_corrected_aggregate // null)),
              ecc_uncorrected_delta:             d($n.ecc_uncorrected_aggregate; ($p.ecc_uncorrected_aggregate // null)),
              remapped_rows_correctable_delta:   d($n.remapped_rows_correctable;  ($p.remapped_rows_correctable // null)),
              remapped_rows_uncorrectable_delta: d($n.remapped_rows_uncorrectable;($p.remapped_rows_uncorrectable // null)),
              replay_delta:                      d($n.replays_since_reset;       ($p.replays_since_reset // null)),
              remapped_rows_pending_now: $n.remapped_rows_pending,
              remapped_rows_failure_now: $n.remapped_rows_failure
          } ] as $dl
        # A counter that went DOWN was cleared by something between the
        # baseline and now -- nvidia-smi -p clears ECC counts, and
        # replays_since_reset is per-reset by definition. The comparison is then
        # void: it cannot show a fault it no longer has the history for. That is
        # reported rather than passed, since "cannot prove clean" is not clean.
        | ([ $dl[] | select(
              ((.ecc_uncorrected_delta // 0) < 0)
              or ((.ecc_corrected_delta // 0) < 0)
              or ((.remapped_rows_uncorrectable_delta // 0) < 0)
              or ((.remapped_rows_correctable_delta // 0) < 0)
              or ((.replay_delta // 0) < 0)) ]) as $back
        | {
            baseline_available: true,
            deltas: $dl,
            final: $now,
            counters_went_backwards: (($back | length) > 0),
            counters_reset_gpus: [ $back[] | .gpu_index ],
            new_faults: ([ $dl[] | select(
                ((.ecc_uncorrected_delta // 0) > 0)
                or ((.remapped_rows_uncorrectable_delta // 0) > 0)
                or (.remapped_rows_failure_now == true)
                or (.remapped_rows_pending_now == true)) ] | length)
          }'
}

# Emits the cumulative block on stdout capture; sets POST_NEW_FAULTS.
# Also dumps the closing `nvidia-smi -q` to stderr, so the final state of every
# card is retained in the MAAS commissioning log as the last word on it.
post_test_counter_check() {
    local now
    log "=== Post-test counter check ==="
    now=$(snapshot_gpu_counters)
    POST_COUNTERS=$(counters_since_baseline "$now")
    POST_NEW_FAULTS=$(printf '%s' "$POST_COUNTERS" | jq '.new_faults // 0')
    if [[ "$(printf '%s' "$POST_COUNTERS" | jq -r '.baseline_available')" != "true" ]]; then
        warn "  No pre-load baseline at $NEXGEN_BASELINE_FILE -- deltas unavailable"
        warn "  (script 92 writes it; absolute counters are still reported)"
    else
        log "  Cumulative deltas since the pre-load baseline:"
        printf '%s' "$POST_COUNTERS" | jq -r '.deltas[] |
            "    GPU \(.gpu_index): eccUCE+\(.ecc_uncorrected_delta // "?") " +
            "eccCE+\(.ecc_corrected_delta // "?") " +
            "remapUCE+\(.remapped_rows_uncorrectable_delta // "?") " +
            "replay+\(.replay_delta // "?")"' >&2 || true
    fi
    if [[ "$(printf '%s' "$POST_COUNTERS" | jq -r '.counters_went_backwards // false')" == "true" ]]; then
        POST_COUNTERS_VOID=1
        local back
        back=$(printf '%s' "$POST_COUNTERS" | jq -r '[.counters_reset_gpus[] | "GPU \(.)"] | join(", ")')
        warn "  Counters went BACKWARDS since the baseline on: $back"
        warn "  Something cleared them mid-sequence (nvidia-smi -p clears ECC counts;"
        warn "  replays_since_reset is per-reset). The comparison cannot evidence"
        warn "  health for those GPUs."
    fi
    log "---------- begin final nvidia-smi -q ----------"
    nvidia-smi -q >&2 2>&1 || warn "final nvidia-smi -q failed"
    log "---------- end final nvidia-smi -q ----------"
}

###############################################################################
# HALT GATE
###############################################################################
# Script 91 halts the run when it has had to enable ECC.  Every ECC counter
# read afterwards starts from zero -- enabling ECC does not backfill, and while
# it was off nothing was being detected at all -- so anything measured in this
# run cannot evidence the card's memory health.  Exiting immediately saves the
# 30-90 minutes the burn-in and stress phases would otherwise spend producing
# numbers that only look clean.  The marker lives on tmpfs, so the next
# commissioning run proceeds normally.
NEXGEN_HALT_FILE="${NEXGEN_HALT_FILE:-/run/nexgen-commissioning-halt}"
halt_gate() {
    [[ -f "$NEXGEN_HALT_FILE" ]] || return 0
    local reason
    reason=$(head -c 800 "$NEXGEN_HALT_FILE" 2>/dev/null | tr '\n' ' ')
    err "=============================================================="
    err "  SKIPPED -- commissioning halted by an earlier script"
    err "=============================================================="
    err "  $reason"
    err ""
    err "  This script is not running: the evidence it would collect"
    err "  cannot be used. Re-run commissioning."
    err "=============================================================="
    jq -n --arg v "$SCRIPT_VERSION" --arg name "gpu-burn-in" --arg r "$reason" \
        --arg ts "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" \
        '{
            report_metadata:{script_version:$v, script_name:$name, generated_at:$ts, duration_seconds:0},
            verdict:{overall:"FAIL", issues:[{"issue":("Skipped -- " + $r),"severity":"critical"}]},
            skipped:true, halt_reason:$r
        }'
    exit 1
}

###############################################################################
# MAIN
###############################################################################
main() {
    SCRIPT_START=$(date +%s)

    halt_gate
    log "=========================================="
    log "NexGen GPU Burn-In v${SCRIPT_VERSION}"
    log "mode=$BURN_MODE duration=${BURN_DURATION}s tool=$BURN_TOOL"
    log "=========================================="

    local missing=""
    command -v nvidia-smi &>/dev/null || missing+=" nvidia-smi"
    command -v jq         &>/dev/null || missing+=" jq"
    [[ -n "$missing" ]] && fail_json "Missing required tools:$missing -- run 90-nexgen-gpu-install first"
    nvidia-smi &>/dev/null || fail_json "nvidia-smi not functional -- run 90-nexgen-gpu-install first"

    get_smi_header_info
    log "Driver $SMI_DRIVER, CUDA $SMI_CUDA, $SMI_GPU_COUNT GPU(s)"
    detect_throttle_fields
    probe_counter_fields

    local issues="[]" overall="PASS"

    # --- pre-load state -------------------------------------------------
    snapshot_counters "$WORK_DIR/pre.csv"
    dmesg 2>/dev/null > "$WORK_DIR/dmesg-pre.txt" || : > "$WORK_DIR/dmesg-pre.txt"

    # --- load -----------------------------------------------------------
    local tool="none" load_exit=0 load_dur=0
    if select_load_tool; then
        tool="$LOAD_TOOL"
        log "Applying sustained load for ${BURN_DURATION}s ($LOAD_MODE, $LOAD_TOOL)"
        start_sampler "$WORK_DIR/samples.csv"
        local t0
        t0=$(date +%s)
        # Hard ceiling well past BURN_DURATION: a wedged load generator must not
        # be left to be killed by MAAS, which would emit no report at all.
        apply_load || load_exit=$?
        load_dur=$(( $(date +%s) - t0 ))
        # A rejected argument fails fast. Distinguish that from a real load fault
        # and retry as a single whole-node process rather than losing the run.
        if [[ "$load_exit" -ne 0 && "$load_dur" -lt 30 && "$LOAD_MODE" == "per-gpu" ]]; then
            warn "Per-GPU load exited $load_exit after ${load_dur}s -- retrying as one process for all GPUs"
            LOAD_MODE="single-fallback"
            load_exit=0
            t0=$(date +%s)
            timeout $(( BURN_DURATION + 300 )) "$LOAD_BIN" --no-dcgm-validation \
                -t 1004 -d "$BURN_DURATION" >&2 2>&1 || load_exit=$?
            load_dur=$(( $(date +%s) - t0 ))
        fi
        stop_sampler
        log "Load finished after ${load_dur}s (exit $load_exit)"
    else
        # No load applied means nothing was tested.  Never report PASS here --
        # that is exactly the "enumeration is not a test" failure mode.
        overall="FAIL"
        issues=$(printf '%s' "$issues" | jq \
            '. + [{"issue":"No load generator available (need dcgmproftester from DCGM, or BURN_TOOL=gpu-burn with git, nvcc and network) -- no sustained load was applied, so nothing was tested","severity":"critical"}]')
        err "No load generator available -- burn-in did not run"
    fi

    # --- post-load state ------------------------------------------------
    snapshot_counters "$WORK_DIR/post.csv"
    dmesg 2>/dev/null > "$WORK_DIR/dmesg-post.txt" || : > "$WORK_DIR/dmesg-post.txt"

    # Multiset difference, so repeated identical lines are counted properly and
    # a wrapped ring buffer cannot be mistaken for new output.
    awk 'NR==FNR{seen[$0]++; next} !(seen[$0] && seen[$0]--)' \
        "$WORK_DIR/dmesg-pre.txt" "$WORK_DIR/dmesg-post.txt" > "$WORK_DIR/dmesg-window.txt" || :

    local xid_all xid_crit xid_other
    xid_all=$(grep -iE 'NVRM: *Xid' "$WORK_DIR/dmesg-window.txt" 2>/dev/null || true)
    if [[ -n "$xid_all" ]]; then
        err "--- Xid events during burn-in window ---"
        printf '%s\n' "$xid_all" >&2
        err "--- end Xid events ---"
    fi
    # The Xid number follows the parenthesised PCI address:
    #   "NVRM: Xid (PCI:0000:1b:00): 79, pid=..."
    # Anchoring on the first digit run after "Xid" instead captures 0000 out of
    # the PCI address, which silently misclassifies every event.
    local xid_nums crit_list other_list n
    xid_nums=$(printf '%s\n' "$xid_all" | sed -nE '
            s/.*Xid[[:space:]]*\([^)]*\)[[:space:]]*:[[:space:]]*([0-9]+).*/\1/p
            s/.*Xid[[:space:]]+([0-9]+).*/\1/p
        ' | sort -n | uniq || true)
    crit_list="" other_list=""
    for n in $xid_nums; do
        if [[ " $XID_CRITICAL " == *" $n "* ]]; then crit_list+="${crit_list:+,}$n"
        else other_list+="${other_list:+,}$n"; fi
    done
    xid_crit=$(printf '[%s]' "$crit_list")
    xid_other=$(printf '[%s]' "$other_list")

    if [[ -n "$crit_list" ]]; then
        overall="FAIL"
        issues=$(printf '%s' "$issues" | jq --arg x "$crit_list" \
            '. + [{"issue":"Disqualifying Xid event(s) raised during burn-in: \($x)","severity":"critical"}]')
    fi
    if [[ -n "$other_list" ]]; then
        [[ "$overall" == "PASS" ]] && overall="WARN"
        issues=$(printf '%s' "$issues" | jq --arg x "$other_list" \
            '. + [{"issue":"Non-disqualifying Xid event(s) during burn-in: \($x)","severity":"warning"}]')
    fi

    # --- telemetry + deltas ---------------------------------------------
    local have_throttle=0
    [[ -n "$THROTTLE_FIELDS" ]] && have_throttle=1
    local telemetry
    telemetry=$(aggregate_samples "$WORK_DIR/samples.csv" "$have_throttle")

    local deltas
    deltas=$(awk -F',' '
        function trim(s){gsub(/^[ \t]+|[ \t]+$/,"",s);return s}
        function num(s){s=trim(s); return (s ~ /^-?[0-9.]+$/) ? s+0 : ""}
        NR==FNR { i=num($1); if(i!="") {pce[i]=num($2); puce[i]=num($3); prep[i]=num($4); prc[i]=num($5); pru[i]=num($6)} ; next }
        { i=num($1); if(i=="") next
          printf "%s{\"gpu_index\":%d", (started++?",":"["), i
          printf ",\"ecc_corrected_aggregate_delta\":%s",   (pce[i]!=""&&num($2)!="") ? num($2)-pce[i]  : "null"
          printf ",\"ecc_uncorrected_aggregate_delta\":%s", (puce[i]!=""&&num($3)!="")? num($3)-puce[i] : "null"
          printf ",\"pcie_replay_delta\":%s",               (prep[i]!=""&&num($4)!="")? num($4)-prep[i] : "null"
          printf ",\"remapped_rows_correctable_delta\":%s", (prc[i]!=""&&num($5)!="") ? num($5)-prc[i]  : "null"
          printf ",\"remapped_rows_uncorrectable_delta\":%s",(pru[i]!=""&&num($6)!="")? num($6)-pru[i]  : "null"
          printf "}" }
        END { printf "%s", (started?"]":"[]") }
    ' "$WORK_DIR/pre.csv" "$WORK_DIR/post.csv")
    [[ -z "$deltas" ]] && deltas="[]"

    # Any new uncorrectable error or newly remapped row under load is a real
    # signal -- this is the failure mode that only sustained load exposes.
    local bad_delta
    bad_delta=$(printf '%s' "$deltas" | jq '[.[] | select(
        (.ecc_uncorrected_aggregate_delta // 0) > 0 or
        (.remapped_rows_uncorrectable_delta // 0) > 0
    )] | length')
    if [[ "$bad_delta" -gt 0 ]]; then
        overall="FAIL"
        issues=$(printf '%s' "$issues" | jq --argjson n "$bad_delta" \
            '. + [{"issue":"\($n) GPU(s) gained uncorrectable ECC errors or uncorrectable remapped rows during sustained load","severity":"critical"}]')
    fi

    # --- throttle + clock assessment ------------------------------------
    # sw_power_cap is expected: it is the power limiter doing its job at full
    # load, and gating on it would fail every healthy card.  Only the hardware
    # slowdowns and a sustained clock floor are treated as faults.
    local hw_throttled
    hw_throttled=$(printf '%s' "$telemetry" | jq '[.[] | select(
        (.throttle_samples.hw_thermal_slowdown      // 0) > 0 or
        (.throttle_samples.hw_power_brake_slowdown  // 0) > 0 or
        (.throttle_samples.sw_thermal_slowdown      // 0) > 0
    )] | length')

    if [[ "$BURN_MODE" == "enforce" ]]; then
        if [[ "$hw_throttled" -gt 0 ]]; then
            overall="FAIL"
            issues=$(printf '%s' "$issues" | jq --argjson n "$hw_throttled" \
                '. + [{"issue":"\($n) GPU(s) hit thermal or power-brake slowdown under sustained load","severity":"critical"}]')
        fi
        if [[ -n "$BURN_MIN_SM_CLOCK_MHZ" ]]; then
            local slow
            slow=$(printf '%s' "$telemetry" | jq --argjson f "$BURN_MIN_SM_CLOCK_MHZ" \
                '[.[] | select((.clocks_sm_mhz.mean // 0) < $f)] | length')
            if [[ "$slow" -gt 0 ]]; then
                overall="FAIL"
                issues=$(printf '%s' "$issues" | jq --argjson n "$slow" --argjson f "$BURN_MIN_SM_CLOCK_MHZ" \
                    '. + [{"issue":"\($n) GPU(s) averaged below the \($f) MHz sustained SM clock floor","severity":"critical"}]')
            fi
        else
            issues=$(printf '%s' "$issues" | jq \
                '. + [{"issue":"BURN_MODE=enforce but BURN_MIN_SM_CLOCK_MHZ is unset -- clock floor not enforced. Derive it from a characterization run.","severity":"warning"}]')
            [[ "$overall" == "PASS" ]] && overall="WARN"
        fi
        if [[ -n "$BURN_MIN_POWER_W" ]]; then
            local weak
            weak=$(printf '%s' "$telemetry" | jq --argjson f "$BURN_MIN_POWER_W" \
                '[.[] | select((.power_w.max // 0) < $f)] | length')
            if [[ "$weak" -gt 0 ]]; then
                overall="FAIL"
                issues=$(printf '%s' "$issues" | jq --argjson n "$weak" --argjson f "$BURN_MIN_POWER_W" \
                    '. + [{"issue":"\($n) GPU(s) never reached \($f) W under load -- load may not have engaged","severity":"critical"}]')
            fi
        fi
    else
        # Characterization: report, never gate.
        if [[ "$hw_throttled" -gt 0 ]]; then
            issues=$(printf '%s' "$issues" | jq --argjson n "$hw_throttled" \
                '. + [{"issue":"\($n) GPU(s) recorded thermal or power-brake slowdown (characterization only, not gated)","severity":"info"}]')
        fi
        issues=$(printf '%s' "$issues" | jq \
            '. + [{"issue":"BURN_MODE=characterize -- telemetry recorded, no thresholds enforced","severity":"info"}]')
    fi

    if [[ "$load_exit" -eq 124 ]]; then
        # Distinguish "the load did not run long enough" from "the tool would not
        # stop on its own". The second is a tool quirk, not missing evidence:
        # dcgmproftester can keep looping past its -d and get killed by our
        # ceiling having already delivered the full window.
        if [[ "$load_dur" -ge "$BURN_DURATION" ]]; then
            [[ "$overall" == "PASS" ]] && overall="WARN"
            issues=$(printf '%s' "$issues" | jq \
                --argjson d "$load_dur" --argjson r "$BURN_DURATION" \
                '. + [{"issue":"Load generator ran \($d)s (>= the \($r)s required) but did not exit on its own and was killed at the ceiling -- the load window is complete, the tool did not self-terminate","severity":"warning"}]')
        else
            overall="FAIL"
            issues=$(printf '%s' "$issues" | jq \
                --argjson d "$load_dur" --argjson r "$BURN_DURATION" \
                '. + [{"issue":"Load generator was killed after \($d)s, short of the \($r)s required -- burn-in incomplete","severity":"critical"}]')
        fi
    elif [[ "$load_exit" -ne 0 && "$LOAD_MODE" == "dcgmi-diag" ]]; then
        # dcgmi diag exits non-zero when a test FAILS, so unlike a bare load
        # generator this exit code carries a hardware verdict.
        overall="FAIL"
        issues=$(printf '%s' "$issues" | jq --argjson e "$load_exit" \
            '. + [{"issue":"dcgmi diag targeted_stress exited \($e) -- NVIDIA'"'"'s own sustained-load test reported a failure","severity":"critical"}]')
    elif [[ "$load_exit" -ne 0 && "$tool" != "none" ]]; then
        [[ "$overall" == "PASS" ]] && overall="WARN"
        issues=$(printf '%s' "$issues" | jq --argjson e "$load_exit" \
            '. + [{"issue":"Load generator exited \($e)","severity":"warning"}]')
    fi

    # Per-GPU load coverage.  Aggregate success hides a load generator that only
    # covered some cards: a real run loaded 4 of 8 GPUs for the full window and
    # the other 4 for ~290s, and reported PASS. A card that was not loaded was
    # not tested, so this fails in either mode.
    local required_loaded under_loaded
    required_loaded=$(awk -v d="$BURN_DURATION" -v f="${BURN_COVERAGE_FRAC:-0.9}" \
        'BEGIN{printf "%d", d * f}')
    under_loaded=$(printf '%s' "$telemetry" | jq --argjson r "$required_loaded" \
        '[.[] | select((.loaded_seconds // 0) < $r)] | length')
    if [[ "${under_loaded:-0}" -gt 0 && "$tool" != "none" ]]; then
        overall="FAIL"
        local under_detail
        under_detail=$(printf '%s' "$telemetry" | jq -r --argjson r "$required_loaded" \
            '[.[] | select((.loaded_seconds // 0) < $r)
              | "GPU \(.gpu_index): \(.loaded_seconds // 0)s"] | join(", ")')
        issues=$(printf '%s' "$issues" | jq \
            --argjson n "$under_loaded" --argjson r "$required_loaded" --arg d "$under_detail" \
            '. + [{"issue":"\($n) GPU(s) were not under load for the required \($r)s -- the load generator did not cover them, so they were not tested","severity":"critical","details":$d}]')
        err "Load coverage shortfall: $under_detail (need ${required_loaded}s each)"
    fi

    # gpu-burn's own per-GPU verdict
    local burn_results burn_errors faulty
    burn_results="[]"; burn_errors=0
    if [[ "$LOAD_MODE" == "gpu-burn" ]]; then
        burn_results=$(parse_gpu_burn_results)
        burn_errors=$(gpu_burn_error_total)
        faulty=$(printf '%s' "$burn_results" | jq '[.[] | select(.status == "FAULTY")] | length')
        if [[ "${faulty:-0}" -gt 0 ]]; then
            overall="FAIL"
            local faulty_list
            faulty_list=$(printf '%s' "$burn_results" | jq -r \
                '[.[] | select(.status == "FAULTY") | "GPU \(.gpu_index)"] | join(", ")')
            issues=$(printf '%s' "$issues" | jq \
                --argjson n "$faulty" --arg d "$faulty_list" \
                '. + [{"issue":"gpu-burn reported \($n) GPU(s) FAULTY -- wrong arithmetic under sustained load","severity":"critical","details":$d}]')
            err "gpu-burn FAULTY: $faulty_list"
        fi
        if [[ "${burn_errors:-0}" -gt 0 ]]; then
            overall="FAIL"
            issues=$(printf '%s' "$issues" | jq --argjson e "$burn_errors" \
                '. + [{"issue":"gpu-burn counted \($e) computation error(s) under load","severity":"critical"}]')
        fi
        # A run that produced no per-GPU summary did not complete its check.
        local reported
        reported=$(printf '%s' "$burn_results" | jq 'length')
        if [[ "${reported:-0}" -eq 0 ]]; then
            [[ "$overall" == "PASS" ]] && overall="WARN"
            issues=$(printf '%s' "$issues" | jq \
                '. + [{"issue":"gpu-burn produced no per-GPU OK/FAULTY summary -- its own verdict is unavailable","severity":"warning"}]')
        elif [[ "${reported:-0}" -lt "${SMI_GPU_COUNT:-0}" ]]; then
            overall="FAIL"
            issues=$(printf '%s' "$issues" | jq \
                --argjson r "$reported" --argjson g "${SMI_GPU_COUNT:-0}" \
                '. + [{"issue":"gpu-burn reported on \($r) of \($g) GPU(s) -- the rest were not tested","severity":"critical"}]')
        fi
    fi

    local sample_total
    sample_total=$(printf '%s' "$telemetry" | jq '[.[].samples] | add // 0')
    if [[ "$tool" != "none" && "$sample_total" -eq 0 ]]; then
        overall="FAIL"
        issues=$(printf '%s' "$issues" | jq \
            '. + [{"issue":"No telemetry samples captured during the load window -- the run cannot be evidenced","severity":"critical"}]')
    fi

    # Last phase in the pipeline, so this is the closing state of every card.
    post_test_counter_check
    if [[ "${POST_NEW_FAULTS:-0}" -gt 0 ]]; then
        overall="FAIL"
        local pf_detail
        pf_detail=$(printf '%s' "$POST_COUNTERS" | jq -r '[.deltas[]
            | select(((.ecc_uncorrected_delta // 0) > 0)
                     or ((.remapped_rows_uncorrectable_delta // 0) > 0)
                     or (.remapped_rows_failure_now == true)
                     or (.remapped_rows_pending_now == true))
            | "GPU \(.gpu_index)"] | join(", ")')
        issues=$(printf '%s' "$issues" | jq --argjson n "${POST_NEW_FAULTS}" --arg d "$pf_detail" \
            '. + [{"issue":"\($n) GPU(s) gained uncorrectable ECC errors, uncorrectable remapped rows, a pending remap or a remap failure across the whole test sequence","severity":"critical","details":$d}]')
        err "New memory faults since the pre-load baseline: $pf_detail"
    fi
    if [[ "${POST_COUNTERS_VOID:-0}" -eq 1 ]]; then
        [[ "$overall" == "PASS" ]] && overall="WARN"
        issues=$(printf '%s' "$issues" | jq \
            '. + [{"issue":"Error counters went backwards since the pre-load baseline -- something cleared them mid-sequence, so the comparison cannot evidence memory health","severity":"warning"}]')
    fi

    local xid_b64 diag_b64="" burn_b64=""
    xid_b64=$(grep -iE 'NVRM|Xid' "$WORK_DIR/dmesg-window.txt" 2>/dev/null | base64 2>/dev/null | tr -d '\n' || true)
    [[ -s "$WORK_DIR/diag.json" ]] && \
        diag_b64=$(base64 < "$WORK_DIR/diag.json" 2>/dev/null | tr -d '\n' || true)
    [[ -s "$WORK_DIR/gpu-burn.out" ]] && \
        burn_b64=$(base64 < "$WORK_DIR/gpu-burn.out" 2>/dev/null | tr -d '\n' || true)

    jq -n \
        --arg ver "$SCRIPT_VERSION" --arg name "gpu-burn-in" \
        --arg ts "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" \
        --argjson dur "$(( $(date +%s) - SCRIPT_START ))" \
        --arg verdict "$overall" --argjson issues "$issues" \
        --arg drv "$SMI_DRIVER" --arg cuda "$SMI_CUDA" \
        --argjson gpus "${SMI_GPU_COUNT:-0}" \
        --arg tool "$tool" --arg mode "$BURN_MODE" \
        --arg loadmode "${LOAD_MODE:-none}" \
        --arg diagraw "$diag_b64" \
        --arg burnraw "$burn_b64" \
        --argjson burnres "${burn_results:-[]}" \
        --argjson burnerrs "${burn_errors:-0}" \
        --argjson counters "${POST_COUNTERS:-null}" \
        --argjson requested "$BURN_DURATION" --argjson actual "$load_dur" \
        --argjson load_exit "$load_exit" \
        --argjson telemetry "$telemetry" --argjson deltas "$deltas" \
        --argjson xidcrit "$xid_crit" --argjson xidother "$xid_other" \
        --arg xidraw "$xid_b64" \
        --arg tbase "${THROTTLE_BASE:-unavailable}" \
        '{
            report_metadata:{script_version:$ver, script_name:$name, generated_at:$ts, duration_seconds:$dur},
            verdict:{overall:$verdict, issues:$issues},
            system:{nvidia_driver_version:$drv, cuda_version:$cuda, gpu_count:$gpus},
            burn_in:{
                tool:$tool, mode:$mode, load_mode:$loadmode,
                duration_requested_seconds:$requested,
                duration_actual_seconds:$actual,
                load_exit_code:$load_exit,
                throttle_field_base:$tbase,
                telemetry:$telemetry,
                counter_deltas:$deltas,
                xid:{critical:$xidcrit, other:$xidother, dmesg_window_b64:$xidraw},
                dcgmi_diag_b64:$diagraw,
                gpu_burn_results:$burnres,
                gpu_burn_error_count:$burnerrs,
                gpu_burn_output_b64:$burnraw,
                counters_since_baseline:$counters
            }
        }'

    log "=========================================="
    log "Burn-in complete -- Verdict: $overall"
    log "=========================================="
    [[ "$overall" == "FAIL" ]] && exit 1
    exit 0
}

main "$@"
