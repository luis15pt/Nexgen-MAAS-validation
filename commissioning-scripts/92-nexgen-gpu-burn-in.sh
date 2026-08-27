#!/bin/bash
# --- Start MAAS Metadata ---
# name: 92-nexgen-gpu-burn-in
# title: NexGen GPU Sustained Burn-In (optional)
# description: Applies a sustained full-power load and records what the GPUs
#   actually did under it: power, temperature, SM clock, throttle reasons, ECC
#   and remapped-row deltas, PCIe replay deltas, and any Xid events raised
#   during the window.  A DCGM diagnostic is a diagnostic, not a burn-in --
#   reballed and reflowed cards pass cold and fail hot, so only sustained load
#   exposes them.  Optional: simply do not upload this script to skip it.
#   Requires 90-nexgen-gpu-install to have run first.
#   Override: BURN_DURATION=1800 BURN_MODE=characterize|enforce BURN_TOOL=auto|dcgmproftester|gpu-burn
# script_type: commissioning
# parallel: disabled
# hardware_type: gpu
# timeout: 01:30:00
# destructive: false
# may_reboot: false
# --- End MAAS Metadata ---

set -o pipefail
trap 'warn "Command failed at line $LINENO (exit code $?)"' ERR

###############################################################################
# CONFIG
###############################################################################
BURN_DURATION="${BURN_DURATION:-1800}"     # seconds of sustained load
BURN_SAMPLE_INTERVAL="${BURN_SAMPLE_INTERVAL:-10}"
BURN_TOOL="${BURN_TOOL:-auto}"             # auto | dcgmproftester | gpu-burn
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
SCRIPT_VERSION="1.0.0"

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
# Absolute counters are not the interesting quantity: a replay count can carry
# boot-time link-training events that are not defects.  What matters is the
# delta across the load window, so each counter is snapshotted either side.
snapshot_counters() {
    local out="$1" fields
    fields="index,ecc.errors.corrected.aggregate.total,ecc.errors.uncorrected.aggregate.total"
    fields+=",pcie.replay_counter,remapped_rows.correctable,remapped_rows.uncorrectable"
    if ! nvidia-smi --query-gpu="$fields" --format=csv,noheader,nounits > "$out" 2>/dev/null; then
        # One unsupported field would otherwise cost us the whole snapshot
        warn "Full counter snapshot failed -- retrying with ECC and replay only"
        nvidia-smi --query-gpu="index,ecc.errors.corrected.aggregate.total,ecc.errors.uncorrected.aggregate.total,pcie.replay_counter" \
            --format=csv,noheader,nounits > "$out" 2>/dev/null || : > "$out"
    fi
}

###############################################################################
# LOAD TOOL SELECTION
###############################################################################
# dcgmproftester ships with DCGM, which script 90 already installs, so it needs
# no compilation and no network.  gpu-burn is offered as a genuinely
# independent load source but has to be built from source on the node, which
# needs outbound network and adds a failure mode -- hence not the default.
select_load_tool() {
    LOAD_TOOL="" LOAD_CMD=""
    local cand

    if [[ "$BURN_TOOL" == "auto" || "$BURN_TOOL" == "dcgmproftester" ]]; then
        for cand in dcgmproftester13 dcgmproftester12 dcgmproftester11 dcgmproftester; do
            if command -v "$cand" &>/dev/null; then
                LOAD_TOOL="$cand"
                # -t 1004 drives the FP32 pipe, which is what pulls a card to
                # its power limit.  --no-dcgm-validation keeps this a pure load
                # generator: pass/fail is decided from our own telemetry, not
                # from dcgmproftester's thresholds.
                LOAD_CMD="$cand --no-dcgm-validation -t 1004 -d $BURN_DURATION"
                log "Load tool: $cand (no build required)"
                return 0
            fi
        done
        [[ "$BURN_TOOL" == "dcgmproftester" ]] && \
            { warn "dcgmproftester requested but not found"; return 1; }
    fi

    if [[ "$BURN_TOOL" == "auto" || "$BURN_TOOL" == "gpu-burn" ]]; then
        if command -v gpu_burn &>/dev/null; then
            LOAD_TOOL="gpu-burn"; LOAD_CMD="gpu_burn $BURN_DURATION"
            log "Load tool: gpu-burn (preinstalled)"
            return 0
        fi
        if [[ "$BURN_TOOL" == "gpu-burn" ]]; then
            log "Building gpu-burn from source..."
            if ! command -v nvcc &>/dev/null; then
                warn "nvcc not on PATH -- cannot build gpu-burn"
                [[ -x /usr/local/cuda/bin/nvcc ]] && export PATH="/usr/local/cuda/bin:$PATH"
            fi
            if command -v git &>/dev/null && command -v nvcc &>/dev/null \
               && git clone --depth 1 "$GPU_BURN_REPO" "$WORK_DIR/gpu-burn" >&2 2>&1 \
               && make -C "$WORK_DIR/gpu-burn" >&2 2>&1 \
               && [[ -x "$WORK_DIR/gpu-burn/gpu_burn" ]]; then
                LOAD_TOOL="gpu-burn"
                LOAD_CMD="$WORK_DIR/gpu-burn/gpu_burn $BURN_DURATION"
                log "Load tool: gpu-burn (built from source)"
                return 0
            fi
            warn "gpu-burn build failed (needs git, nvcc and outbound network)"
        fi
    fi
    return 1
}

###############################################################################
# SAMPLER
###############################################################################
start_sampler() {
    local out="$1"
    (
        while :; do
            nvidia-smi --query-gpu="index,power.draw,temperature.gpu,temperature.memory,clocks.sm${THROTTLE_FIELDS}" \
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

# Aggregate samples per GPU into min/max/mean plus throttle-reason counts.
# Emits a JSON array.
aggregate_samples() {
    local csv="$1" have_throttle="$2"
    [[ ! -s "$csv" ]] && { echo "[]"; return; }
    awk -F',' -v ht="$have_throttle" '
        function trim(s) { gsub(/^[ \t]+|[ \t]+$/, "", s); return s }
        {
            gi = trim($2) + 0
            p  = trim($3); t = trim($4); tm = trim($5); c = trim($6)
            n[gi]++
            if (p  ~ /^[0-9.]+$/) { ps[gi]+=p; if (pmin[gi]==""||p<pmin[gi]) pmin[gi]=p; if (p>pmax[gi]) pmax[gi]=p }
            if (t  ~ /^[0-9.]+$/) { ts[gi]+=t; if (tmin[gi]==""||t<tmin[gi]) tmin[gi]=t; if (t>tmax[gi]) tmax[gi]=t }
            if (tm ~ /^[0-9.]+$/) { ms[gi]+=tm; if (mmax[gi]==""||tm>mmax[gi]) mmax[gi]=tm }
            if (c  ~ /^[0-9.]+$/) { cs[gi]+=c; if (cmin[gi]==""||c<cmin[gi]) cmin[gi]=c; if (c>cmax[gi]) cmax[gi]=c }
            if (ht == "1") {
                if (trim($7)  ~ /Active/ && trim($7)  !~ /Not/) swpc[gi]++
                if (trim($8)  ~ /Active/ && trim($8)  !~ /Not/) hwth[gi]++
                if (trim($9)  ~ /Active/ && trim($9)  !~ /Not/) hwpb[gi]++
                if (trim($10) ~ /Active/ && trim($10) !~ /Not/) swth[gi]++
            }
            if (gi > maxi) maxi = gi
        }
        END {
            printf "["
            first = 1
            for (i = 0; i <= maxi; i++) {
                if (!(i in n)) continue
                if (!first) printf ","
                first = 0
                printf "{\"gpu_index\":%d,\"samples\":%d", i, n[i]
                printf ",\"power_w\":{\"min\":%.2f,\"max\":%.2f,\"mean\":%.2f}", pmin[i], pmax[i], (n[i]?ps[i]/n[i]:0)
                printf ",\"temp_gpu_c\":{\"min\":%.1f,\"max\":%.1f,\"mean\":%.1f}", tmin[i], tmax[i], (n[i]?ts[i]/n[i]:0)
                printf ",\"temp_memory_c\":{\"max\":%.1f,\"mean\":%.1f}", mmax[i], (n[i]?ms[i]/n[i]:0)
                printf ",\"clocks_sm_mhz\":{\"min\":%.0f,\"max\":%.0f,\"mean\":%.0f}", cmin[i], cmax[i], (n[i]?cs[i]/n[i]:0)
                if (ht == "1")
                    printf ",\"throttle_samples\":{\"sw_power_cap\":%d,\"hw_thermal_slowdown\":%d,\"hw_power_brake_slowdown\":%d,\"sw_thermal_slowdown\":%d}", swpc[i]+0, hwth[i]+0, hwpb[i]+0, swth[i]+0
                else
                    printf ",\"throttle_samples\":null"
                printf "}"
            }
            printf "]"
        }
    ' "$csv"
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

    local issues="[]" overall="PASS"

    # --- pre-load state -------------------------------------------------
    snapshot_counters "$WORK_DIR/pre.csv"
    dmesg 2>/dev/null > "$WORK_DIR/dmesg-pre.txt" || : > "$WORK_DIR/dmesg-pre.txt"

    # --- load -----------------------------------------------------------
    local tool="none" load_exit=0 load_dur=0
    if select_load_tool; then
        tool="$LOAD_TOOL"
        log "Applying sustained load for ${BURN_DURATION}s: $LOAD_CMD"
        start_sampler "$WORK_DIR/samples.csv"
        local t0
        t0=$(date +%s)
        # Hard ceiling well past BURN_DURATION: a wedged load generator must not
        # be left to be killed by MAAS, which would emit no report at all.
        timeout $(( BURN_DURATION + 300 )) $LOAD_CMD >&2 2>&1 || load_exit=$?
        load_dur=$(( $(date +%s) - t0 ))
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
        overall="FAIL"
        issues=$(printf '%s' "$issues" | jq \
            '. + [{"issue":"Load generator exceeded its timeout and was killed -- burn-in incomplete","severity":"critical"}]')
    elif [[ "$load_exit" -ne 0 && "$tool" != "none" ]]; then
        [[ "$overall" == "PASS" ]] && overall="WARN"
        issues=$(printf '%s' "$issues" | jq --argjson e "$load_exit" \
            '. + [{"issue":"Load generator exited \($e)","severity":"warning"}]')
    fi

    local sample_total
    sample_total=$(printf '%s' "$telemetry" | jq '[.[].samples] | add // 0')
    if [[ "$tool" != "none" && "$sample_total" -eq 0 ]]; then
        overall="FAIL"
        issues=$(printf '%s' "$issues" | jq \
            '. + [{"issue":"No telemetry samples captured during the load window -- the run cannot be evidenced","severity":"critical"}]')
    fi

    local xid_b64
    xid_b64=$(grep -iE 'NVRM|Xid' "$WORK_DIR/dmesg-window.txt" 2>/dev/null | base64 2>/dev/null | tr -d '\n' || true)

    jq -n \
        --arg ver "$SCRIPT_VERSION" --arg name "gpu-burn-in" \
        --arg ts "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" \
        --argjson dur "$(( $(date +%s) - SCRIPT_START ))" \
        --arg verdict "$overall" --argjson issues "$issues" \
        --arg drv "$SMI_DRIVER" --arg cuda "$SMI_CUDA" \
        --argjson gpus "${SMI_GPU_COUNT:-0}" \
        --arg tool "$tool" --arg mode "$BURN_MODE" \
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
                tool:$tool, mode:$mode,
                duration_requested_seconds:$requested,
                duration_actual_seconds:$actual,
                load_exit_code:$load_exit,
                throttle_field_base:$tbase,
                telemetry:$telemetry,
                counter_deltas:$deltas,
                xid:{critical:$xidcrit, other:$xidother, dmesg_window_b64:$xidraw}
            }
        }'

    log "=========================================="
    log "Burn-in complete -- Verdict: $overall"
    log "=========================================="
    [[ "$overall" == "FAIL" ]] && exit 1
    exit 0
}

main "$@"
