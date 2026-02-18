#!/bin/bash
# --- Start MAAS Metadata ---
# name: 91-nexgen-gpu-mig-ecc-config
# title: NexGen GPU MIG Disable & ECC Enable
# description: Disables MIG mode and enables ECC on all NVIDIA GPUs.
#   Changes are written to GPU NVRAM, then activated via nvidia-smi --gpu-reset
#   (no full reboot needed). Falls back to "pending reboot" if reset fails.
#   Requires 90-nexgen-gpu-install to run first.
# script_type: commissioning
# hardware_type: gpu
# timeout: 00:05:00
# destructive: false
# may_reboot: false
# --- End MAAS Metadata ---

set -o pipefail
trap 'warn "Command failed at line $LINENO (exit code $?)"' ERR

###############################################################################
# CONFIG
###############################################################################
WORK_DIR="/tmp/gpu-mig-ecc-$$"
SCRIPT_VERSION="1.1.0"

###############################################################################
# LOGGING
###############################################################################
log()  { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [INFO]  $*" >&2; }
warn() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [WARN]  $*" >&2; }
err()  { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [ERROR] $*" >&2; }

mkdir -p "$WORK_DIR"

###############################################################################
# HELPER: Parse driver + CUDA from nvidia-smi header
###############################################################################
get_smi_header_info() {
    local header
    header=$(nvidia-smi 2>/dev/null | head -5)
    SMI_DRIVER=$(echo "$header" | grep -oP 'Driver Version:\s*\K[0-9.]+' || echo "unknown")
    SMI_CUDA=$(echo "$header" | grep -oP 'CUDA Version:\s*\K[0-9.]+' || echo "unknown")
    SMI_GPU_COUNT=$(nvidia-smi --query-gpu=count --format=csv,noheader,nounits 2>/dev/null | head -1 || echo "0")
}

###############################################################################
# FAIL with JSON output
###############################################################################
fail_json() {
    local msg="$1"
    err "$msg"
    jq -n \
        --arg v "$SCRIPT_VERSION" --arg m "$msg" \
        --arg drv "${SMI_DRIVER:-unknown}" --arg cuda "${SMI_CUDA:-unknown}" \
        --argjson gpus "${SMI_GPU_COUNT:-0}" \
        '{
            report_metadata:{script_version:$v, script_name:"gpu-mig-ecc-config"},
            verdict:{overall:"FAIL", issues:[{"issue":$m,"severity":"critical"}]},
            system:{nvidia_driver_version:$drv, cuda_version:$cuda, gpu_count:$gpus},
            gpu_config:[], reboot_triggered:false
        }'
    exit 1
}

###############################################################################
# PREFLIGHT
###############################################################################
preflight() {
    log "=== Preflight checks ==="

    local missing=""
    command -v nvidia-smi &>/dev/null || missing+=" nvidia-smi"
    command -v jq         &>/dev/null || missing+=" jq"

    [[ -n "$missing" ]] && fail_json "Missing required tools:$missing -- run 90-nexgen-gpu-install first"

    if ! nvidia-smi &>/dev/null; then
        fail_json "nvidia-smi not functional -- run 90-nexgen-gpu-install first"
    fi

    get_smi_header_info
    log "nvidia-smi: $SMI_GPU_COUNT GPU(s), driver $SMI_DRIVER, CUDA $SMI_CUDA"
}

###############################################################################
# QUERY GPU MIG/ECC STATUS
# Returns trimmed value: Enabled, Disabled, [N/A], N/A, etc.
###############################################################################
query_gpu_field() {
    local gpu_idx="$1" field="$2"
    nvidia-smi -i "$gpu_idx" --query-gpu="$field" --format=csv,noheader 2>/dev/null | xargs
}

###############################################################################
# CONFIGURE GPUS -- write NVRAM changes
###############################################################################
configure_gpus() {
    log "=== Configuring GPU NVRAM ==="

    local needs_reboot="false"
    local gpu_configs="[]"
    local overall="PASS"
    local issues="[]"

    local gpu
    for (( gpu=0; gpu<SMI_GPU_COUNT; gpu++ )); do
        local mig_before ecc_before mig_changed="false" ecc_changed="false"

        # --- MIG ---
        mig_before=$(query_gpu_field "$gpu" "mig.mode.current")
        log "  GPU $gpu: MIG=$mig_before"

        if [[ "$mig_before" == "Enabled" ]]; then
            log "  GPU $gpu: Disabling MIG mode..."
            # Destroy any existing MIG instances (may fail if none, that's OK)
            nvidia-smi mig -dci -i "$gpu" 2>/dev/null || true
            nvidia-smi mig -dgi -i "$gpu" 2>/dev/null || true

            if nvidia-smi -i "$gpu" -mig 0 2>&1; then
                log "  GPU $gpu: MIG disabled (pending reboot)"
                mig_changed="true"
                needs_reboot="true"
            else
                err "  GPU $gpu: Failed to disable MIG"
                overall="FAIL"
                issues=$(echo "$issues" | jq --argjson g "$gpu" \
                    '. + [{"issue":"GPU \($g): failed to disable MIG","severity":"critical"}]')
            fi
        elif [[ "$mig_before" == *"N/A"* ]]; then
            log "  GPU $gpu: MIG not supported (skipping)"
        else
            log "  GPU $gpu: MIG already disabled"
        fi

        # --- ECC ---
        ecc_before=$(query_gpu_field "$gpu" "ecc.mode.current")
        log "  GPU $gpu: ECC=$ecc_before"

        if [[ "$ecc_before" == "Disabled" ]]; then
            log "  GPU $gpu: Enabling ECC..."
            if nvidia-smi -i "$gpu" -e 1 2>&1; then
                log "  GPU $gpu: ECC enabled (pending reboot)"
                ecc_changed="true"
                needs_reboot="true"
            else
                err "  GPU $gpu: Failed to enable ECC"
                overall="FAIL"
                issues=$(echo "$issues" | jq --argjson g "$gpu" \
                    '. + [{"issue":"GPU \($g): failed to enable ECC","severity":"critical"}]')
            fi
        elif [[ "$ecc_before" == *"N/A"* ]]; then
            log "  GPU $gpu: ECC not supported (skipping)"
        else
            log "  GPU $gpu: ECC already enabled"
        fi

        # Record per-GPU config
        gpu_configs=$(echo "$gpu_configs" | jq \
            --argjson idx "$gpu" \
            --arg mb "$mig_before" --argjson mc "$mig_changed" \
            --arg eb "$ecc_before" --argjson ec "$ecc_changed" \
            '. + [{gpu_index:$idx, mig_before:$mb, mig_changed:$mc, ecc_before:$eb, ecc_changed:$ec}]')
    done

    # Save state for report
    echo "$gpu_configs" > "$WORK_DIR/gpu_configs.json"
    echo "$needs_reboot" > "$WORK_DIR/needs_reboot.txt"
    echo "$overall" > "$WORK_DIR/overall.txt"
    echo "$issues" > "$WORK_DIR/issues.json"
}

###############################################################################
# RESET GPUs & VERIFY -- activate NVRAM changes without full reboot
###############################################################################
reset_and_verify() {
    log "=== GPU reset to activate NVRAM changes ==="

    local gpu_configs overall issues
    gpu_configs=$(cat "$WORK_DIR/gpu_configs.json" 2>/dev/null || echo "[]")
    overall=$(cat "$WORK_DIR/overall.txt" 2>/dev/null || echo "PASS")
    issues=$(cat "$WORK_DIR/issues.json" 2>/dev/null || echo "[]")

    # Collect GPU indices that had changes
    local gpus_to_reset
    gpus_to_reset=$(echo "$gpu_configs" | jq -r \
        '.[] | select(.mig_changed==true or .ecc_changed==true) | .gpu_index')

    if [[ -z "$gpus_to_reset" ]]; then
        log "No GPUs need reset"
        return
    fi

    # Reset each changed GPU
    local gpu reset_failed="false"
    for gpu in $gpus_to_reset; do
        log "  Resetting GPU $gpu..."
        if nvidia-smi --gpu-reset -i "$gpu" >&2 2>&1; then
            log "  GPU $gpu: reset OK"
        else
            warn "  GPU $gpu: reset failed"
            reset_failed="true"
        fi
    done

    # Wait for GPUs to recover
    log "  Waiting for GPUs to recover..."
    local attempt
    for attempt in 1 2 3 4 5; do
        sleep 3
        if nvidia-smi &>/dev/null; then
            log "  GPUs available after $((attempt * 3))s"
            break
        fi
        [[ "$attempt" -eq 5 ]] && warn "  nvidia-smi still not responding after 15s"
    done

    # Verify each GPU that had changes
    local still_pending="false"
    for gpu in $gpus_to_reset; do
        local mig_changed ecc_changed
        mig_changed=$(echo "$gpu_configs" | jq -r ".[] | select(.gpu_index==$gpu) | .mig_changed")
        ecc_changed=$(echo "$gpu_configs" | jq -r ".[] | select(.gpu_index==$gpu) | .ecc_changed")

        if [[ "$mig_changed" == "true" ]]; then
            local mig_now
            mig_now=$(query_gpu_field "$gpu" "mig.mode.current")
            if [[ "$mig_now" == "Disabled" ]]; then
                log "  GPU $gpu: MIG confirmed Disabled"
            else
                warn "  GPU $gpu: MIG still $mig_now after reset (needs full reboot)"
                still_pending="true"
                issues=$(echo "$issues" | jq --argjson g "$gpu" --arg s "$mig_now" \
                    '. + [{"issue":"GPU \($g): MIG still \($s) after reset, pending full reboot","severity":"warning"}]')
            fi
        fi

        if [[ "$ecc_changed" == "true" ]]; then
            local ecc_now
            ecc_now=$(query_gpu_field "$gpu" "ecc.mode.current")
            if [[ "$ecc_now" == "Enabled" ]]; then
                log "  GPU $gpu: ECC confirmed Enabled"
            else
                warn "  GPU $gpu: ECC still $ecc_now after reset (needs full reboot)"
                still_pending="true"
                issues=$(echo "$issues" | jq --argjson g "$gpu" --arg s "$ecc_now" \
                    '. + [{"issue":"GPU \($g): ECC still \($s) after reset, pending full reboot","severity":"warning"}]')
            fi
        fi
    done

    if [[ "$still_pending" == "true" || "$reset_failed" == "true" ]]; then
        [[ "$overall" == "PASS" ]] && overall="WARN"
        if [[ "$still_pending" == "true" ]]; then
            issues=$(echo "$issues" | jq \
                '. + [{"issue":"Some NVRAM changes did not activate via GPU reset, will take effect on deployment reboot","severity":"info"}]')
        fi
    fi

    echo "$overall" > "$WORK_DIR/overall.txt"
    echo "$issues" > "$WORK_DIR/issues.json"
    # Changes activated (or noted as pending) -- no reboot needed
    echo "false" > "$WORK_DIR/needs_reboot.txt"
}

###############################################################################
# OUTPUT REPORT
###############################################################################
output_report() {
    local test_end dur overall issues needs_reboot
    test_end=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
    dur=$(( $(date +%s) - SCRIPT_START ))
    overall=$(cat "$WORK_DIR/overall.txt" 2>/dev/null || echo "PASS")
    issues=$(cat "$WORK_DIR/issues.json" 2>/dev/null || echo "[]")
    needs_reboot=$(cat "$WORK_DIR/needs_reboot.txt" 2>/dev/null || echo "false")

    local gpu_configs
    gpu_configs=$(cat "$WORK_DIR/gpu_configs.json" 2>/dev/null || echo "[]")

    jq -n \
        --arg ver "$SCRIPT_VERSION" --arg name "gpu-mig-ecc-config" \
        --arg ts "$test_end" --argjson dur "$dur" \
        --arg verdict "$overall" --argjson issues "$issues" \
        --arg drv "$SMI_DRIVER" --arg cuda "$SMI_CUDA" \
        --argjson gpus "$SMI_GPU_COUNT" \
        --argjson gpu_config "$gpu_configs" \
        --argjson reboot "$needs_reboot" \
        '{
            report_metadata:{
                script_version:$ver, script_name:$name,
                generated_at:$ts, duration_seconds:$dur
            },
            verdict:{overall:$verdict, issues:$issues},
            system:{nvidia_driver_version:$drv, cuda_version:$cuda, gpu_count:$gpus},
            gpu_config:$gpu_config,
            reboot_triggered:$reboot
        }'

    log "=== MIG/ECC CONFIG COMPLETE -- Verdict: $overall ==="
}

###############################################################################
# MAIN
###############################################################################
main() {
    SCRIPT_START=$(date +%s)

    log "=========================================="
    log "NexGen GPU MIG/ECC Config v${SCRIPT_VERSION}"
    log "=========================================="

    preflight
    configure_gpus

    local needs_reboot
    needs_reboot=$(cat "$WORK_DIR/needs_reboot.txt" 2>/dev/null || echo "false")

    # If NVRAM changes were made, activate them via GPU reset (no full reboot).
    # This keeps the MAAS ephemeral environment intact so scripts 98/99 can run.
    if [[ "$needs_reboot" == "true" ]]; then
        reset_and_verify
    fi

    local overall
    overall=$(cat "$WORK_DIR/overall.txt" 2>/dev/null || echo "PASS")

    output_report

    rm -rf "$WORK_DIR"

    log "=========================================="
    log "MIG/ECC config complete. Total time: $(( $(date +%s) - SCRIPT_START ))s"
    log "=========================================="

    [[ "$overall" == "FAIL" ]] && exit 1
    exit 0
}

main "$@"
