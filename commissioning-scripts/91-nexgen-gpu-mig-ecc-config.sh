#!/bin/bash
# --- Start MAAS Metadata ---
# name: 91-nexgen-gpu-mig-ecc-config
# title: NexGen GPU MIG Disable & ECC Enable
# description: Disables MIG mode and enables ECC on all NVIDIA GPUs.
#   Both changes require a reboot to take effect. On first run, if changes
#   are needed, the script configures GPUs and reboots. On second run
#   (after reboot), it verifies the config is correct and exits cleanly.
#   Requires 90-nexgen-gpu-install to run first.
# script_type: commissioning
# hardware_type: gpu
# timeout: 00:05:00
# destructive: false
# may_reboot: true
# --- End MAAS Metadata ---

set -o pipefail
trap 'warn "Command failed at line $LINENO (exit code $?)"' ERR

###############################################################################
# CONFIG
###############################################################################
WORK_DIR="/tmp/gpu-mig-ecc-$$"
SCRIPT_VERSION="1.0.0"

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
# CONFIGURE GPUS (first run)
###############################################################################
configure_gpus() {
    log "=== Configuring GPUs (first run) ==="

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
# VERIFY GPUS (post-reboot)
###############################################################################
verify_gpus() {
    log "=== Post-reboot verification (HAS_STARTED=True) ==="

    local gpu_configs="[]"
    local overall="PASS"
    local issues="[]"

    local gpu
    for (( gpu=0; gpu<SMI_GPU_COUNT; gpu++ )); do
        local mig_now ecc_now

        mig_now=$(query_gpu_field "$gpu" "mig.mode.current")
        ecc_now=$(query_gpu_field "$gpu" "ecc.mode.current")
        log "  GPU $gpu: MIG=$mig_now, ECC=$ecc_now"

        # Warn if MIG is still enabled after reboot
        if [[ "$mig_now" == "Enabled" ]]; then
            warn "  GPU $gpu: MIG still enabled after reboot"
            [[ "$overall" == "PASS" ]] && overall="WARN"
            issues=$(echo "$issues" | jq --argjson g "$gpu" \
                '. + [{"issue":"GPU \($g): MIG still enabled after reboot","severity":"warning"}]')
        fi

        # Warn if ECC is still disabled after reboot
        if [[ "$ecc_now" == "Disabled" ]]; then
            warn "  GPU $gpu: ECC still disabled after reboot"
            [[ "$overall" == "PASS" ]] && overall="WARN"
            issues=$(echo "$issues" | jq --argjson g "$gpu" \
                '. + [{"issue":"GPU \($g): ECC still disabled after reboot","severity":"warning"}]')
        fi

        gpu_configs=$(echo "$gpu_configs" | jq \
            --argjson idx "$gpu" \
            --arg mig "$mig_now" --arg ecc "$ecc_now" \
            '. + [{gpu_index:$idx, mig_status:$mig, ecc_status:$ecc}]')
    done

    echo "$gpu_configs" > "$WORK_DIR/gpu_configs.json"
    echo "false" > "$WORK_DIR/needs_reboot.txt"
    echo "$overall" > "$WORK_DIR/overall.txt"
    echo "$issues" > "$WORK_DIR/issues.json"
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

    if [[ "${HAS_STARTED:-}" == "True" ]]; then
        # Post-reboot: verify changes took effect
        verify_gpus
    else
        # First run: configure GPUs
        configure_gpus
    fi

    local overall
    overall=$(cat "$WORK_DIR/overall.txt" 2>/dev/null || echo "PASS")
    local needs_reboot
    needs_reboot=$(cat "$WORK_DIR/needs_reboot.txt" 2>/dev/null || echo "false")

    output_report

    # If changes were made, reboot to apply
    if [[ "$needs_reboot" == "true" ]]; then
        log "=========================================="
        log "Changes require reboot -- rebooting now..."
        log "=========================================="
        rm -rf "$WORK_DIR"
        sudo reboot
        sleep 120  # reboot will kill this process
    fi

    rm -rf "$WORK_DIR"

    log "=========================================="
    log "MIG/ECC config complete. Total time: $(( $(date +%s) - SCRIPT_START ))s"
    log "=========================================="

    [[ "$overall" == "FAIL" ]] && exit 1
    exit 0
}

main "$@"
