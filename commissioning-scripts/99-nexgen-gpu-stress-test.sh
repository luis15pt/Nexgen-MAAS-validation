#!/bin/bash
# --- Start MAAS Metadata ---
# name: 99-nexgen-gpu-stress-test
# title: NexGen GPU Stress Test (DCGM Diagnostics)
# description: Runs DCGM diagnostics at configurable levels (1-4).
#   Requires 90-nexgen-gpu-install to have installed DCGM 4.x.
#   Level 1: ~1 min quick check. Level 4: ~90 min full validation.
#   Override level: DCGM_DIAG_LEVEL=4
# script_type: commissioning
# hardware_type: gpu
# timeout: 02:00:00
# destructive: false
# may_reboot: false
# --- End MAAS Metadata ---

set -o pipefail
trap 'warn "Command failed at line $LINENO (exit code $?)"' ERR

###############################################################################
# CONFIG
###############################################################################
DCGM_DIAG_LEVEL="${DCGM_DIAG_LEVEL:-3}"
WORK_DIR="/tmp/gpu-stress-$$"
SCRIPT_VERSION="2.1.4"

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
# HELPER: Get DCGM version
###############################################################################
get_dcgm_version() {
    DCGM_VER=$(dcgmi --version 2>/dev/null | grep -oP '[0-9]+\.[0-9]+\.[0-9]+' | head -1 || true)
    [[ -z "$DCGM_VER" ]] && DCGM_VER=$(dcgmi -v 2>/dev/null | grep -oP '[0-9]+\.[0-9]+\.[0-9]+' | head -1 || true)
    [[ -z "$DCGM_VER" ]] && DCGM_VER=$(dcgmi version 2>/dev/null | grep -oP '[0-9]+\.[0-9]+\.[0-9]+' | head -1 || true)
    [[ -z "$DCGM_VER" ]] && DCGM_VER="unknown"
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
        --arg dcgm "${DCGM_VER:-unknown}" --argjson gpus "${SMI_GPU_COUNT:-0}" \
        '{
            report_metadata:{script_version:$v, script_name:"gpu-stress-test"},
            verdict:{overall:"FAIL", issues:[{"issue":$m,"severity":"critical"}]},
            system:{nvidia_driver_version:$drv, cuda_version:$cuda, dcgm_version:$dcgm, gpu_count:$gpus},
            dcgm_diagnostics:{run_level:0, exit_code:-1, duration_seconds:0, test_results:[]}
        }'
    exit 1
}

###############################################################################
# COLLECT FAILURE DIAGNOSTICS (runs only on DCGM FAIL)
###############################################################################
collect_failure_diagnostics() {
    log "============================================"
    log "=== Collecting failure diagnostics ===      "
    log "============================================"

    # --- nvidia-bug-report.sh ---
    if command -v nvidia-bug-report.sh &>/dev/null; then
        log "Running nvidia-bug-report.sh..."
        local bug_report_dir="$WORK_DIR/bug-report"
        mkdir -p "$bug_report_dir"

        # Run from bug_report_dir so .log.gz lands there (120s timeout)
        if ( cd "$bug_report_dir" && timeout 120 nvidia-bug-report.sh ) 2>&1 >&2; then
            :
        else
            warn "nvidia-bug-report.sh exited non-zero (may still have produced output)"
        fi

        # Find the .log.gz — check our dir first, then CWD, then /tmp
        local gz_file=""
        local search_dir
        for search_dir in "$bug_report_dir" "." "/tmp"; do
            gz_file=$(find "$search_dir" -maxdepth 1 -name "nvidia-bug-report.log.gz" 2>/dev/null | head -1)
            [[ -n "$gz_file" ]] && break
        done

        if [[ -n "$gz_file" && -f "$gz_file" ]]; then
            local gz_size
            gz_size=$(stat -c%s "$gz_file" 2>/dev/null || echo "0")
            log "=== BEGIN nvidia-bug-report ($gz_size bytes compressed) ==="
            gunzip -c "$gz_file" >&2 2>/dev/null || warn "Failed to decompress $gz_file"
            log "=== END nvidia-bug-report ==="
            rm -f "$gz_file"
        else
            warn "nvidia-bug-report.sh ran but no .log.gz found"
        fi
    else
        warn "nvidia-bug-report.sh not available -- skipping"
    fi

    # --- fieldiag ---
    if command -v fieldiag &>/dev/null; then
        log "Running fieldiag..."
        log "=== BEGIN fieldiag ==="
        timeout 300 fieldiag 2>&1 >&2 || warn "fieldiag failed or timed out"
        log "=== END fieldiag ==="
    else
        log "fieldiag not available -- skipping"
    fi

    log "============================================"
    log "=== End failure diagnostics ===             "
    log "============================================"
}

###############################################################################
# PREFLIGHT
###############################################################################
preflight() {
    log "=== Preflight checks ==="

    # Check tools
    local missing=""
    command -v dcgmi      &>/dev/null || missing+=" dcgmi"
    command -v nvidia-smi &>/dev/null || missing+=" nvidia-smi"
    command -v jq         &>/dev/null || missing+=" jq"

    [[ -n "$missing" ]] && fail_json "Missing required tools:$missing -- run 90-nexgen-gpu-install first"

    # Ensure DCGM service running (4.x: systemd, 3.x: nv-hostengine)
    if systemctl is-active nvidia-dcgm &>/dev/null 2>&1; then
        log "nvidia-dcgm service running"
    elif systemctl start nvidia-dcgm &>/dev/null 2>&1; then
        log "Started nvidia-dcgm systemd service"
        sleep 3
    elif ! pgrep -x nv-hostengine &>/dev/null; then
        log "Starting nv-hostengine..."
        nv-hostengine 2>&1 >&2 || {
            rm -f /var/run/nvidia-hostengine/socket 2>/dev/null
            nv-hostengine 2>&1 >&2 || fail_json "DCGM service failed to start"
        }
        sleep 3
    fi

    # Get versions
    get_smi_header_info
    get_dcgm_version
    log "nvidia-smi: $SMI_GPU_COUNT GPU(s), driver $SMI_DRIVER, CUDA $SMI_CUDA"
    log "DCGM: $DCGM_VER"

    # Check DCGM can see GPUs (retry up to 3 times)
    local dcgm_gpus=0
    local attempt
    for attempt in 1 2 3; do
        dcgm_gpus=$(dcgmi discovery -l 2>/dev/null | grep -oP '^\d+ GPUs found' | grep -oP '^\d+' || echo "0")
        dcgm_gpus=$((dcgm_gpus + 0))
        [[ "$dcgm_gpus" -gt 0 ]] && break
        log "DCGM discovery attempt $attempt: 0 GPUs, retrying..."
        sleep 3
    done
    log "DCGM sees $dcgm_gpus GPU(s)"

    if [[ "$dcgm_gpus" -eq 0 ]]; then
        # Dump diagnostic info before failing
        warn "--- DCGM diagnostics ---"
        dcgmi discovery -l >&2 2>&1 || true
        nvidia-smi --query-gpu=persistence_mode --format=csv >&2 2>&1 || true
        ls -la /dev/nvidia* >&2 2>&1 || true
        lsmod | grep nvidia >&2 2>&1 || true
        warn "--- end DCGM diagnostics ---"
        fail_json "DCGM sees 0 GPUs while nvidia-smi sees $SMI_GPU_COUNT -- DCGM/driver incompatibility (driver $SMI_DRIVER, DCGM $DCGM_VER)"
    fi

    log "Run level: $DCGM_DIAG_LEVEL"
}

###############################################################################
# RUN DCGM DIAGNOSTICS
###############################################################################
run_diagnostics() {
    log "=== Running DCGM Level $DCGM_DIAG_LEVEL diagnostics ==="
    log "This may take several minutes (Level 3 ~8-15 min, Level 4 ~30-90 min)..."

    local diag_start diag_end diag_exit
    diag_start=$(date +%s)

    dcgmi diag -r "$DCGM_DIAG_LEVEL" -j \
        > "$WORK_DIR/diag_raw.json" 2>"$WORK_DIR/diag_stderr.txt"
    diag_exit=$?
    diag_end=$(date +%s)

    local diag_dur=$(( diag_end - diag_start ))
    log "DCGM diag exited $diag_exit in ${diag_dur}s"

    # Parse results
    local overall="PASS"
    local test_results="[]"
    local issues="[]"

    if [[ -s "$WORK_DIR/diag_raw.json" ]] && jq empty "$WORK_DIR/diag_raw.json" 2>/dev/null; then

        # Dump top-level keys so we can see what DCGM gave us
        local top_keys
        top_keys=$(jq -r 'keys[]' "$WORK_DIR/diag_raw.json" 2>/dev/null | tr '\n' ', ' || echo "unknown")
        log "DCGM JSON top-level keys: $top_keys"

        # --- Strategy 1: DCGM 4.x .categories[].tests[] ---
        test_results=$(jq '[
            .categories[]?.tests[]? |
            {
                test: .name,
                results: [.results[]? | {
                    gpu_id: (.gpu_ids // [null])[0],
                    status: .status,
                    info: (.info // ""),
                    warnings: [.warnings[]?.message // empty]
                }]
            }
        ] // []' "$WORK_DIR/diag_raw.json" 2>/dev/null || echo "[]")

        # --- Strategy 2: DCGM 4.x .DCGM_DIAG_RESPONSE.categories[] ---
        if [[ "$(echo "$test_results" | jq 'length')" -eq 0 ]]; then
            log "Trying DCGM_DIAG_RESPONSE.categories path..."
            test_results=$(jq '[
                .DCGM_DIAG_RESPONSE?.categories[]?.tests[]? |
                {
                    test: .name,
                    results: [.results[]? | {
                        gpu_id: (.gpu_ids // [null])[0],
                        status: .status,
                        info: (.info // ""),
                        warnings: [.warnings[]?.message // empty]
                    }]
                }
            ] // []' "$WORK_DIR/diag_raw.json" 2>/dev/null || echo "[]")
        fi

        # --- Strategy 3: DCGM 3.x flat .tests[] or .DCGM_DIAG_RESPONSE.tests[] ---
        if [[ "$(echo "$test_results" | jq 'length')" -eq 0 ]]; then
            log "Trying legacy .tests[] path..."
            test_results=$(jq '[
                (.tests // .DCGM_DIAG_RESPONSE.tests // [])[] |
                {
                    test: (.name // .test_name // "unknown"),
                    results: [(.results // [])[] | {
                        gpu_id: (.gpu_id // .gpuId // null),
                        status: (.status // .result // "Unknown"),
                        info: (.info // ""),
                        warnings: [(.warnings // [])[] | .message? // . // empty]
                    }]
                }
            ] // []' "$WORK_DIR/diag_raw.json" 2>/dev/null || echo "[]")
        fi

        # --- Strategy 4: Generic deep scan for any test-like objects ---
        if [[ "$(echo "$test_results" | jq 'length')" -eq 0 ]]; then
            log "Trying deep scan for test results..."
            test_results=$(jq '[
                .. | objects | select(has("name") and has("results")) |
                {
                    test: .name,
                    results: [.results[] | {
                        gpu_id: (.gpu_id // (.gpu_ids // [null])[0] // null),
                        status: (.status // .result // "Unknown"),
                        info: (.info // ""),
                        warnings: []
                    }]
                }
            ] | unique_by(.test) // []' "$WORK_DIR/diag_raw.json" 2>/dev/null || echo "[]")
        fi

        local test_count
        test_count=$(echo "$test_results" | jq 'length' 2>/dev/null || echo "0")

        if [[ "$test_count" -eq 0 ]]; then
            warn "Could not parse DCGM JSON -- dumping raw structure:"
            jq '.. | objects | keys' "$WORK_DIR/diag_raw.json" 2>/dev/null | sort -u | head -30 >&2
            warn "Raw JSON (first 2000 chars):"
            head -c 2000 "$WORK_DIR/diag_raw.json" >&2
            echo "" >&2
        else
            # Log per-test summary to stderr
            log "--- Test Results ($test_count tests) ---"
            echo "$test_results" | jq -r '.[] |
                .test as $t |
                if (.results | length) == 0 then
                    "\($t): No per-GPU results"
                else
                    .results[] |
                    if .gpu_id != null then
                        "\($t) [GPU \(.gpu_id)]: \(.status)\(if .info != "" then " -- " + .info else "" end)"
                    else
                        "\($t): \(.status)\(if .info != "" then " -- " + .info else "" end)"
                    end
                end
            ' 2>/dev/null | while IFS= read -r line; do
                # Color-code in log
                if echo "$line" | grep -qiE "fail|error"; then
                    err "  $line"
                elif echo "$line" | grep -qi "warn"; then
                    warn "  $line"
                else
                    log "  $line"
                fi
            done
            log "--- End Test Results ---"
        fi

        # Count failures and warnings
        local fail_count warn_count pass_count skip_count
        fail_count=$(echo "$test_results" | jq '[.[].results[] | select(.status | test("(?i)fail"))] | length' 2>/dev/null || echo "0")
        warn_count=$(echo "$test_results" | jq '[.[].results[] | select(.status | test("(?i)warn"))] | length' 2>/dev/null || echo "0")
        pass_count=$(echo "$test_results" | jq '[.[].results[] | select(.status | test("(?i)pass"))] | length' 2>/dev/null || echo "0")
        skip_count=$(echo "$test_results" | jq '[.[].results[] | select(.status | test("(?i)skip"))] | length' 2>/dev/null || echo "0")

        log "Summary: $pass_count passed, $fail_count failed, $warn_count warnings, $skip_count skipped"

        if [[ "$fail_count" -gt 0 ]]; then
            overall="FAIL"
            # Extract specific failure details
            local fail_details
            fail_details=$(echo "$test_results" | jq -r '[
                .[].results[] | select(.status | test("(?i)fail")) |
                "\(.gpu_id // "all"):\(.info)"
            ] | join("; ")' 2>/dev/null | head -c 500 || echo "")
            issues=$(echo "$issues" | jq \
                --argjson n "$fail_count" --arg d "$fail_details" \
                '. + [{"issue":"\($n) test(s) failed","severity":"critical","details":$d}]')
        fi
        if [[ "$warn_count" -gt 0 ]]; then
            [[ "$overall" == "PASS" ]] && overall="WARN"
            issues=$(echo "$issues" | jq --argjson n "$warn_count" \
                '. + [{"issue":"\($n) test(s) with warnings","severity":"warning"}]')
        fi
    else
        # No valid JSON output
        if [[ "$diag_exit" -ne 0 ]]; then
            overall="FAIL"
            local stderr_msg
            stderr_msg=$(head -5 "$WORK_DIR/diag_stderr.txt" 2>/dev/null | tr '\n' ' ' || echo "unknown error")
            issues=$(echo "$issues" | jq --arg m "DCGM diag failed (exit $diag_exit): $stderr_msg" \
                '. + [{"issue":$m,"severity":"critical"}]')
        fi
        warn "DCGM produced no valid JSON output"
        [[ -s "$WORK_DIR/diag_stderr.txt" ]] && { warn "stderr:"; head -10 "$WORK_DIR/diag_stderr.txt" >&2; }
    fi

    # Non-zero exit with empty results = FAIL
    if [[ "$diag_exit" -ne 0 && "$overall" == "PASS" ]]; then
        local total
        total=$(echo "$test_results" | jq '[.[].results[]] | length' 2>/dev/null || echo "0")
        if [[ "$total" -eq 0 ]]; then
            overall="FAIL"
            issues=$(echo "$issues" | jq --argjson e "$diag_exit" \
                '. + [{"issue":"DCGM exited \($e) with no test results","severity":"critical"}]')
        fi
    fi

    log "=== STRESS TEST COMPLETE -- Verdict: $overall (${diag_dur}s) ==="

    # Final report
    jq -n \
        --arg ver "$SCRIPT_VERSION" --arg name "gpu-stress-test" \
        --arg ts "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" \
        --argjson dur "$(( $(date +%s) - SCRIPT_START ))" \
        --arg verdict "$overall" --argjson issues "$issues" \
        --arg drv "$SMI_DRIVER" --arg cuda "$SMI_CUDA" \
        --arg dcgm "$DCGM_VER" --argjson gpus "$SMI_GPU_COUNT" \
        --argjson level "$DCGM_DIAG_LEVEL" --argjson exit_code "$diag_exit" \
        --argjson diag_dur "$diag_dur" --argjson results "$test_results" \
        '{
            report_metadata:{script_version:$ver, script_name:$name, generated_at:$ts, duration_seconds:$dur},
            verdict:{overall:$verdict, issues:$issues},
            system:{nvidia_driver_version:$drv, cuda_version:$cuda, dcgm_version:$dcgm, gpu_count:$gpus},
            dcgm_diagnostics:{run_level:$level, exit_code:$exit_code, duration_seconds:$diag_dur, test_results:$results}
        }'

    [[ "$overall" == "FAIL" ]] && return 1
    return 0
}

###############################################################################
# MAIN
###############################################################################
main() {
    SCRIPT_START=$(date +%s)

    log "=========================================="
    log "NexGen GPU Stress Test v${SCRIPT_VERSION}"
    log "DCGM Level $DCGM_DIAG_LEVEL"
    log "=========================================="

    preflight
    local stress_ok=true
    if ! run_diagnostics; then
        stress_ok=false
        collect_failure_diagnostics
    fi

    rm -rf "$WORK_DIR"

    log "=========================================="
    log "Stress test complete. Total time: $(( $(date +%s) - SCRIPT_START ))s"
    log "=========================================="

    if [[ "$stress_ok" == "false" ]]; then
        exit 1
    fi
}

main "$@"
