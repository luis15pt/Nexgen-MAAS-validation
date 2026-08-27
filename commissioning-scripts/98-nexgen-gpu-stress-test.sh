#!/bin/bash
# --- Start MAAS 1.0 script metadata ---
# name: 98-nexgen-gpu-stress-test
# title: NexGen GPU Stress Test (DCGM Diagnostics)
# description: >-
#   Runs DCGM diagnostics at configurable levels (1-4).
#   Requires 90-nexgen-gpu-install to have installed DCGM 4.x.
#   Level 1: ~1 min quick check. Level 4: ~90 min full validation.
#   Override level: DCGM_DIAG_LEVEL=4
# script_type: commissioning
# parallel: disabled
# hardware_type: gpu
# timeout: 02:00:00
# destructive: false
# may_reboot: false
# --- End MAAS 1.0 script metadata ---

set -o pipefail
trap 'warn "Command failed at line $LINENO (exit code $?)"' ERR

###############################################################################
# CONFIG
###############################################################################
DCGM_DIAG_LEVEL="${DCGM_DIAG_LEVEL:-3}"
# Hard ceiling on the diag run.  Without it a hung dcgmi is killed by MAAS at
# the script timeout with NO JSON emitted, so the run yields no evidence
# either way.  Level 4 can legitimately take ~90 min, hence the wide default.
DCGM_DIAG_TIMEOUT="${DCGM_DIAG_TIMEOUT:-6000}"
WORK_DIR="/tmp/gpu-stress-$$"
SCRIPT_VERSION="2.3.0"

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
# Artifacts are written to stderr, which MAAS retains as part of the
# commissioning result and the report generator can fetch back out.  They are
# deliberately NOT uploaded anywhere: these logs carry GPU serials and host
# identifiers, and the previous sendit.sh path put them on a third-party host
# with a single-download, short-retention link that only ever appeared in the
# log text -- so the evidence was simultaneously exposed and unreachable.
#
# Sets DIAG_ARTIFACTS to a JSON array describing what was captured, so the
# report can state which artifacts exist.  Must therefore run BEFORE the final
# report is emitted.
collect_failure_diagnostics() {
    log "============================================"
    log "=== Collecting failure diagnostics ===      "
    log "============================================"

    local hostname stamp
    hostname=$(hostname 2>/dev/null || echo "unknown")
    stamp=$(date -u '+%Y%m%dT%H%M%SZ')
    DIAG_ARTIFACTS="[]"

    _artifact() { # name, bytes, note
        DIAG_ARTIFACTS=$(printf '%s' "$DIAG_ARTIFACTS" | jq \
            --arg n "$1" --argjson b "${2:-0}" --arg note "${3:-}" \
            '. + [{artifact:$n, bytes:$b, note:$note, location:"MAAS commissioning log (stderr)"}]')
    }

    # --- Xid scan: the fault history the acceptance spec asks for ----------
    # Reported here as well as in the burn-in script so that a stress failure
    # carries its own kernel evidence.
    local xid_lines xid_count
    xid_lines=$(dmesg 2>/dev/null | grep -iE 'NVRM: *Xid' || true)
    xid_count=$(printf '%s' "$xid_lines" | grep -c . || true)
    if [[ "${xid_count:-0}" -gt 0 ]]; then
        err "--- Xid events in dmesg ($xid_count) ---"
        printf '%s\n' "$xid_lines" >&2
        err "--- end Xid events ---"
    else
        log "No Xid events in dmesg"
    fi
    _artifact "dmesg-xid" "${#xid_lines}" "$xid_count Xid line(s)"

    # --- nvidia-bug-report.sh ---------------------------------------------
    if command -v nvidia-bug-report.sh &>/dev/null; then
        log "Running nvidia-bug-report.sh..."
        local bug_report_dir="$WORK_DIR/bug-report"
        mkdir -p "$bug_report_dir"
        ( cd "$bug_report_dir" && timeout 120 nvidia-bug-report.sh ) >&2 2>&1 \
            || warn "nvidia-bug-report.sh exited non-zero (may still have produced output)"

        local gz_file="" search_dir
        for search_dir in "$bug_report_dir" "." "/tmp"; do
            gz_file=$(find "$search_dir" -maxdepth 1 -name "nvidia-bug-report.log.gz" 2>/dev/null | head -1)
            [[ -n "$gz_file" ]] && break
        done

        if [[ -n "$gz_file" && -f "$gz_file" ]]; then
            local gz_size
            gz_size=$(stat -c%s "$gz_file" 2>/dev/null || echo "0")
            log "nvidia-bug-report.log.gz captured ($gz_size bytes)"
            # Emit the decompressed text to stderr so it is retained in the
            # commissioning result rather than left in an ephemeral /tmp file.
            log "---------- begin nvidia-bug-report ----------"
            zcat "$gz_file" 2>/dev/null | head -c 2000000 >&2 || \
                warn "could not decompress $gz_file"
            log "---------- end nvidia-bug-report ----------"
            _artifact "nvidia-bug-report" "$gz_size" "${hostname} ${stamp}"
        else
            warn "nvidia-bug-report.sh ran but no .log.gz found"
        fi
    else
        warn "nvidia-bug-report.sh not available -- skipping"
    fi

    # --- fieldiag ---------------------------------------------------------
    # Entitlement-gated and usually absent; the DCGM path is the one we can
    # rely on. Captured when present because the spec accepts it as evidence.
    if command -v fieldiag &>/dev/null; then
        log "Running fieldiag..."
        local fieldiag_out="$WORK_DIR/fieldiag-${stamp}.txt"
        timeout 300 fieldiag > "$fieldiag_out" 2>&1 || warn "fieldiag failed or timed out"
        if [[ -s "$fieldiag_out" ]]; then
            local fd_size
            fd_size=$(stat -c%s "$fieldiag_out" 2>/dev/null || echo "0")
            log "---------- begin fieldiag ----------"
            head -c 2000000 "$fieldiag_out" >&2
            log "---------- end fieldiag ----------"
            _artifact "fieldiag" "$fd_size" "${hostname} ${stamp}"
        fi
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
        nv-hostengine >&2 2>&1 || {
            rm -f /var/run/nvidia-hostengine/socket 2>/dev/null
            nv-hostengine >&2 2>&1 || fail_json "DCGM service failed to start"
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

    timeout "$DCGM_DIAG_TIMEOUT" dcgmi diag -r "$DCGM_DIAG_LEVEL" -j \
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
        # --- Classify every result status --------------------------------
        # DCGM statuses are not limited to Pass/Fail/Warn.  CONFIG and RETEST
        # mean the rig was misconfigured and the test never characterised the
        # hardware.  SKIP means it did not run.  Neither is a hardware pass,
        # and counting them as one is how a card that was never actually
        # tested ends up reported as healthy.
        local fail_count warn_count pass_count skip_count config_count
        local unknown_count total_count
        _count() {
            echo "$test_results" | jq --arg re "$1" \
                '[.[].results[] | select(.status | test($re; "i"))] | length' 2>/dev/null || echo "0"
        }
        total_count=$(echo "$test_results" | jq '[.[].results[]] | length' 2>/dev/null || echo "0")
        pass_count=$(_count 'pass')
        fail_count=$(_count 'fail')
        warn_count=$(_count 'warn')
        skip_count=$(_count 'skip|not[ _-]?run')
        config_count=$(_count 'config|retest')
        unknown_count=$(( total_count - pass_count - fail_count - warn_count - skip_count - config_count ))
        [[ "$unknown_count" -lt 0 ]] && unknown_count=0

        log "Summary: $total_count result(s) -- $pass_count pass, $fail_count fail, $warn_count warn, $skip_count skip, $config_count config/retest, $unknown_count unclassified"

        if [[ "$fail_count" -gt 0 ]]; then
            overall="FAIL"
            local fail_details
            fail_details=$(echo "$test_results" | jq -r '[
                .[].results[] | select(.status | test("(?i)fail")) |
                "\(.gpu_id // "all"):\(.info)"
            ] | join("; ")' 2>/dev/null | head -c 500 || echo "")
            issues=$(echo "$issues" | jq \
                --argjson n "$fail_count" --arg d "$fail_details" \
                '. + [{"issue":"\($n) test(s) failed","severity":"critical","details":$d}]')
        fi

        # A CONFIG/RETEST result is a test-rig failure, not a hardware pass.
        if [[ "$config_count" -gt 0 ]]; then
            overall="FAIL"
            local cfg_details
            cfg_details=$(echo "$test_results" | jq -r '[
                .[].results[] | select(.status | test("(?i)config|retest")) |
                "\(.gpu_id // "all"):\(.status)"
            ] | join("; ")' 2>/dev/null | head -c 500 || echo "")
            issues=$(echo "$issues" | jq \
                --argjson n "$config_count" --arg d "$cfg_details" \
                '. + [{"issue":"\($n) test(s) returned CONFIG/RETEST -- the rig was misconfigured and the hardware was never characterised","severity":"critical","details":$d}]')
        fi

        # Nothing ran, or nothing passed -- enumeration is not a test.
        if [[ "$total_count" -eq 0 ]]; then
            overall="FAIL"
            issues=$(echo "$issues" | jq \
                '. + [{"issue":"DCGM returned no test results -- no test was executed","severity":"critical"}]')
        elif [[ "$pass_count" -eq 0 ]]; then
            overall="FAIL"
            issues=$(echo "$issues" | jq --argjson t "$total_count" \
                '. + [{"issue":"DCGM produced \($t) result(s) but none passed","severity":"critical"}]')
        fi

        if [[ "$skip_count" -gt 0 ]]; then
            [[ "$overall" == "PASS" ]] && overall="WARN"
            local skip_details
            skip_details=$(echo "$test_results" | jq -r '[
                .[] | select([.results[].status | test("(?i)skip|not[ _-]?run")] | any) | .test
            ] | unique | join(", ")' 2>/dev/null | head -c 300 || echo "")
            issues=$(echo "$issues" | jq \
                --argjson n "$skip_count" --arg d "$skip_details" \
                '. + [{"issue":"\($n) test(s) skipped -- they did not run, so they evidence nothing","severity":"warning","details":$d}]')
        fi

        if [[ "$warn_count" -gt 0 ]]; then
            [[ "$overall" == "PASS" ]] && overall="WARN"
            issues=$(echo "$issues" | jq --argjson n "$warn_count" \
                '. + [{"issue":"\($n) test(s) with warnings","severity":"warning"}]')
        fi

        if [[ "$unknown_count" -gt 0 ]]; then
            [[ "$overall" == "PASS" ]] && overall="WARN"
            local unk_details
            unk_details=$(echo "$test_results" | jq -r \
                '[.[].results[].status] | unique | join(", ")' 2>/dev/null | head -c 300 || echo "")
            issues=$(echo "$issues" | jq \
                --argjson n "$unknown_count" --arg d "$unk_details" \
                '. + [{"issue":"\($n) result(s) carry an unrecognised status and are not counted as a pass","severity":"warning","details":$d}]')
        fi
    else
        # No parseable JSON at all.  A failure regardless of exit code: there
        # is no evidence that any test ran.
        overall="FAIL"
        local stderr_msg
        stderr_msg=$(head -5 "$WORK_DIR/diag_stderr.txt" 2>/dev/null | tr '\n' ' ' || echo "")
        issues=$(echo "$issues" | jq \
            --argjson e "$diag_exit" --arg m "${stderr_msg:-no stderr}" \
            '. + [{"issue":"DCGM produced no parseable JSON (exit \($e)) -- no test evidence","severity":"critical","details":$m}]')
        warn "DCGM produced no valid JSON output"
        [[ -s "$WORK_DIR/diag_stderr.txt" ]] && { warn "stderr:"; head -10 "$WORK_DIR/diag_stderr.txt" >&2; }
    fi

    # `timeout` kills with 124 -- the run never completed, so it proves nothing.
    if [[ "$diag_exit" -eq 124 ]]; then
        overall="FAIL"
        issues=$(echo "$issues" | jq --arg t "$DCGM_DIAG_TIMEOUT" \
            '. + [{"issue":"DCGM diag exceeded its \($t)s timeout and was killed -- run incomplete","severity":"critical"}]')
    # A non-zero exit is never silently discarded, even when results parsed.
    elif [[ "$diag_exit" -ne 0 && "$overall" == "PASS" ]]; then
        overall="WARN"
        issues=$(echo "$issues" | jq --argjson e "$diag_exit" \
            '. + [{"issue":"DCGM exited \($e) but every parsed result passed -- exit code and results disagree","severity":"warning"}]')
    fi

    # Failure diagnostics must run BEFORE the report is emitted so the
    # artifacts they capture can be named in it.  This used to be called from
    # main() after the JSON had already gone to stdout, which made every
    # artifact it collected unreferenceable.
    DIAG_ARTIFACTS="[]"
    if [[ "$overall" == "FAIL" ]]; then
        collect_failure_diagnostics
    fi

    # The diagnostic is itself a substantial load -- level 3 runs ~35 min on an
    # 8-GPU host -- so any fault it induces must be caught here. Nothing else
    # would: 92 ran before it, and the burn-in's own window delta subtracts a
    # baseline already containing whatever happened during this phase.
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
        issues=$(echo "$issues" | jq --argjson n "${POST_NEW_FAULTS}" --arg d "$pf_detail" \
            '. + [{"issue":"\($n) GPU(s) gained uncorrectable ECC errors, uncorrectable remapped rows, a pending remap or a remap failure during the diagnostic","severity":"critical","details":$d}]')
        err "New memory faults during the diagnostic: $pf_detail"
    fi
    if [[ "${POST_COUNTERS_VOID:-0}" -eq 1 ]]; then
        [[ "$overall" == "PASS" ]] && overall="WARN"
        issues=$(echo "$issues" | jq \
            '. + [{"issue":"Error counters went backwards since the pre-load baseline -- something cleared them mid-sequence, so the comparison cannot evidence memory health","severity":"warning"}]')
    fi

    log "=== STRESS TEST COMPLETE -- Verdict: $overall (${diag_dur}s) ==="

    # Final report
    jq -n \
        --arg ver "$SCRIPT_VERSION" --arg name "gpu-stress-test" \
        --arg ts "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" \
        --argjson dur "$(( $(date +%s) - SCRIPT_START ))" \
        --arg verdict "$overall" --argjson issues "$issues" \
        --arg drv "$SMI_DRIVER" --arg cuda "$SMI_CUDA" \
        --arg dcgm "$DCGM_VER" --argjson gpus "${SMI_GPU_COUNT:-0}" \
        --arg level "$DCGM_DIAG_LEVEL" --argjson exit_code "$diag_exit" \
        --argjson diag_dur "$diag_dur" --argjson results "$test_results" \
        --argjson artifacts "${DIAG_ARTIFACTS:-[]}" \
        --argjson counters "${POST_COUNTERS:-null}" \
        '{
            report_metadata:{script_version:$ver, script_name:$name, generated_at:$ts, duration_seconds:$dur},
            verdict:{overall:$verdict, issues:$issues},
            system:{nvidia_driver_version:$drv, cuda_version:$cuda, dcgm_version:$dcgm, gpu_count:$gpus},
            dcgm_diagnostics:{run_level:$level, exit_code:$exit_code, duration_seconds:$diag_dur, test_results:$results},
            diagnostic_artifacts:$artifacts,
            counters_since_baseline:$counters
        }'

    [[ "$overall" == "FAIL" ]] && return 1
    return 0
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
    jq -n --arg v "$SCRIPT_VERSION" --arg name "gpu-stress-test" --arg r "$reason" \
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
    log "NexGen GPU Stress Test v${SCRIPT_VERSION}"
    log "DCGM Level $DCGM_DIAG_LEVEL"
    log "=========================================="

    preflight
    local stress_ok=true
    # run_diagnostics collects failure diagnostics itself, before emitting the
    # report, so there is nothing to do here but record the outcome.
    if ! run_diagnostics; then
        stress_ok=false
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
