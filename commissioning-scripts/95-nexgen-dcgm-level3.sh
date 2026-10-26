#!/bin/bash
# --- Start MAAS Metadata ---
# name: 95-nexgen-dcgm-level3
# title: NexGen GPU DCGM Level 3 (adds stress, power, SM diagnostic)
# description: Runs the NVIDIA DCGM diagnostic at level 3 and adjudicates it.
#   Level 3: adds targeted stress, targeted power, the SM diagnostic and
#   memory bandwidth.  NVIDIA: under 35 min on an 8-GPU Hopper host.  Ceiling
#   DCGM_DIAG_TIMEOUT=9600 (2h40).
#   One rung of the DCGM ladder (93-96).  DCGM levels are cumulative, so
#   each rung repeats the lower ones; every rung records its verdict in
#   /run/nexgen-dcgm-ladder.jsonl and refuses to run if a lower rung FAILED.
#   Upload only the rungs you want -- a rung that is absent is not consulted.
#   Requires 90-nexgen-gpu-install and 92-nexgen-gpu-inventory first.
# script_type: commissioning
# hardware_type: gpu
# timeout: 03:00:00
# destructive: false
# may_reboot: false
# --- End MAAS Metadata ---

set -o pipefail
trap 'warn "Command failed at line $LINENO (exit code $?)"' ERR

###############################################################################
# CONFIG
###############################################################################
# Which level this file runs comes from its own name: 93-96 share one body
# (tools/dcgm-ladder-sync.sh keeps them identical), so the body is written
# once and the name decides.  An explicit DCGM_DIAG_LEVEL still wins -- that
# is also how a named test list such as "pcie,memory" is requested.
case "$(basename "${BASH_SOURCE[0]:-$0}")" in
    93*) _LEVEL_FROM_NAME=1 ;;
    94*) _LEVEL_FROM_NAME=2 ;;
    95*) _LEVEL_FROM_NAME=3 ;;
    96*) _LEVEL_FROM_NAME=4 ;;
    *)    _LEVEL_FROM_NAME=3 ;;
esac
if [[ -n "${DCGM_DIAG_LEVEL:-}" ]]; then
    DCGM_LEVEL_SOURCE="DCGM_DIAG_LEVEL env"
else
    DCGM_DIAG_LEVEL="$_LEVEL_FROM_NAME"
    DCGM_LEVEL_SOURCE="script name"
fi
# Hard ceiling on the diag run, per level.  Without it a hung dcgmi runs until
# MAAS gives up, with NO JSON emitted, so the run yields no evidence either
# way.  NVIDIA quotes level 3 under 35 min and level 4 under 2.25 h on an
# 8-GPU Hopper host; the defaults leave room for slower silicon (an RTX PRO
# 6000 host was still inside level 3 at 100 min) and give level 4 the hours
# it needs rather than killing it.  The MAAS metadata timeout is documentation
# only in this deployment (tests/validate-maas-metadata.py explains why), so
# this value is the real limit -- and the MAAS node timeout must exceed it.
case "$DCGM_DIAG_LEVEL" in
    1) _CEILING_DEFAULT=1200  ;;
    2) _CEILING_DEFAULT=4800  ;;
    4) _CEILING_DEFAULT=27600 ;;
    *) _CEILING_DEFAULT=9600  ;;
esac
DCGM_DIAG_TIMEOUT="${DCGM_DIAG_TIMEOUT:-$_CEILING_DEFAULT}"
# Passed straight to `dcgmi diag -p`, e.g. "pcie.is_allowed=false" or
# "targeted_power.test_duration=120;memory.max_free_memory=..." -- the way to
# exclude or shorten one plugin once a run has shown which one is the problem.
DCGM_DIAG_PARAMS="${DCGM_DIAG_PARAMS:-}"
# Chassis sensors sampled across the diagnostic, on their own slower cadence:
# the BMC is read over the in-band KCS interface, which serialises requests,
# and fan RPM and inlet air move on a timescale of minutes. 0 disables.
CHASSIS_SAMPLE_INTERVAL="${CHASSIS_SAMPLE_INTERVAL:-30}"
# GPU load is sampled on the same cadence: utilisation, power, SM clock and
# temperature per GPU, plus which nvvs-family processes exist. When a run is
# killed at the ceiling this is the only record of when the cards were last
# busy, which separates "a test is slow on this silicon" from "nvvs hung after
# the load phase finished". 0 disables.
GPU_SAMPLE_INTERVAL="${GPU_SAMPLE_INTERVAL:-30}"
# A one-line progress report is written every PROGRESS_INTERVAL seconds while
# the diag runs: elapsed time, per-GPU utilisation and power, which nvvs
# processes exist, and the last plugin line in nvvs' log.  It goes to stderr
# (MAAS keeps it with the result) and, best effort, to /dev/console so it can
# be watched live on the BMC remote console.  Must be a multiple of
# GPU_SAMPLE_INTERVAL; 0 disables.
PROGRESS_INTERVAL="${PROGRESS_INTERVAL:-60}"
WORK_DIR="/tmp/gpu-stress-$$"
SCRIPT_VERSION="3.0.0"

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
    # No `|| echo "0"` fallback on this pipeline: --query-gpu=count prints one
    # line per GPU, head -1 closes the pipe early, and under pipefail the
    # fallback would append to the real value ("8\n0") rather than replace it.
    # Sanitise the captured value instead.
    SMI_GPU_COUNT=$(nvidia-smi --query-gpu=count --format=csv,noheader,nounits 2>/dev/null | awk 'NR==1{print;exit}')
    SMI_GPU_COUNT="${SMI_GPU_COUNT//[^0-9]/}"
    SMI_GPU_COUNT="${SMI_GPU_COUNT:-0}"
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
    ladder_record "FAIL" -1 0
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

    # --- DCGM's own account of the run -------------------------------------
    # The JSON only says the run was killed or failed; nvvs' log names the
    # plugin it was in, and the load timeline says whether the cards were
    # still busy when it ended.  Both are gone with the ephemeral image.
    local nvvs_log nvvs_size
    for nvvs_log in "${DCGM_DEBUG_LOG:-}" /var/log/nvidia-dcgm/nvvs.log; do
        [[ -n "$nvvs_log" && -s "$nvvs_log" ]] || continue
        nvvs_size=$(stat -c%s "$nvvs_log" 2>/dev/null || echo "0")
        log "---------- $nvvs_log (${nvvs_size} bytes): plugin/test markers, then the last 150 lines ----------"
        grep -niE 'plugin|running test|starting test|test (start|end|finish|complet)|timeout|timed out|killed|abort|fatal' \
            "$nvvs_log" 2>/dev/null | tail -60 >&2 || true
        log "..."
        tail -n 150 "$nvvs_log" >&2 || true
        log "---------- end $nvvs_log ----------"
        _artifact "$(basename "$nvvs_log")" "$nvvs_size" "${hostname} ${stamp}"
    done
    if [[ -s "$WORK_DIR/diag_stderr.txt" ]]; then
        log "---------- dcgmi diag stderr ----------"
        head -c 20000 "$WORK_DIR/diag_stderr.txt" >&2 || true
        echo >&2
        log "---------- end dcgmi diag stderr ----------"
    fi
    print_gpu_load_timeline "$WORK_DIR/gpu.psv"
    _artifact "gpu-load-timeline" "$(stat -c%s "$WORK_DIR/gpu.psv" 2>/dev/null || echo "0")" \
        "${GPU_SAMPLE_INTERVAL}s samples; ${LOAD_NOTE:-no load samples}"

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
            # head closes the pipe at the cap, zcat then dies of SIGPIPE (141)
            # and pipefail reports the whole pipeline as failed -- so an
            # `|| warn` on that line fired on every report over 2 MB, AFTER
            # printing it.  Integrity is checked on its own, first.
            if gzip -t "$gz_file" 2>/dev/null; then
                local gz_decomp
                gz_decomp=$(zcat "$gz_file" 2>/dev/null | wc -c || echo "0")
                log "---------- begin nvidia-bug-report (${gz_decomp} bytes decompressed) ----------"
                zcat "$gz_file" 2>/dev/null | head -c 2000000 >&2 || true
                echo >&2
                [[ "${gz_decomp:-0}" -gt 2000000 ]] && \
                    log "(nvidia-bug-report truncated at 2000000 of ${gz_decomp} bytes)"
                log "---------- end nvidia-bug-report ----------"
            else
                warn "could not decompress $gz_file -- gzip -t failed"
            fi
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
# nvbandwidth AVAILABILITY
###############################################################################
# DCGM's nvbandwidth plugin shells out to the standalone nvbandwidth binary.
# When it is absent the test reports status "Skip" with an empty info field and
# no warning, which is unattributable: the same output appears on a machine
# with no NVLink to measure, on a machine whose fabric is untested because the
# tool is missing, and on a machine DCGM skipped for a reason of its own.
# Recording whether the binary is discoverable turns that into evidence, and
# the report can then say which case it is rather than inferring from NVLink
# presence -- an inference that gave the right answer for the wrong reason on
# a PCIe host and the wrong answer on a bridged one.
nvbandwidth_state() {
    local path ver
    path=$(command -v nvbandwidth 2>/dev/null || true)
    if [[ -z "$path" ]]; then
        # DCGM ships it under plugins/cuda<major>/, where <major> is the CUDA
        # driver major version -- cuda13 for a 5xx driver, cuda12 for older.
        # Omitting that segment reported available:false on a machine where the
        # binary was present, which is worse than not checking: it attributed a
        # by-design SKU skip to a missing tool. Globbed rather than pinned so a
        # DCGM repackaging does not silently reintroduce the false negative.
        for c in /usr/local/bin/nvbandwidth /usr/bin/nvbandwidth \
                 /usr/libexec/datacenter-gpu-manager-4/plugins/cuda*/nvbandwidth \
                 /usr/libexec/datacenter-gpu-manager*/plugins/*/nvbandwidth \
                 /usr/libexec/datacenter-gpu-manager-4/nvbandwidth; do
            [[ -x "$c" ]] && { path="$c"; break; }
        done
    fi
    if [[ -z "$path" ]]; then
        jq -n '{available:false, path:null, version:null,
                note:"nvbandwidth binary not found in any known location; DCGM cannot run its nvbandwidth plugin. Note a missing binary makes the plugin FAIL, not Skip, so this does not by itself explain a Skip result"}'
        return 0
    fi
    ver=$("$path" --version 2>&1 | head -1 || true)
    jq -n --arg p "$path" --arg v "${ver:-unknown}" \
        '{available:true, path:$p, version:$v, note:null}'
}

###############################################################################
# CHASSIS SAMPLER -- fans, inlet air, board zones, PSU draw
###############################################################################
start_chassis_sampler() {
    local out="$1"
    [[ "${CHASSIS_SAMPLE_INTERVAL:-0}" -gt 0 ]] || return 0
    command -v ipmitool >/dev/null 2>&1 || {
        warn "ipmitool unavailable -- no fan or inlet telemetry for the diagnostic"
        return 0; }
    # stdout is closed for the sampler on purpose. A backgrounded subshell
    # inherits the script's stdout, and when the subshell is killed the `sleep`
    # it spawned is orphaned and keeps that descriptor open. Anything reading
    # the script's output -- MAAS capturing the result, or $( ) in a test --
    # blocks until every writer closes, so the run appears to hang for up to
    # one sample interval after it has actually finished. The samples go to a
    # file by explicit redirect, so nothing is lost by detaching stdout.
    (
        while :; do
            ipmitool sdr elist full 2>/dev/null | awk -v ts="$(date +%s)" -F'|' '
                {
                    name = $1; gsub(/^[ \t]+|[ \t]+$/, "", name)
                    val  = $5; gsub(/^[ \t]+|[ \t]+$/, "", val)
                    if (val ~ /^-?[0-9.]+ /) {
                        n = val; sub(/ .*/, "", n)
                        u = val; sub(/^[^ ]+ /, "", u)
                        if (u ~ /RPM/)          u = "RPM"
                        else if (u ~ /degrees/) u = "C"
                        else if (u ~ /Watts/)   u = "W"
                        else next
                        printf "%s|%s|%s|%s\n", ts, name, n, u
                    }
                }' >> "$out" || true
            sleep "$CHASSIS_SAMPLE_INTERVAL"
        done
    ) >/dev/null 2>&1 &
    CHASSIS_PID=$!
}

stop_chassis_sampler() {
    [[ -n "${CHASSIS_PID:-}" ]] && kill "$CHASSIS_PID" 2>/dev/null || true
    wait "${CHASSIS_PID:-}" 2>/dev/null || true
}

###############################################################################
# GPU LOAD SAMPLER -- what the cards were doing while nvvs ran
###############################################################################
# Rows: ts|gpu|index|util%|W|sm_mhz|temp_c  and  ts|proc|<name(count),...>
# Last plugin/test line nvvs has logged, for the progress report.  DCGM 4.7.0
# records each plugin and test result in nvvs.log; older versions log less,
# and the line is best effort either way.
nvvs_last_plugin_line() {
    local f line=""
    for f in "${DCGM_DEBUG_LOG:-}" /var/log/nvidia-dcgm/nvvs.log; do
        [[ -n "$f" && -s "$f" ]] || continue
        line=$(tail -n 400 "$f" 2>/dev/null | grep -iE 'plugin|running test|test (start|end|finish|complet|result)' | tail -1 | cut -c1-140 || true)
        [[ -n "$line" ]] && break
    done
    printf '%s' "${line:-n/a}"
}

start_gpu_sampler() {
    local out="$1"
    [[ "${GPU_SAMPLE_INTERVAL:-0}" -gt 0 ]] || return 0
    local every=0
    if [[ "${PROGRESS_INTERVAL:-0}" -gt 0 ]]; then
        every=$(( PROGRESS_INTERVAL / GPU_SAMPLE_INTERVAL ))
        [[ "$every" -lt 1 ]] && every=1
    fi
    # stdout detached for the same reason as the chassis sampler.  stderr is
    # kept for the progress lines; the sleep -- the only thing that outlives a
    # kill -- gets its descriptors closed so it cannot hold the log open.
    (
        local ts procs smi n=0 t0 line
        t0=$(date +%s)
        while :; do
            ts=$(date +%s)
            smi=$(nvidia-smi --query-gpu=index,utilization.gpu,power.draw,clocks.sm,temperature.gpu \
                --format=csv,noheader,nounits 2>/dev/null || true)
            printf '%s\n' "$smi" \
                | awk -v ts="$ts" -F', *' 'NF>=5 {printf "%s|gpu|%s|%s|%s|%s|%s\n", ts, $1, $2, $3, $4, $5}' \
                >> "$out" || true
            procs=$(ps -eo comm= 2>/dev/null \
                | grep -E '^(nvvs|nvbandwidth|cuda_eud|eud|dcgmproftester|nv-hostengine)' \
                | sort | uniq -c \
                | awk '{printf "%s%s(%d)", (NR>1?",":""), $2, $1}' || true)
            printf '%s|proc|%s\n' "$ts" "${procs:-none}" >> "$out" || true
            n=$((n + 1))
            if [[ "$every" -gt 0 && $(( n % every )) -eq 0 ]]; then
                line=$(printf '%s\n' "$smi" | awk -F', *' '
                    NF>=5 { u = u (u==""?"":" ") ($2+0); w = w (w==""?"":" ") int($3+0); c = c (c==""?"":" ") int($4+0) }
                    END { printf "util%%=[%s] W=[%s] sm=[%s]", u, w, c }')
                line="diag progress t+$(( ts - t0 ))s: ${line} procs=${procs:-none} nvvs: $(nvvs_last_plugin_line)"
                log "$line"
                [[ -w /dev/console ]] && \
                    timeout 2 sh -c 'printf "[98-gpu-stress] %s\n" "$1" > /dev/console' _ "$line" >/dev/null 2>&1 || true
            fi
            sleep "$GPU_SAMPLE_INTERVAL" >/dev/null 2>&1
        done
    ) >/dev/null &
    GPU_SAMPLER_PID=$!
}

stop_gpu_sampler() {
    [[ -n "${GPU_SAMPLER_PID:-}" ]] && kill "$GPU_SAMPLER_PID" 2>/dev/null || true
    wait "${GPU_SAMPLER_PID:-}" 2>/dev/null || true
}

# JSON summary: how many samples, how many had any GPU at >=50% utilisation,
# the first and last such sample (epoch), and the peak summed board power.
summarise_gpu_load() {
    local raw="$1" out
    [[ -s "$raw" ]] || { echo 'null'; return 0; }
    out=$(awk -F'|' -v iv="${GPU_SAMPLE_INTERVAL:-0}" '
        $2 == "gpu" {
            t = $1; u = $4 + 0; p = $5 + 0
            if (!(t in seen)) { seen[t] = 1; order[++k] = t }
            if (u > mu[t]) mu[t] = u
            sp[t] += p
        }
        END {
            loaded = 0; first = ""; last = ""; peak = 0; lastts = ""
            for (i = 1; i <= k; i++) {
                t = order[i]; lastts = t
                if (mu[t] >= 50) { loaded++; if (first == "") first = t; last = t }
                if (sp[t] > peak) peak = sp[t]
            }
            printf "{\"samples\":%d,\"interval_s\":%d,\"samples_under_load\":%d,", k, iv, loaded
            printf "\"first_load_ts\":%s,\"last_load_ts\":%s,\"last_sample_ts\":%s,\"peak_total_power_w\":%.0f}", \
                (first == "" ? "null" : first), (last == "" ? "null" : last), (lastts == "" ? "null" : lastts), peak
        }' "$raw" 2>/dev/null | jq -c . 2>/dev/null) || out=""
    echo "${out:-null}"
}

# One line per sample so the log shows the shape of the run: when load began,
# when it stopped, and whether nvvs was still alive afterwards.
print_gpu_load_timeline() {
    local raw="$1"
    [[ -s "$raw" ]] || { log "no GPU load samples recorded"; return 0; }
    log "---------- GPU load timeline (${GPU_SAMPLE_INTERVAL}s samples: util% / W / SM MHz per GPU; nvvs-family processes) ----------"
    awk -F'|' '
        function flush() { if (cur != "") printf "%s util%%=[%s] W=[%s] sm=[%s] procs=%s\n", cur, u, w, c, p }
        $1 != cur  { flush(); cur = $1; u = ""; w = ""; c = ""; p = "none" }
        $2 == "gpu"  { u = u (u == "" ? "" : " ") ($4 + 0); w = w (w == "" ? "" : " ") int($5 + 0); c = c (c == "" ? "" : " ") int($6 + 0) }
        $2 == "proc" { p = $3 }
        END { flush() }' "$raw" 2>/dev/null \
    | while IFS=' ' read -r ts rest; do
        printf '%s %s\n' "$(date -d "@$ts" '+%H:%M:%S' 2>/dev/null || echo "$ts")" "$rest" >&2
    done || true
    log "---------- end GPU load timeline ----------"
}

aggregate_chassis() {
    local raw="$1"
    [[ -s "$raw" ]] || { echo 'null'; return 0; }
    awk -F'|' '
        { key = $2 "|" $4
          n[key]++; sum[key] += $3
          if (!(key in mn) || $3 + 0 < mn[key]) mn[key] = $3 + 0
          if (!(key in mx) || $3 + 0 > mx[key]) mx[key] = $3 + 0 }
        END {
            printf "["; first = 1
            for (k in n) {
                split(k, p, "|")
                if (!first) printf ","; first = 0
                printf "{\"sensor\":\"%s\",\"unit\":\"%s\",\"samples\":%d,", p[1], p[2], n[k]
                printf "\"min\":%.1f,\"max\":%.1f,\"mean\":%.1f}", mn[k], mx[k], sum[k] / n[k]
            }
            printf "]"
        }' "$raw" 2>/dev/null | jq '
        # What this is for: confirming every fan turns, and that it responds to
        # load. Absolute RPM answers neither on its own, because the reading
        # depends on the BMC fan-control mode -- 9880 might be full throttle or
        # an energy-saving curve, and nothing readable from the host says which.
        # A fan at 0 RPM is unambiguous in any mode, so that is the primitive.
        #
        # Spread is secondary and grouped BY BANK. Measured on a live
        # ESC8000A-E12P the GPU bank sits near 9880 RPM and the system bank near
        # 8300: different fans, different duty curves. Pooling them shows ~21%
        # with nothing wrong, which would have flagged every server in the fleet.
        . as $sensors
        | [ $sensors[] | select(.unit == "RPM")
            | . + {bank: (.sensor | sub("[0-9]+$"; "") | sub("_$"; ""))} ] as $rpm
        | ($rpm | group_by(.bank)
           | map({ bank: .[0].bank, count: length,
                   slowest: (map(.mean) | min), fastest: (map(.mean) | max),
                   slowest_sensor: (min_by(.mean).sensor),
                   spread_pct: (if (map(.mean) | max) > 0
                                then (((map(.mean) | max) - (map(.mean) | min))
                                      / (map(.mean) | max) * 100 | . * 10 | round / 10)
                                else 0 end) })) as $banks
        | { sensors: $sensors,
            fans: { banks: $banks,
                    fan_count: ($rpm | length),
                    stopped: [ $rpm[] | select(.max == 0) | .sensor ],
                    # null, not true, when the BMC exposes no RPM sensors (HPE iLO
                    # reports fans as a percentage, which the sampler drops): a
                    # check that measured nothing has not confirmed anything.
                    all_spinning: (if ($rpm | length) == 0 then null
                                   else (([ $rpm[] | select(.max == 0) ] | length) == 0) end),
                    worst_bank: ($banks | if length > 0 then (max_by(.spread_pct) | .bank) else null end),
                    worst_spread_pct: ($banks | map(.spread_pct) | max // 0) },
            inlet_c: ($sensors | map(select(.sensor | test("Inlet"; "i")))
                      | if length > 0 then {min: .[0].min, max: .[0].max, mean: .[0].mean} else null end)
          }' 2>/dev/null || echo 'null'
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
        dcgm_gpus=$(dcgmi discovery -l 2>/dev/null | grep -oP '^\d+ GPUs? found' | grep -oP '^\d+' || echo "0")
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
    # NVIDIA's own figures (DCGM 4 docs, measured on Hopper): level 3 under
    # 35 min and level 4 under 2.25 h on an 8-GPU host. Other silicon is not
    # characterised there, so the only hard fact is the ceiling below -- and a
    # run that reaches it evidences nothing, which is why the samplers record
    # what the cards and nvvs were doing on the way.
    log "NVIDIA quotes level 3 < 35 min and level 4 < 2.25 h on an 8-GPU Hopper host; hard ceiling here is ${DCGM_DIAG_TIMEOUT}s"

    local diag_start diag_end diag_exit
    diag_start=$(date +%s)

    # The diagnostic is the hottest phase before the burn-in, so the chassis is
    # sampled across it too. Whether a card throttled here is only half the
    # story; whether its fans were turning and the inlet was in spec is the
    # other half, and neither is recoverable once the run is over.
    start_chassis_sampler "$WORK_DIR/chassis.psv"
    start_gpu_sampler "$WORK_DIR/gpu.psv"

    # Options are probed from --help rather than assumed, because the diag CLI
    # differs between DCGM 3.x and 4.x.  --debugLogFile keeps nvvs' own log,
    # which names the plugin that was running when a run died -- the JSON
    # only ever says "killed by a signal".  -t lets DCGM stop itself shortly
    # before the outer ceiling, so it can still write JSON (and the test it
    # was in) instead of being shot by SIGTERM with nothing to show.
    local diag_help
    local -a diag_opts=()
    diag_help=$(dcgmi diag --help 2>&1 || true)
    DCGM_DEBUG_LOG="$WORK_DIR/dcgm_diag_debug.log"
    if [[ "$diag_help" == *--debugLogFile* ]]; then
        diag_opts+=(--debugLogFile "$DCGM_DEBUG_LOG")
    else
        DCGM_DEBUG_LOG=""
        warn "dcgmi diag has no --debugLogFile option -- nvvs log will only be available at /var/log/nvidia-dcgm/nvvs.log"
    fi
    if [[ "$diag_help" == *--timeout* && "$DCGM_DIAG_TIMEOUT" -gt 300 ]]; then
        diag_opts+=(-t "$(( DCGM_DIAG_TIMEOUT - 120 ))")
    fi
    [[ -n "$DCGM_DIAG_PARAMS" ]] && diag_opts+=(-p "$DCGM_DIAG_PARAMS")
    log "Running: dcgmi diag -r $DCGM_DIAG_LEVEL -j ${diag_opts[*]}"
    [[ "${PROGRESS_INTERVAL:-0}" -gt 0 ]] && \
        log "Progress is logged every ${PROGRESS_INTERVAL}s (and echoed to /dev/console for the BMC remote console)"

    timeout "$DCGM_DIAG_TIMEOUT" dcgmi diag -r "$DCGM_DIAG_LEVEL" -j "${diag_opts[@]}" \
        > "$WORK_DIR/diag_raw.json" 2>"$WORK_DIR/diag_stderr.txt"
    diag_exit=$?

    stop_chassis_sampler
    stop_gpu_sampler
    diag_end=$(date +%s)

    local diag_dur=$(( diag_end - diag_start ))
    log "DCGM diag exited $diag_exit in ${diag_dur}s"

    # Where the load went.  Logged on every run, not only failures, because a
    # pass that spent 90 of its 100 minutes idle is worth knowing about too.
    LOAD_PROFILE=$(summarise_gpu_load "$WORK_DIR/gpu.psv")
    LOAD_NOTE=""
    if [[ "$LOAD_PROFILE" != "null" ]]; then
        local l_samples l_loaded l_last l_first
        l_samples=$(printf '%s' "$LOAD_PROFILE" | jq '.samples')
        l_loaded=$(printf '%s' "$LOAD_PROFILE" | jq '.samples_under_load')
        l_first=$(printf '%s' "$LOAD_PROFILE" | jq -r '.first_load_ts // empty')
        l_last=$(printf '%s' "$LOAD_PROFILE" | jq -r '.last_load_ts // empty')
        if [[ -n "$l_last" ]]; then
            LOAD_NOTE="GPUs were at >=50% utilisation in ${l_loaded} of ${l_samples} samples, first at $(date -d "@$l_first" '+%H:%M:%S'), last at $(date -d "@$l_last" '+%H:%M:%S') -- $(( diag_end - l_last ))s before the diag ended"
        else
            LOAD_NOTE="no sample ever showed a GPU at >=50% utilisation across ${l_samples} samples -- the load phase never started"
        fi
        log "Load profile: $LOAD_NOTE"
    fi

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
        issues=$(echo "$issues" | jq --arg t "$DCGM_DIAG_TIMEOUT" --arg d "${LOAD_NOTE:-no load samples}" \
            '. + [{"issue":"DCGM diag exceeded its \($t)s timeout and was killed -- run incomplete","severity":"critical","details":$d}]')
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
    ladder_record "$overall" "$diag_exit" "$diag_dur"

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
        --argjson load "${LOAD_PROFILE:-null}" \
        --argjson ladder "$(ladder_previous_json)" --arg lvlsrc "$DCGM_LEVEL_SOURCE" \
        --argjson artifacts "${DIAG_ARTIFACTS:-[]}" \
        --argjson counters "${POST_COUNTERS:-null}" \
        --argjson chassis "$(aggregate_chassis "$WORK_DIR/chassis.psv")" \
        --argjson nvbw "$(nvbandwidth_state)" \
        '{
            report_metadata:{script_version:$ver, script_name:$name, generated_at:$ts, duration_seconds:$dur, dcgm_level:$level},
            verdict:{overall:$verdict, issues:$issues},
            system:{nvidia_driver_version:$drv, cuda_version:$cuda, dcgm_version:$dcgm, gpu_count:$gpus},
            dcgm_diagnostics:{run_level:$level, level_source:$lvlsrc, exit_code:$exit_code, duration_seconds:$diag_dur, load_profile:$load, test_results:$results},
            ladder:{previous:$ladder},
            chassis:$chassis,
            nvbandwidth:$nvbw,
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
# DCGM LADDER -- levels 1..4 as separate scripts, each gated on the last
###############################################################################
# 93-96 share this body.  Each records its verdict in a tmpfs file, and the
# next rung refuses to run if a lower one FAILED: a level-3 result on hardware
# that already failed level 2 is noise, and the hours level 4 costs are better
# spent on a card that has earned them.  A rung that was not uploaded leaves no
# record and is simply not consulted, so the operator chooses which rungs run
# by choosing which scripts to upload.  WARN does not gate.
NEXGEN_DCGM_LADDER_FILE="${NEXGEN_DCGM_LADDER_FILE:-/run/nexgen-dcgm-ladder.jsonl}"
DCGM_LADDER_IGNORE_PREVIOUS="${DCGM_LADDER_IGNORE_PREVIOUS:-0}"

ladder_previous_json() {   # records from lower levels, as a JSON array
    [[ -s "$NEXGEN_DCGM_LADDER_FILE" ]] || { echo '[]'; return 0; }
    local mine="$DCGM_DIAG_LEVEL"
    [[ "$mine" =~ ^[0-9]+$ ]] || mine=99
    jq -sc --argjson mine "$mine" \
        '[ .[] | select((.level | type) == "number" and .level < $mine) ]' \
        "$NEXGEN_DCGM_LADDER_FILE" 2>/dev/null || echo '[]'
}

ladder_record() {   # verdict, exit code, diag seconds
    local lvl="$DCGM_DIAG_LEVEL"
    [[ "$lvl" =~ ^[0-9]+$ ]] || return 0
    jq -nc --argjson level "$lvl" --arg verdict "$1" --argjson exit "${2:-0}" \
        --argjson secs "${3:-0}" --arg script "$(basename "${BASH_SOURCE[0]:-$0}")" \
        --arg ts "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" \
        '{level:$level, verdict:$verdict, exit_code:$exit, diag_seconds:$secs, script:$script, ts:$ts}' \
        >> "$NEXGEN_DCGM_LADDER_FILE" 2>/dev/null \
        || warn "could not record the level $lvl verdict in $NEXGEN_DCGM_LADDER_FILE"
}

ladder_gate() {
    [[ "$DCGM_DIAG_LEVEL" =~ ^[0-9]+$ ]] || return 0
    local prev failed
    prev=$(ladder_previous_json)
    [[ "$prev" == "[]" ]] && return 0
    log "Lower DCGM levels already run this boot: $(printf '%s' "$prev" | jq -r '[.[] | "L\(.level)=\(.verdict)"] | join(" ")')"
    failed=$(printf '%s' "$prev" | jq -r '[.[] | select(.verdict == "FAIL") | .level] | max // empty')
    [[ -n "$failed" ]] || return 0
    if [[ "$DCGM_LADDER_IGNORE_PREVIOUS" == "1" ]]; then
        warn "Level $failed FAILED earlier but DCGM_LADDER_IGNORE_PREVIOUS=1 -- running level $DCGM_DIAG_LEVEL anyway"
        return 0
    fi
    local reason="DCGM level $failed FAILED earlier in this commissioning run, so level $DCGM_DIAG_LEVEL was not run (DCGM_LADDER_IGNORE_PREVIOUS=1 forces it)"
    err "=============================================================="
    err "  SKIPPED -- $reason"
    err "=============================================================="
    jq -n --arg v "$SCRIPT_VERSION" --arg name "gpu-stress-test" --arg r "$reason" \
        --arg level "$DCGM_DIAG_LEVEL" --argjson prev "$prev" \
        --arg ts "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" \
        '{
            report_metadata:{script_version:$v, script_name:$name, generated_at:$ts, duration_seconds:0, dcgm_level:$level},
            verdict:{overall:"FAIL", issues:[{"issue":("Skipped -- " + $r),"severity":"critical"}]},
            skipped:true, skip_reason:$r,
            dcgm_diagnostics:{run_level:$level, exit_code:-1, duration_seconds:0, test_results:[]},
            ladder:{previous:$prev}
        }'
    exit 1
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
    ladder_gate

    log "=========================================="
    log "NexGen GPU Stress Test v${SCRIPT_VERSION} -- $(basename "${BASH_SOURCE[0]:-$0}")"
    log "DCGM Level $DCGM_DIAG_LEVEL (from $DCGM_LEVEL_SOURCE), ceiling ${DCGM_DIAG_TIMEOUT}s"
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
