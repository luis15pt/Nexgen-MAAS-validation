#!/bin/bash
# --- Start MAAS Metadata ---
# name: 98-nexgen-gpu-inventory
# title: NexGen GPU Inventory & Health Check
# description: Collects GPU inventory (serials, UUIDs, VRAM, ECC counters,
#   PCIe link status, NUMA topology) using a single bulk nvidia-smi query.
#   No packages installed -- requires 97-nexgen-gpu-install to run first.
#   Designed to run every commissioning cycle. Outputs structured JSON.
#   Resilient to nvidia-smi field changes across driver versions.
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
WORK_DIR="/tmp/gpu-inventory-$$"
SCRIPT_VERSION="2.0.2"

###############################################################################
# LOGGING
###############################################################################
log()  { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [INFO]  $*" >&2; }
warn() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [WARN]  $*" >&2; }
err()  { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [ERROR] $*" >&2; }

mkdir -p "$WORK_DIR"

safe_run() { local o; if o=$("$@" 2>/dev/null); then echo "$o"; else echo ""; fi; }

###############################################################################
# HELPER: Parse driver + CUDA from nvidia-smi header (always works)
###############################################################################
get_smi_header_info() {
    local header
    header=$(nvidia-smi 2>/dev/null | head -5)
    SMI_DRIVER=$(echo "$header" | grep -oP 'Driver Version:\s*\K[0-9.]+' || echo "unknown")
    SMI_CUDA=$(echo "$header" | grep -oP 'CUDA Version:\s*\K[0-9.]+' || echo "unknown")
}

###############################################################################
# Normalise nvidia-smi PCI bus ID -> sysfs path
###############################################################################
pci_to_sysfs() {
    local bus="${1,,}"
    bus=$(echo "$bus" | sed 's/^0000\(0000\)/0000/')
    [[ -d "/sys/bus/pci/devices/${bus}" ]] && { echo "$bus"; return; }
    local short="${bus#*:}"
    local found
    found=$(find /sys/bus/pci/devices/ -maxdepth 1 -name "*:${short}" 2>/dev/null | head -1)
    [[ -n "$found" ]] && basename "$found" || echo "$bus"
}

###############################################################################
# Memory type from model name (nvidia-smi memory.type not valid on 580+)
###############################################################################
guess_mem_type() {
    case "$1" in
        *A100*)                echo "HBM2e" ;;
        *H100*)                echo "HBM3"  ;;
        *H200*)                echo "HBM3e" ;;
        *B100*|*B200*|*B300*)  echo "HBM3e" ;;
        *A6000*|*A5000*|*A4000*|*A2000*|*A10*|*A16*|*A30*|*A40*) echo "GDDR6" ;;
        *L4*|*L40*)            echo "GDDR6" ;;
        *RTX*50*)              echo "GDDR7" ;;
        *RTX*40*|*RTX*Ada*)    echo "GDDR6X" ;;
        *RTX*60*|*RTX*30*|*RTX*20*) echo "GDDR6" ;;
        *Tesla*V100*)          echo "HBM2"  ;;
        *Tesla*P100*)          echo "HBM2"  ;;
        *Tesla*T4*)            echo "GDDR6" ;;
        *GTX*)                 echo "GDDR6" ;;
        *)                     echo "Unknown" ;;
    esac
}

###############################################################################
# PREFLIGHT
###############################################################################
preflight() {
    log "=== Preflight checks ==="

    local missing=""
    command -v nvidia-smi &>/dev/null || missing+=" nvidia-smi"
    command -v jq          &>/dev/null || missing+=" jq"
    command -v lspci       &>/dev/null || missing+=" lspci"
    command -v dmidecode   &>/dev/null || missing+=" dmidecode"

    if [[ -n "$missing" ]]; then
        err "Missing required tools:$missing -- run 97-nexgen-gpu-install first"
        jq -n --arg v "$SCRIPT_VERSION" --arg m "Missing:$missing" '{
            report_metadata:{script_version:$v, script_name:"gpu-inventory"},
            verdict:{overall:"FAIL", issues:[{"issue":$m}]}
        }' 2>/dev/null || echo '{"verdict":{"overall":"FAIL"}}'
        exit 1
    fi

    if ! nvidia-smi &>/dev/null; then
        err "nvidia-smi not functional -- run 97-nexgen-gpu-install first"
        jq -n --arg v "$SCRIPT_VERSION" '{
            report_metadata:{script_version:$v, script_name:"gpu-inventory"},
            verdict:{overall:"FAIL", issues:[{"issue":"nvidia-smi not functional"}]}
        }'
        exit 1
    fi

    log "All tools available, nvidia-smi OK"
}

###############################################################################
# SYSTEM CONTEXT
###############################################################################
collect_system_context() {
    log "=== System context ==="

    get_smi_header_info

    jq -n \
        --arg hostname "$(hostname)" \
        --arg serial "$(safe_run dmidecode -s system-serial-number)" \
        --arg product "$(safe_run dmidecode -s system-product-name)" \
        --arg mfg "$(safe_run dmidecode -s system-manufacturer)" \
        --arg mobo "$(safe_run dmidecode -s baseboard-product-name)" \
        --arg cpu "$(lscpu | grep 'Model name:' | sed 's/.*:\s*//')" \
        --argjson sockets "$(lscpu | grep 'Socket(s):' | awk '{print $2}')" \
        --argjson threads "$(lscpu | grep '^CPU(s):' | awk '{print $2}')" \
        --argjson ram "$(awk '/MemTotal/{printf "%d",$2/1024/1024}' /proc/meminfo)" \
        --arg kernel "$(uname -r)" \
        --arg drv "$SMI_DRIVER" \
        --arg cuda "$SMI_CUDA" \
        '{
            hostname:$hostname, serial_number:$serial,
            product_name:$product, manufacturer:$mfg, motherboard:$mobo,
            cpu_model:$cpu, cpu_sockets:$sockets, cpu_total_threads:$threads,
            ram_total_gb:$ram, kernel_version:$kernel,
            nvidia_driver_version:$drv, cuda_version:$cuda
        }' > "$WORK_DIR/system.json"

    log "System: $(jq -r '.product_name' "$WORK_DIR/system.json") (driver $SMI_DRIVER, CUDA $SMI_CUDA)"
}

###############################################################################
# GPU + NUMA -- ONE nvidia-smi + sysfs reads
#
# Strategy: try full field list, if it fails fall back to safe-only fields.
# This handles nvidia-smi field changes across driver versions without
# having to play whack-a-mole removing fields one by one.
#
# PCIe degradation is detected from nvidia-smi gen/width fields per GPU.
###############################################################################
collect_gpu_data() {
    log "=== GPU data (single nvidia-smi) ==="

    # --- Safe fields: work on all driver versions ---
    local SAFE="index,gpu_name,serial,uuid"
    SAFE+=",memory.total,memory.free"
    SAFE+=",pcie.link.gen.current,pcie.link.gen.max"
    SAFE+=",pcie.link.width.current,pcie.link.width.max"
    SAFE+=",temperature.gpu,power.draw,power.limit"
    SAFE+=",vbios_version,ecc.mode.current,pci.bus_id"

    # --- Extended fields: ECC counters + retired pages (may fail on 580+) ---
    local EXT=",ecc.errors.corrected.volatile.total"
    EXT+=",ecc.errors.uncorrected.volatile.total"
    EXT+=",ecc.errors.corrected.aggregate.total"
    EXT+=",ecc.errors.uncorrected.aggregate.total"
    EXT+=",retired_pages.single_bit_ecc.count"
    EXT+=",retired_pages.double_bit_ecc.count"

    # Try full query first
    local HAS_EXT="true"
    log "  Trying full field query (safe + ECC/retired pages)..."
    if nvidia-smi --query-gpu="${SAFE}${EXT}" --format=csv,noheader,nounits \
            > "$WORK_DIR/gpu_bulk.csv" 2>"$WORK_DIR/smi_stderr.txt"; then
        local line_count
        line_count=$(wc -l < "$WORK_DIR/gpu_bulk.csv")
        if [[ "$line_count" -gt 0 ]] && head -1 "$WORK_DIR/gpu_bulk.csv" | grep -q "^[0-9]"; then
            log "  Full query OK -- $line_count GPU(s)"
        else
            HAS_EXT="false"
        fi
    else
        HAS_EXT="false"
    fi

    # Fallback to safe-only if full query failed
    if [[ "$HAS_EXT" == "false" ]]; then
        warn "  Extended fields not supported -- falling back to safe fields only"
        nvidia-smi --query-gpu="${SAFE}" --format=csv,noheader,nounits \
            > "$WORK_DIR/gpu_bulk.csv" 2>"$WORK_DIR/smi_stderr.txt"
        if [[ $? -ne 0 ]]; then
            err "  Even safe-only query failed!"
            cat "$WORK_DIR/smi_stderr.txt" >&2
        fi
        log "  Safe query -- $(wc -l < "$WORK_DIR/gpu_bulk.csv") GPU(s)"
    fi

    local gpu_count
    gpu_count=$(wc -l < "$WORK_DIR/gpu_bulk.csv")

    # Sanity check against lspci
    local expected_pci
    expected_pci=$(lspci -n | grep "10de:" | grep -E "030[02]:" | wc -l)
    if [[ "$gpu_count" -lt "$expected_pci" ]]; then
        warn "  Expected $expected_pci GPUs from lspci but got $gpu_count from nvidia-smi"
        cat "$WORK_DIR/gpu_bulk.csv" >&2
        [[ -s "$WORK_DIR/smi_stderr.txt" ]] && cat "$WORK_DIR/smi_stderr.txt" >&2
    fi

    # NUMA count
    local numa_total
    numa_total=$(lscpu | grep "NUMA node(s):" | awk '{print $3}' 2>/dev/null || echo "0")

    #-----------------------------------------------------------------------
    # Parse CSV
    # Safe mode:     16 fields (index through pci.bus_id)
    # Extended mode: 22 fields (safe + 6 ECC/retired fields)
    #-----------------------------------------------------------------------
    local gpus="[]" numa="[]" pcie_ok="true"

    while IFS= read -r line; do
        # Split on comma
        local f_idx f_name f_serial f_uuid f_mtot f_mfree
        local f_pg_c f_pg_m f_pw_c f_pw_m
        local f_temp f_pwr f_plim f_vbios f_ecc f_bus
        local f_ecv="" f_euv="" f_eca="" f_eua="" f_rsb="" f_rdb=""

        if [[ "$HAS_EXT" == "true" ]]; then
            IFS=',' read -r \
                f_idx f_name f_serial f_uuid \
                f_mtot f_mfree \
                f_pg_c f_pg_m f_pw_c f_pw_m \
                f_temp f_pwr f_plim \
                f_vbios f_ecc f_bus \
                f_ecv f_euv f_eca f_eua \
                f_rsb f_rdb \
                <<< "$line"
        else
            IFS=',' read -r \
                f_idx f_name f_serial f_uuid \
                f_mtot f_mfree \
                f_pg_c f_pg_m f_pw_c f_pw_m \
                f_temp f_pwr f_plim \
                f_vbios f_ecc f_bus \
                <<< "$line"
        fi

        # Trim whitespace
        f_idx=$(echo $f_idx);     f_name=$(echo $f_name)
        f_serial=$(echo $f_serial); f_uuid=$(echo $f_uuid)
        f_mtot=$(echo $f_mtot);   f_pg_c=$(echo $f_pg_c)
        f_pg_m=$(echo $f_pg_m);   f_pw_c=$(echo $f_pw_c)
        f_pw_m=$(echo $f_pw_m);   f_temp=$(echo $f_temp)
        f_pwr=$(echo $f_pwr);     f_plim=$(echo $f_plim)
        f_vbios=$(echo $f_vbios); f_ecc=$(echo $f_ecc)
        f_bus=$(echo $f_bus)
        f_ecv=$(echo $f_ecv);     f_euv=$(echo $f_euv)
        f_eca=$(echo $f_eca);     f_eua=$(echo $f_eua)
        f_rsb=$(echo $f_rsb);     f_rdb=$(echo $f_rdb)

        # Skip garbage lines
        if ! [[ "$f_idx" =~ ^[0-9]+$ ]]; then
            warn "  Skipping non-GPU line: $line"
            continue
        fi

        # Memory type from model name
        local mt
        mt=$(guess_mem_type "$f_name")

        # ECC support
        local ecc_sup="false"
        [[ "$f_ecc" == "Enabled" || "$f_ecc" == "Disabled" ]] && ecc_sup="true"

        # Clean ECC counters -> null if N/A, Not Supported, or empty
        local ecv euv eca eua rsb rdb
        for _varname in ecv euv eca eua rsb rdb; do
            local _srcvar="f_${_varname}"
            local _val="${!_srcvar}"
            if [[ -z "$_val" || "$_val" == *"N/A"* || "$_val" == *"Not Supported"* ]]; then
                eval "$_varname=null"
            else
                eval "$_varname=$_val"
            fi
        done

        # PCIe degradation (nvidia-smi gen/width comparison)
        local pdeg="false"
        if [[ "$f_pg_c" != "$f_pg_m" || "$f_pw_c" != "$f_pw_m" ]]; then
            pdeg="true"; pcie_ok="false"
            warn "  GPU $f_idx: PCIe degraded Gen${f_pg_c}x${f_pw_c} (max Gen${f_pg_m}x${f_pw_m})"
        fi

        # NUMA node
        local nn="-1"
        if [[ "$numa_total" -gt 0 ]]; then
            local sb
            sb=$(pci_to_sysfs "$f_bus")
            [[ -f "/sys/bus/pci/devices/${sb}/numa_node" ]] && \
                nn=$(cat "/sys/bus/pci/devices/${sb}/numa_node" 2>/dev/null || echo "-1")
        fi

        # Build GPU JSON
        gpus=$(echo "$gpus" | jq --argjson e "$(jq -n \
            --argjson idx "$f_idx" --arg name "$f_name" --arg serial "$f_serial" \
            --arg uuid "$f_uuid" --arg mtot "$f_mtot" --arg mt "$mt" \
            --arg pgc "$f_pg_c" --arg pgm "$f_pg_m" --arg pwc "$f_pw_c" --arg pwm "$f_pw_m" \
            --argjson pdeg "$pdeg" --arg temp "$f_temp" --arg pwr "$f_pwr" --arg plim "$f_plim" \
            --arg vbios "$f_vbios" --arg ecc "$f_ecc" --argjson esup "$ecc_sup" \
            --arg bus "$f_bus" --argjson nn "$nn" \
            --argjson ecv "$ecv" --argjson euv "$euv" \
            --argjson eca "$eca" --argjson eua "$eua" \
            --argjson rsb "$rsb" --argjson rdb "$rdb" \
            '{
                gpu_index:$idx, name:$name, serial:$serial, uuid:$uuid,
                vram_mib:($mtot|tonumber), vram_type:$mt,
                pcie_gen_current:($pgc|tonumber), pcie_gen_max:($pgm|tonumber),
                pcie_width_current:($pwc|tonumber), pcie_width_max:($pwm|tonumber),
                pcie_degraded:$pdeg,
                temp_idle_c:($temp|tonumber), power_draw_w:($pwr|tonumber),
                power_limit_w:($plim|tonumber), vbios_version:$vbios,
                ecc_mode:$ecc, ecc_supported:$esup, pci_bus_id:$bus, numa_node:$nn,
                ecc:{
                    corrected_volatile:$ecv, uncorrected_volatile:$euv,
                    corrected_aggregate:$eca, uncorrected_aggregate:$eua,
                    retired_pages_sbit:$rsb, retired_pages_dbit:$rdb
                }
            }')" '. + [$e]')

        # Build NUMA JSON
        numa=$(echo "$numa" | jq --argjson i "$f_idx" --arg b "$f_bus" --argjson n "$nn" \
            '. + [{gpu_index:$i, pci_bus:$b, numa_node:$n}]')

        log "  GPU $f_idx: $f_name ($f_serial) ${f_mtot}MiB $mt Gen${f_pg_c}x${f_pw_c} NUMA:$nn"

    done < "$WORK_DIR/gpu_bulk.csv"

    echo "$gpus" > "$WORK_DIR/gpus.json"

    local numa_avail="false" numa_used=0
    if [[ "$numa_total" -gt 0 ]]; then
        numa_avail="true"
        numa_used=$(echo "$numa" | jq '[.[].numa_node] | unique | length')
    fi
    jq -n --argjson a "$numa_avail" --argjson m "$numa" \
        --argjson u "$numa_used" --argjson t "${numa_total:-0}" \
        '{numa_available:$a, gpu_to_numa_mapping:$m, numa_nodes_used:$u, numa_nodes_total:$t}' \
        > "$WORK_DIR/numa.json"

    log "Done: $gpu_count GPUs, ECC fields:$HAS_EXT, NUMA:$numa_used/$numa_total"
}

###############################################################################
# ASSEMBLE & OUTPUT
###############################################################################
assemble_report() {
    log "=== Assembling report ==="

    local test_end dur overall issues
    test_end=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
    dur=$(( $(date +%s) - SCRIPT_START ))
    overall="PASS"
    issues="[]"

    # GPU count check
    local expected actual
    expected=$(lspci -n | grep "10de:" | grep -E "030[02]:" | wc -l)
    expected=$((${expected:-0} + 0))
    actual=$(jq 'length' "$WORK_DIR/gpus.json" 2>/dev/null || echo "0")
    if [[ "$actual" -lt "$expected" ]]; then
        [[ "$overall" != "FAIL" ]] && overall="WARN"
        issues=$(echo "$issues" | jq --arg m "Expected $expected GPUs but driver sees $actual" '. + [{"issue":$m}]')
    fi

    # ECC errors (only check if fields were available)
    local ecc_fail
    ecc_fail=$(jq '[.[] | select(.ecc_supported==true and (
        (.ecc.uncorrected_aggregate!=null and .ecc.uncorrected_aggregate>0) or
        (.ecc.retired_pages_dbit!=null and .ecc.retired_pages_dbit>0)
    ))] | length' "$WORK_DIR/gpus.json")
    if [[ "$ecc_fail" -gt 0 ]]; then
        overall="FAIL"
        issues=$(echo "$issues" | jq --argjson n "$ecc_fail" \
            '. + [{"issue":("\($n) GPU(s) with ECC uncorrectable errors or double-bit retired pages"),"severity":"critical"}]')
    fi

    # ECC unavailable note
    local ecc_na
    ecc_na=$(jq '[.[] | select(.ecc_supported==true and .ecc.corrected_aggregate==null)] | length' "$WORK_DIR/gpus.json")
    [[ "$ecc_na" -gt 0 ]] && issues=$(echo "$issues" | jq --argjson n "$ecc_na" \
        '. + [{"issue":("\($n) GPU(s) ECC enabled but counters unavailable (driver may not support query)"),"severity":"info"}]')

    # PCIe degradation (from per-GPU nvidia-smi data)
    local pcie_deg
    pcie_deg=$(jq '[.[] | select(.pcie_degraded==true)] | length' "$WORK_DIR/gpus.json")
    if [[ "$pcie_deg" -gt 0 ]]; then
        [[ "$overall" != "FAIL" ]] && overall="WARN"
        issues=$(echo "$issues" | jq --argjson n "$pcie_deg" \
            '. + [{"issue":("\($n) GPU(s) with PCIe link degradation"),"severity":"warning"}]')
    fi

    local hash
    hash=$(cat "$WORK_DIR"/*.json 2>/dev/null | sha256sum | awk '{print $1}')

    jq -n \
        --arg ver "$SCRIPT_VERSION" --arg name "gpu-inventory" \
        --arg ts "$test_end" --argjson dur "$dur" \
        --arg verdict "$overall" --argjson issues "$issues" \
        --arg hash "$hash" \
        --argjson expected "$expected" --argjson actual "$actual" \
        --slurpfile sys "$WORK_DIR/system.json" \
        --slurpfile gpus "$WORK_DIR/gpus.json" \
        --slurpfile numa "$WORK_DIR/numa.json" \
        '{
            report_metadata:{
                script_version:$ver, script_name:$name, generated_at:$ts,
                test_duration_seconds:$dur, data_hash_sha256:$hash,
                gpu_count_expected:$expected, gpu_count_visible:$actual
            },
            verdict:{overall:$verdict, issues:$issues},
            system:$sys[0], gpus:$gpus[0],
            numa_topology:$numa[0]
        }'

    log "=== INVENTORY COMPLETE -- Verdict: $overall ==="
}

###############################################################################
# MAIN
###############################################################################
main() {
    SCRIPT_START=$(date +%s)

    log "=========================================="
    log "NexGen GPU Inventory v${SCRIPT_VERSION}"
    log "=========================================="

    preflight
    collect_system_context
    collect_gpu_data
    assemble_report

    rm -rf "$WORK_DIR"

    log "=========================================="
    log "Inventory complete. Total time: $(( $(date +%s) - SCRIPT_START ))s"
    log "=========================================="
}

main "$@"
