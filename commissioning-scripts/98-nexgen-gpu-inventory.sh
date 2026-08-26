#!/bin/bash
# --- Start MAAS Metadata ---
# name: 98-nexgen-gpu-inventory
# title: NexGen GPU Inventory & Health Check
# description: Collects GPU inventory (serials, UUIDs, VRAM, ECC counters,
#   PCIe link status, NUMA topology) using a single bulk nvidia-smi query.
#   No packages installed -- requires 90-nexgen-gpu-install to run first.
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
SCRIPT_VERSION="2.2.0"

###############################################################################
# LOGGING
###############################################################################
log()  { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [INFO]  $*" >&2; }
warn() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [WARN]  $*" >&2; }
err()  { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [ERROR] $*" >&2; }

mkdir -p "$WORK_DIR"

safe_run() { local o; if o=$("$@" 2>/dev/null); then echo "$o"; else echo ""; fi; }

###############################################################################
# HELPERS: nvidia-smi -q text parsing + JSON value coercion
###############################################################################
# Extract the indented body beneath a heading, stopping when indentation
# returns to the heading's own level.  nvidia-smi -q reuses field names across
# sections ("Pending" under both ECC Mode and Remapped Rows; "Correctable
# Error" under both DRAM and the row remapper), so fields must be read from
# within their own section rather than grepped out of the whole document.
smi_section() {
    awk -v want="$1" '
        !found && index($0, want) { match($0, /^ */); ind = RLENGTH; found = 1; next }
        found {
            if ($0 ~ /^[[:space:]]*$/) next
            match($0, /^ */)
            if (RLENGTH <= ind) exit
            print
        }
    '
}

# Read a single "Label : value" field from stdin, matching the label exactly
# after trimming.  Exact match matters: "SRAM Correctable" must not be
# satisfied by "SRAM Correctable Parity".
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

# Coerce an nvidia-smi value to a JSON number, or null.  Strips units and
# thousands separators, so "640 bank(s)" -> 640 and "N/A" -> null.
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

# Sum two possibly-absent integers.  Empty only when BOTH are absent, so a
# single reported half is never silently dropped to zero.
sum_or_empty() {
    local a b
    a=$(printf '%s' "${1:-}" | grep -oE '[0-9]+' | head -1)
    b=$(printf '%s' "${2:-}" | grep -oE '[0-9]+' | head -1)
    if [[ -z "$a" && -z "$b" ]]; then echo ""; else echo $(( ${a:-0} + ${b:-0} )); fi
}

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
        err "Missing required tools:$missing -- run 90-nexgen-gpu-install first"
        jq -n --arg v "$SCRIPT_VERSION" --arg m "Missing:$missing" '{
            report_metadata:{script_version:$v, script_name:"gpu-inventory"},
            verdict:{overall:"FAIL", issues:[{"issue":$m}]}
        }' 2>/dev/null || echo '{"verdict":{"overall":"FAIL"}}'
        exit 1
    fi

    if ! nvidia-smi &>/dev/null; then
        err "nvidia-smi not functional -- run 90-nexgen-gpu-install first"
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

        # PCIe degradation — only flag width mismatch (real hardware issue).
        # Gen dropping at idle (e.g. Gen4 -> Gen2) is normal power saving.
        local pdeg="false"
        if [[ "$f_pw_c" != "$f_pw_m" ]]; then
            pdeg="true"; pcie_ok="false"
            warn "  GPU $f_idx: PCIe width degraded x${f_pw_c} (max x${f_pw_m})"
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

    #-----------------------------------------------------------------------
    # ROW REMAPPER + ECC -- always collected, via nvidia-smi -q text.
    #
    # This runs UNCONDITIONALLY.  It used to be gated on the --query-gpu
    # extended fields having *failed*, which meant that on a healthy modern
    # driver -- where that query succeeds -- remapped_rows,
    # bank_remap_availability and sram_threshold_exceeded were never emitted
    # at all, and the RMA checks in assemble_report silently evaluated
    # missing keys and passed.  The -q text is the authoritative source for
    # this data, so we always read it.
    #-----------------------------------------------------------------------
    log "  Collecting row remapper + ECC via nvidia-smi -q (per GPU)..."
    local gpu_indices gi
    gpu_indices=$(jq -r '.[].gpu_index' "$WORK_DIR/gpus.json")

    for gi in $gpu_indices; do
        local ecc_out
        ecc_out=$(nvidia-smi -i "$gi" -q -d ROW_REMAPPER,ECC 2>/dev/null || true)
        # Drivers predating the ROW_REMAPPER section name, then last-resort full -q
        [[ -z "$ecc_out" ]] && ecc_out=$(nvidia-smi -i "$gi" -q -d ECC 2>/dev/null || true)
        [[ -z "$ecc_out" ]] && ecc_out=$(nvidia-smi -i "$gi" -q 2>/dev/null || true)

        if [[ -z "$ecc_out" ]]; then
            warn "  GPU $gi: nvidia-smi -q returned nothing -- row remapper/ECC unavailable"
            continue
        fi

        # --- Section-aware extraction ------------------------------------
        # "Pending" appears under BOTH "ECC Mode" and "Remapped Rows", and
        # "Correctable Error" under both DRAM and the remapper.  Every field
        # is therefore read from inside its own section rather than by
        # grepping the whole document.
        local ecc_errors vol agg remap histo
        ecc_errors=$(printf '%s\n' "$ecc_out"    | smi_section "ECC Errors")
        vol=$(printf        '%s\n' "$ecc_errors" | smi_section "Volatile")
        agg=$(printf        '%s\n' "$ecc_errors" | smi_section "Aggregate")
        remap=$(printf      '%s\n' "$ecc_out"    | smi_section "Remapped Rows")
        histo=$(printf      '%s\n' "$remap"      | smi_section "Bank Remap Availability Histogram")

        local v_dram_ce v_dram_uce v_sram_ce v_sram_uce
        v_dram_ce=$(printf  '%s\n' "$vol" | smi_field "DRAM Correctable")
        v_dram_uce=$(printf '%s\n' "$vol" | smi_field "DRAM Uncorrectable")
        v_sram_ce=$(printf  '%s\n' "$vol" | smi_field "SRAM Correctable")
        v_sram_uce=$(printf '%s\n' "$vol" | smi_field "SRAM Uncorrectable")

        local a_dram_ce a_dram_uce a_sram_ce a_sram_uce
        a_dram_ce=$(printf  '%s\n' "$agg" | smi_field "DRAM Correctable")
        a_dram_uce=$(printf '%s\n' "$agg" | smi_field "DRAM Uncorrectable")
        a_sram_ce=$(printf  '%s\n' "$agg" | smi_field "SRAM Correctable")
        a_sram_uce=$(printf '%s\n' "$agg" | smi_field "SRAM Uncorrectable")

        # Hopper+ splits SRAM uncorrectable into Parity and SEC-DED.  When the
        # combined field is absent, sum the two so the count is never
        # understated -- SRAM uncorrectable > 0 is an RMA condition.
        if [[ -z "$a_sram_uce" ]]; then
            a_sram_uce=$(sum_or_empty \
                "$(printf '%s\n' "$agg" | smi_field "SRAM Uncorrectable Parity")" \
                "$(printf '%s\n' "$agg" | smi_field "SRAM Uncorrectable SEC-DED")")
        fi
        if [[ -z "$v_sram_uce" ]]; then
            v_sram_uce=$(sum_or_empty \
                "$(printf '%s\n' "$vol" | smi_field "SRAM Uncorrectable Parity")" \
                "$(printf '%s\n' "$vol" | smi_field "SRAM Uncorrectable SEC-DED")")
        fi

        # Sits under Aggregate on some drivers, directly under ECC Errors on others
        local sram_threshold
        sram_threshold=$(printf '%s\n' "$agg" | smi_field "SRAM Threshold Exceeded")
        [[ -z "$sram_threshold" ]] && \
            sram_threshold=$(printf '%s\n' "$ecc_errors" | smi_field "SRAM Threshold Exceeded")

        local remap_ce remap_uce remap_pending remap_failure
        remap_ce=$(printf      '%s\n' "$remap" | smi_field "Correctable Error")
        remap_uce=$(printf     '%s\n' "$remap" | smi_field "Uncorrectable Error")
        remap_pending=$(printf '%s\n' "$remap" | smi_field "Pending")
        remap_failure=$(printf '%s\n' "$remap" | smi_field "Remapping Failure Occurred")

        # Histogram values read "640 bank(s)" -- to_json_num strips the unit
        local bank_max bank_high bank_partial bank_low bank_none
        bank_max=$(printf     '%s\n' "$histo" | smi_field "Max")
        bank_high=$(printf    '%s\n' "$histo" | smi_field "High")
        bank_partial=$(printf '%s\n' "$histo" | smi_field "Partial")
        bank_low=$(printf     '%s\n' "$histo" | smi_field "Low")
        bank_none=$(printf    '%s\n' "$histo" | smi_field "None")

        # Totals: prefer the --query-gpu value when it was available (it is
        # the driver's own total), else fall back to DRAM+SRAM from the text.
        local t_cv t_uv t_ca t_ua
        t_cv=$(sum_or_empty "$v_dram_ce"  "$v_sram_ce")
        t_uv=$(sum_or_empty "$v_dram_uce" "$v_sram_uce")
        t_ca=$(sum_or_empty "$a_dram_ce"  "$a_sram_ce")
        t_ua=$(sum_or_empty "$a_dram_uce" "$a_sram_uce")

        # Raw evidence, base64 so that braces in the text cannot desync a
        # consumer scanning for the JSON payload.
        local raw_b64
        raw_b64=$(printf '%s\n' "$ecc_out" | base64 2>/dev/null | tr -d '\n')

        gpus=$(jq \
            --argjson gi    "$gi" \
            --argjson vdce  "$(to_json_num "$v_dram_ce")" \
            --argjson vduce "$(to_json_num "$v_dram_uce")" \
            --argjson vsce  "$(to_json_num "$v_sram_ce")" \
            --argjson vsuce "$(to_json_num "$v_sram_uce")" \
            --argjson adce  "$(to_json_num "$a_dram_ce")" \
            --argjson aduce "$(to_json_num "$a_dram_uce")" \
            --argjson asce  "$(to_json_num "$a_sram_ce")" \
            --argjson asuce "$(to_json_num "$a_sram_uce")" \
            --argjson tcv   "$(to_json_num "$t_cv")" \
            --argjson tuv   "$(to_json_num "$t_uv")" \
            --argjson tca   "$(to_json_num "$t_ca")" \
            --argjson tua   "$(to_json_num "$t_ua")" \
            --argjson rce   "$(to_json_num "$remap_ce")" \
            --argjson rue   "$(to_json_num "$remap_uce")" \
            --argjson rp    "$(to_json_bool "$remap_pending")" \
            --argjson rf    "$(to_json_bool "$remap_failure")" \
            --argjson st    "$(to_json_bool "$sram_threshold")" \
            --argjson bmax  "$(to_json_num "$bank_max")" \
            --argjson bhigh "$(to_json_num "$bank_high")" \
            --argjson bpart "$(to_json_num "$bank_partial")" \
            --argjson blow  "$(to_json_num "$bank_low")" \
            --argjson bnone "$(to_json_num "$bank_none")" \
            --arg     raw   "$raw_b64" \
            '[.[] | if .gpu_index == $gi then
                .ecc = (.ecc + {
                    dram_corrected_volatile:    $vdce,
                    dram_uncorrected_volatile:  $vduce,
                    sram_corrected_volatile:    $vsce,
                    sram_uncorrected_volatile:  $vsuce,
                    dram_corrected_aggregate:   $adce,
                    dram_uncorrected_aggregate: $aduce,
                    sram_corrected_aggregate:   $asce,
                    sram_uncorrected_aggregate: $asuce
                })
                | .ecc.corrected_volatile     = (.ecc.corrected_volatile     // $tcv)
                | .ecc.uncorrected_volatile   = (.ecc.uncorrected_volatile   // $tuv)
                | .ecc.corrected_aggregate    = (.ecc.corrected_aggregate    // $tca)
                | .ecc.uncorrected_aggregate  = (.ecc.uncorrected_aggregate  // $tua)
                | .remapped_rows = {
                    correctable:      $rce,
                    uncorrectable:    $rue,
                    pending:          $rp,
                    failure_occurred: $rf
                }
                | .sram_threshold_exceeded = $st
                | .bank_remap_availability = {
                    max:     $bmax,
                    high:    $bhigh,
                    partial: $bpart,
                    low:     $blow,
                    none:    $bnone
                }
                | .raw_evidence = {row_remapper_ecc_b64: $raw}
            else . end]' "$WORK_DIR/gpus.json")
        echo "$gpus" > "$WORK_DIR/gpus.json"

        # Log summary
        local ecc_summary="aggCE:${a_dram_ce:-n/a} aggUCE:${a_dram_uce:-n/a}"
        [[ -n "$a_sram_uce" && "$a_sram_uce" != "0" ]] && ecc_summary+=" sramUCE:${a_sram_uce}"
        [[ -n "$remap_uce"  && "$remap_uce"  != "0" ]] && ecc_summary+=" remapUCE:${remap_uce}"
        [[ -n "$remap_ce"   && "$remap_ce"   != "0" ]] && ecc_summary+=" remapCE:${remap_ce}"
        [[ "$remap_pending"  == "Yes" ]] && ecc_summary+=" REMAP_PENDING!"
        [[ "$remap_failure"  == "Yes" ]] && ecc_summary+=" REMAP_FAILURE!"
        [[ "$sram_threshold" == "Yes" ]] && ecc_summary+=" SRAM_THRESHOLD!"
        log "    GPU $gi: $ecc_summary"
    done
    gpus=$(cat "$WORK_DIR/gpus.json")

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
# NVIDIA-SMI VERBOSE DUMP (logged to stderr for MAAS commissioning log)
###############################################################################
dump_smi_verbose() {
    log "=== nvidia-smi verbose dump (copy from commissioning log) ==="

    log "---------- nvidia-smi ----------"
    nvidia-smi >&2 2>&1 || warn "nvidia-smi failed"

    log "---------- nvidia-smi -q ----------"
    nvidia-smi -q >&2 2>&1 || warn "nvidia-smi -q failed"

    log "---------- nvidia-smi topo -m ----------"
    nvidia-smi topo -m >&2 2>&1 || warn "nvidia-smi topo -m failed"

    log "---------- end nvidia-smi dump ----------"
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

    # Remapping failure check (NVIDIA RMA condition)
    local remap_fail
    remap_fail=$(jq '[.[] | select(.remapped_rows.failure_occurred==true)] | length' "$WORK_DIR/gpus.json" 2>/dev/null || echo "0")
    if [[ "$remap_fail" -gt 0 ]]; then
        overall="FAIL"
        issues=$(echo "$issues" | jq --argjson n "$remap_fail" \
            '. + [{"issue":("\($n) GPU(s) with row remapping failure (RMA eligible)"),"severity":"critical"}]')
    fi

    # SRAM threshold exceeded check (NVIDIA RMA condition)
    local sram_fail
    sram_fail=$(jq '[.[] | select(.sram_threshold_exceeded==true)] | length' "$WORK_DIR/gpus.json" 2>/dev/null || echo "0")
    if [[ "$sram_fail" -gt 0 ]]; then
        overall="FAIL"
        issues=$(echo "$issues" | jq --argjson n "$sram_fail" \
            '. + [{"issue":("\($n) GPU(s) with SRAM threshold exceeded (RMA eligible)"),"severity":"critical"}]')
    fi

    # Remapped rows with UCEs (not yet at failure but degrading)
    local remap_uce_warn
    remap_uce_warn=$(jq '[.[] | select(.remapped_rows.uncorrectable!=null and .remapped_rows.uncorrectable>0 and .remapped_rows.failure_occurred!=true)] | length' "$WORK_DIR/gpus.json" 2>/dev/null || echo "0")
    if [[ "$remap_uce_warn" -gt 0 ]]; then
        [[ "$overall" == "PASS" ]] && overall="WARN"
        issues=$(echo "$issues" | jq --argjson n "$remap_uce_warn" \
            '. + [{"issue":("\($n) GPU(s) with UCE remapped rows (degrading, not yet at failure threshold)"),"severity":"warning"}]')
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

    [[ "$overall" == "FAIL" ]] && return 1
    return 0
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
    dump_smi_verbose
    collect_system_context
    collect_gpu_data
    local inventory_ok=true
    if ! assemble_report; then
        inventory_ok=false
    fi

    rm -rf "$WORK_DIR"

    log "=========================================="
    log "Inventory complete. Total time: $(( $(date +%s) - SCRIPT_START ))s"
    log "=========================================="

    if [[ "$inventory_ok" == "false" ]]; then
        exit 1
    fi
}

main "$@"
