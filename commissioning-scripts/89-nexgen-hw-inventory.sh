#!/bin/bash
# --- Start MAAS Metadata ---
# name: 89-nexgen-hw-inventory
# title: NexGen Chassis & Component Inventory
# description: Collects the chassis-level facts the GPU scripts do not: BMC
#   firmware revision and power-on hours over the in-band IPMI interface, PSU
#   inventory from SMBIOS type 39, per-drive model/serial/firmware/power-on
#   hours including drives hidden behind a MegaRAID controller, and add-in card
#   serial numbers from PCI VPD. Runs first, before 90 installs anything, so a
#   run that never reaches the GPU phases still yields a component inventory.
#   Installs its own tools. No GPU dependency. Outputs structured JSON.
# script_type: commissioning
# hardware_type: node
# timeout: 00:10:00
# destructive: false
# may_reboot: false
# --- End MAAS Metadata ---

set -o pipefail
trap 'warn "Command failed at line $LINENO (exit code $?)"' ERR

###############################################################################
# CONFIG
###############################################################################
WORK_DIR="/tmp/hw-inventory-$$"
SCRIPT_VERSION="1.5.0"

# How many MegaRAID device slots to probe behind each controller. The members
# are not enumerated anywhere we can read, so they are found by trying.
MEGARAID_MAX_DID="${MEGARAID_MAX_DID:-32}"

# Packages this script needs. Installed up front rather than assumed: the MAAS
# ephemeral image carries dmidecode and pciutils, and MAAS's own
# 30-maas-01-bmc-config pulls in freeipmi, but smartmontools, nvme-cli,
# ipmitool and jq are not guaranteed to be there.
REQUIRED_PKGS="${REQUIRED_PKGS:-ipmitool smartmontools nvme-cli dmidecode pciutils jq}"

###############################################################################
# LOGGING
###############################################################################
log()  { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [INFO]  $*" >&2; }
warn() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [WARN]  $*" >&2; }
err()  { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [ERROR] $*" >&2; }

mkdir -p "$WORK_DIR"

safe_run() { local o; if o=$("$@" 2>/dev/null); then echo "$o"; else echo ""; fi; }

# Emit a JSON string, or null for the empty string. Every field here is
# best-effort, and "" and "not collected" are different claims -- a blank
# serial in the Server Report reads as "this part has no serial", which is a
# statement of fact we cannot make.
jstr() { [[ -z "$1" ]] && echo "null" || jq -n --arg v "$1" '$v'; }
jnum() { [[ "$1" =~ ^-?[0-9]+$ ]] && echo "$1" || echo "null"; }

# The awk parsers below build their objects with printf, so an absent field
# arrives as "". Fold those to null in place, for the same reason jstr does.
normalise_blanks() {
    local f="$1" tmp="$1.tmp"
    if jq 'map(map_values(if . == "" then null else . end))' "$f" > "$tmp" 2>/dev/null; then
        mv "$tmp" "$f"
    else
        rm -f "$tmp"
    fi
}

# No halt_gate here on purpose. The marker is written by 91, and 89 sorts
# ahead of it, so there is nothing to gate on -- this script has always
# already run by the time an ECC-disabled run is declared void.

###############################################################################
# PACKAGES
###############################################################################
install_packages() {
    log "=== Installing inventory tools ==="
    export DEBIAN_FRONTEND=noninteractive

    apt-get update -qq 2>&1 | tail -3 >&2 || warn "apt-get update failed, trying the install anyway"
    # shellcheck disable=SC2086
    apt-get install -y -qq $REQUIRED_PKGS 2>&1 | tail -5 >&2 \
        || warn "apt-get install returned non-zero; checking tools individually"

    local tool missing=""
    for tool in ipmitool smartctl dmidecode lspci jq; do
        if command -v "$tool" >/dev/null 2>&1; then
            log "  $tool: $(command -v "$tool")"
        else
            warn "  $tool: NOT AVAILABLE -- the fields it feeds will be null"
            missing="$missing $tool"
        fi
    done

    # jq assembles the report. Without it there is no deliverable at all, and
    # emitting nothing is better than emitting a half-formed object that the
    # generator would read as collected-and-empty.
    if ! command -v jq >/dev/null 2>&1; then
        err "jq is unavailable -- cannot assemble the report"
        return 1
    fi
    TOOLS_MISSING="${missing# }"
    return 0
}

###############################################################################
# BMC -- in-band, no credentials
###############################################################################
# This talks to the BMC over the host-side KCS interface via /dev/ipmi0, not
# over the network. There is no username, password or Redfish session
# involved: the authorisation is being root on the machine the BMC is bolted
# to. That is the same path MAAS's own 30-maas-01-bmc-config takes, which is
# why it can configure the BMC on a node it has no credentials for.
collect_bmc() {
    log "=== BMC (in-band IPMI) ==="

    modprobe ipmi_devintf  2>/dev/null || true
    modprobe ipmi_si       2>/dev/null || true
    modprobe ipmi_msghandler 2>/dev/null || true

    if ! command -v ipmitool >/dev/null 2>&1; then
        warn "ipmitool unavailable -- skipping BMC collection"
        echo '{"available":false,"reason":"ipmitool not installed"}' > "$WORK_DIR/bmc.json"
        return 0
    fi

    local mc_info
    mc_info=$(safe_run ipmitool mc info)
    echo "$mc_info" > "$WORK_DIR/mc-info.txt"

    if [[ -z "$mc_info" ]]; then
        warn "ipmitool mc info returned nothing -- no in-band BMC interface?"
        echo '{"available":false,"reason":"no response from in-band IPMI"}' > "$WORK_DIR/bmc.json"
        return 0
    fi

    log "---------- ipmitool mc info ----------"
    echo "$mc_info" >&2
    log "---------- end ipmitool mc info ----------"

    local fw aux dev mfr prod ipmiver
    fw=$(echo "$mc_info"      | sed -n 's/^ *Firmware Revision *: *//p' | head -1)
    aux=$(echo "$mc_info"     | sed -n 's/^ *Aux Firmware Rev Info *: *//p' | head -1)
    dev=$(echo "$mc_info"     | sed -n 's/^ *Device ID *: *//p' | head -1)
    mfr=$(echo "$mc_info"     | sed -n 's/^ *Manufacturer Name *: *//p' | head -1)
    prod=$(echo "$mc_info"    | sed -n 's/^ *Product Name *: *//p' | head -1)
    ipmiver=$(echo "$mc_info" | sed -n 's/^ *IPMI Version *: *//p' | head -1)

    log "  BMC firmware revision: ${fw:-unknown}"
    log "  BMC manufacturer:      ${mfr:-unknown}"

    jq -n \
        --argjson fw "$(jstr "$fw")" \
        --argjson aux "$(jstr "$aux")" \
        --argjson dev "$(jstr "$dev")" \
        --argjson mfr "$(jstr "$mfr")" \
        --argjson prod "$(jstr "$prod")" \
        --argjson ver "$(jstr "$ipmiver")" \
        '{available:true, firmware_revision:$fw, aux_firmware_revision:$aux,
          device_id:$dev, manufacturer:$mfr, product_name:$prod,
          ipmi_version:$ver, access:"in-band KCS (/dev/ipmi0), no credentials"}' \
        > "$WORK_DIR/bmc.json"
}

###############################################################################
# FRU -- the board manufacture date, which SMBIOS does not carry
###############################################################################
# dmidecode gives the BIOS release date, not when the board was built. The FRU
# inventory area does carry a real "Board Mfg Date", and on these ASUS boards
# it is populated. That is the closest thing to an age for a machine whose BMC
# exposes no power-on-hours sensor, so it is worth having in the report.
collect_fru() {
    log "=== FRU inventory ==="

    if ! command -v ipmitool >/dev/null 2>&1; then
        echo '{"available":false}' > "$WORK_DIR/fru.json"
        return 0
    fi

    local fru
    fru=$(safe_run ipmitool fru print 0)
    [[ -z "$fru" ]] && fru=$(safe_run ipmitool fru print)

    if [[ -z "$fru" ]]; then
        warn "ipmitool fru print returned nothing"
        echo '{"available":false}' > "$WORK_DIR/fru.json"
        return 0
    fi

    log "---------- ipmitool fru print ----------"
    echo "$fru" >&2
    log "---------- end ipmitool fru print ----------"

    fru_field() { echo "$fru" | sed -n "s/^ *$1 *: *//p" | head -1; }

    local bdate bser bpart bprod cser cpart pser pname pver
    bdate=$(fru_field "Board Mfg Date")
    bser=$(fru_field "Board Serial")
    bpart=$(fru_field "Board Part Number")
    bprod=$(fru_field "Board Product")
    cser=$(fru_field "Chassis Serial")
    cpart=$(fru_field "Chassis Part Number")
    pser=$(fru_field "Product Serial")
    pname=$(fru_field "Product Name")
    pver=$(fru_field "Product Version")

    log "  Board manufactured: ${bdate:-not recorded}"
    log "  Product serial:     ${pser:-unknown}"

    jq -n \
        --argjson a "$(jstr "$bdate")" --argjson b "$(jstr "$bser")" \
        --argjson c "$(jstr "$bpart")" --argjson d "$(jstr "$bprod")" \
        --argjson e "$(jstr "$cser")"  --argjson f "$(jstr "$cpart")" \
        --argjson g "$(jstr "$pser")"  --argjson h "$(jstr "$pname")" \
        --argjson i "$(jstr "$pver")" \
        '{available:true, board_mfg_date:$a, board_serial:$b,
          board_part_number:$c, board_product:$d, chassis_serial:$e,
          chassis_part_number:$f, product_serial:$g, product_name:$h,
          product_version:$i}' \
        > "$WORK_DIR/fru.json"
}

###############################################################################
# POWER-ON HOURS
###############################################################################
# Best effort, and honest about which it got. There is no standard IPMI sensor
# for chassis power-on hours -- some BMCs expose one, most do not, and the OEM
# raw commands that read it differ per vendor and are not worth guessing at on
# a machine that is about to be sold. So: report the sensor if one exists, and
# otherwise report the drives' own hours, which are a real measurement of the
# same chassis and are what Schedule 4 paragraph 1(f) asks for per drive
# anyway. The report should render "source" alongside the number.
collect_power_on_hours() {
    log "=== Power-on hours ==="

    local hours="" source="" sensor_dump=""

    if command -v ipmitool >/dev/null 2>&1; then
        sensor_dump=$(safe_run ipmitool sdr elist full)
        echo "$sensor_dump" > "$WORK_DIR/sdr.txt"

        # Match a sensor whose name mentions hours or uptime, and take the
        # first integer out of its reading.
        local line
        line=$(echo "$sensor_dump" | grep -iE '(power.?on.?(hour|time)|operating.?hour|uptime|total.?hour)' | head -1)
        if [[ -n "$line" ]]; then
            hours=$(echo "$line" | grep -oE '[0-9]+' | tail -1)
            [[ -n "$hours" ]] && source="ipmi_sdr_sensor"
            log "  IPMI sensor: $line"
        fi
    fi

    if [[ -z "$hours" ]]; then
        log "  No IPMI power-on-hours sensor found on this BMC"
        log "  Drive power-on hours are collected per drive instead -- see .drives[]"
        source=""
    fi

    # The System Event Log is the one age record the BMC does keep. Its first
    # entry is normally the machine's first power-up, so the span between that
    # and the BMC clock bounds how long the chassis has been in service. It is
    # elapsed time, not powered time, so it is reported as its own field and
    # never as power-on hours -- but it is real evidence and cheap to take.
    local sel_first="" sel_now="" sel_days=""
    if command -v ipmitool >/dev/null 2>&1; then
        sel_now=$(safe_run ipmitool sel time get | head -1)
        sel_first=$(safe_run ipmitool sel elist first 1 \
                    | awk -F'|' 'NR==1 {gsub(/^[ \t]+|[ \t]+$/,"",$2); print $2}')
        if [[ -n "$sel_first" && -n "$sel_now" ]]; then
            local t0 t1
            t0=$(date -d "$sel_first" +%s 2>/dev/null || echo "")
            t1=$(date -d "${sel_now%% *}" +%s 2>/dev/null || echo "")
            if [[ -n "$t0" && -n "$t1" && "$t1" -gt "$t0" ]]; then
                sel_days=$(( (t1 - t0) / 86400 ))
                log "  SEL spans $sel_days day(s): first entry $sel_first, BMC clock $sel_now"
            fi
        fi
    fi

    jq -n \
        --argjson h "$(jnum "$hours")" \
        --argjson s "$(jstr "$source")" \
        --argjson f "$(jstr "$sel_first")" \
        --argjson n "$(jstr "$sel_now")" \
        --argjson d "$(jnum "$sel_days")" \
        '{hours:$h, source:$s,
          sel_first_entry:$f, sel_clock_now:$n, sel_span_days:$d,
          note:(if $h == null
                then "No chassis power-on-hours sensor is exposed by this BMC over IPMI (confirmed on the ASUS ASMB11: sdr elist full carries temperature, voltage, fan and PSU wattage only). Use the per-drive power_on_hours in .drives[] as the evidenced figure. sel_span_days is elapsed time since the first logged event, not powered hours."
                else null end)}' \
        > "$WORK_DIR/poh.json"
}

###############################################################################
# PSUs -- SMBIOS type 39
###############################################################################
collect_psus() {
    log "=== Power supplies (SMBIOS type 39) ==="

    if ! command -v dmidecode >/dev/null 2>&1; then
        warn "dmidecode unavailable -- skipping PSU collection"
        echo '[]' > "$WORK_DIR/psus.json"
        echo '{"bays":null,"labels":[]}' > "$WORK_DIR/psu_sensors.json"
        echo 'null' > "$WORK_DIR/chassis_idle.json"
        return 0
    fi

    local dump
    dump=$(safe_run dmidecode -t 39)
    echo "$dump" > "$WORK_DIR/dmidecode-39.txt"

    log "---------- dmidecode -t 39 ----------"
    echo "$dump" >&2
    log "---------- end dmidecode -t 39 ----------"

    # One object per "System Power Supply" block. Bays that report themselves
    # as not present are kept with present:false -- an empty bay is a fact the
    # Server Report needs, not a row to drop.
    # flush() runs on the blank line between blocks AND in END: real
    # dmidecode output ends straight after the last block with no trailing
    # blank line, so an END-less version silently drops the final PSU.
    echo "$dump" | awk '
        BEGIN { print "["; n = 0 }
        /^System Power Supply/ { flush(); inblk = 1; loc=""; name=""; mfr=""; ser=""; mdl=""; rev=""; maxw=""; st=""; pres="true"; next }
        inblk && /^$/ { flush(); next }
        inblk {
            line = $0
            sub(/^[ \t]+/, "", line)
            p = index(line, ":")
            if (p == 0) next
            k = substr(line, 1, p-1); v = substr(line, p+1)
            sub(/^[ \t]+/, "", v); sub(/[ \t]+$/, "", v)
            gsub(/"/, "", v)
            if (k == "Location")             loc  = v
            else if (k == "Name")            name = v
            else if (k == "Manufacturer")    mfr  = v
            else if (k == "Serial Number")   ser  = v
            else if (k == "Model Part Number") mdl = v
            else if (k == "Revision")        rev  = v
            else if (k == "Max Power Capacity") maxw = v
            else if (k == "Status")          { st = v; if (v ~ /Not Present/) pres = "false" }
        }
        END { flush(); print "]" }
        function flush() {
            if (!inblk) return
            inblk = 0
            if (loc == "" && name == "" && mfr == "") return
            if (n++) print ","
            printf "{\"location\":\"%s\",\"name\":\"%s\",\"manufacturer\":\"%s\",", loc, name, mfr
            printf "\"serial_number\":\"%s\",\"model\":\"%s\",\"revision\":\"%s\",", ser, mdl, rev
            printf "\"max_power_watts\":\"%s\",\"status\":\"%s\",\"present\":%s}", maxw, st, pres
        }
    ' > "$WORK_DIR/psus.json" 2>/dev/null || echo '[]' > "$WORK_DIR/psus.json"

    if jq -e . "$WORK_DIR/psus.json" >/dev/null 2>&1; then
        normalise_blanks "$WORK_DIR/psus.json"
    else
        warn "PSU block did not parse into valid JSON"
        echo '[]' > "$WORK_DIR/psus.json"
    fi
    log "  PSU entries: $(jq 'length' "$WORK_DIR/psus.json")"

    # Second, independent count from the BMC. SMBIOS type 39 is written by the
    # BIOS and has been seen to under-report: a chassis with four PSUs fitted
    # recorded only two type-39 entries, so a DMI-only count would have
    # understated the Server Report. The BMC exposes a sensor pair per bay
    # regardless, so counting distinct PSU sensors cross-checks the DMI table.
    # Serial numbers still come only from DMI -- the sensors prove presence,
    # not identity -- so a disagreement means bays exist whose serials cannot
    # be read here and have to come off the label.
    local labels bays=0
    if [[ -s "$WORK_DIR/sdr.txt" ]]; then
        labels=$(grep -oiE '^ *PSU *[0-9]+' "$WORK_DIR/sdr.txt" 2>/dev/null \
                 | tr -d ' ' | tr 'a-z' 'A-Z' | sort -u | tr '\n' ' ')
        bays=$(echo "$labels" | wc -w)
    fi
    if [[ "$bays" -gt 0 ]]; then
        log "  PSU bays seen by the BMC: $bays ($labels)"
        jq -n --argjson b "$bays" --arg l "${labels% }" \
            '{bays:$b, labels:($l | split(" ") | map(select(length>0)))}' \
            > "$WORK_DIR/psu_sensors.json"
    else
        echo '{"bays":null,"labels":[]}' > "$WORK_DIR/psu_sensors.json"
    fi

    # Idle baseline for the fans and inlet air. Scripts 98 and 99 sample the
    # same sensors under load; comparing the two is what separates "this fan
    # never spins up" from "this fan is dead". Free here -- the SDR has already
    # been read for the power-on-hours lookup.
    if [[ -s "$WORK_DIR/sdr.txt" ]]; then
        awk -F'|' '
            { name = $1; gsub(/^[ \t]+|[ \t]+$/, "", name)
              val  = $5; gsub(/^[ \t]+|[ \t]+$/, "", val)
              if (val ~ /^-?[0-9.]+ /) {
                  n = val; sub(/ .*/, "", n)
                  u = val; sub(/^[^ ]+ /, "", u)
                  if (u ~ /RPM/)          u = "RPM"
                  else if (u ~ /degrees/) u = "C"
                  else if (u ~ /Watts/)   u = "W"
                  else next
                  printf "{\"sensor\":\"%s\",\"unit\":\"%s\",\"value\":%s}\n", name, u, n
              } }' "$WORK_DIR/sdr.txt" 2>/dev/null \
        | jq -s '
            # Grouped by bank, matching scripts 98 and 99. The banks are
            # different fans on different duty curves -- measured at idle, one
            # chassis read 5330-9490 RPM across the two banks, a 44% pooled
            # spread with nothing wrong. What matters here is simply that every
            # fan turns; the load-time samples are what show whether it ramps.
            . as $sensors
            | [ $sensors[] | select(.unit == "RPM")
                | . + {bank: (.sensor | sub("[0-9]+$"; "") | sub("_$"; ""))} ] as $rpm
            | ($rpm | group_by(.bank)
               | map({ bank: .[0].bank, count: length,
                       slowest: (map(.value) | min), fastest: (map(.value) | max),
                       slowest_sensor: (min_by(.value).sensor),
                       spread_pct: (if (map(.value) | max) > 0
                                    then (((map(.value) | max) - (map(.value) | min))
                                          / (map(.value) | max) * 100 | . * 10 | round / 10)
                                    else 0 end) })) as $banks
            | { at: "idle, before any load",
                sensors: $sensors,
                fans: { banks: $banks,
                        fan_count: ($rpm | length),
                        stopped: [ $rpm[] | select(.value == 0) | .sensor ],
                        all_spinning: (([ $rpm[] | select(.value == 0) ] | length) == 0),
                        worst_bank: ($banks | if length > 0 then (max_by(.spread_pct) | .bank) else null end),
                        worst_spread_pct: ($banks | map(.spread_pct) | max // 0) },
                inlet_c: ($sensors | map(select(.sensor | test("Inlet"; "i"))) | .[0].value // null) }' \
            > "$WORK_DIR/chassis_idle.json" 2>/dev/null \
            || echo 'null' > "$WORK_DIR/chassis_idle.json"
        log "  Idle fans: $(jq -r '.fans.fan_count // 0' "$WORK_DIR/chassis_idle.json") sensor(s), "\
            "spread $(jq -r '.fans.spread_pct // "?"' "$WORK_DIR/chassis_idle.json")%, "\
            "inlet $(jq -r '.inlet_c // "?"' "$WORK_DIR/chassis_idle.json")C"
    else
        echo 'null' > "$WORK_DIR/chassis_idle.json"
    fi

    local dmi_n
    dmi_n=$(jq '[.[] | select(.present)] | length' "$WORK_DIR/psus.json")
    if [[ "$bays" -gt 0 && "$dmi_n" -gt 0 && "$bays" -ne "$dmi_n" ]]; then
        warn "  PSU count disagrees: SMBIOS records $dmi_n, BMC sees $bays bays"
        warn "  The BMC count is the presence evidence; DMI is the only serial source"
    fi
}

###############################################################################
# DRIVES -- including members hidden behind a MegaRAID controller
###############################################################################
# A hardware RAID volume presents one SCSI device to the OS and hides the
# member drives, so the make, model, serial and power-on hours of the drives
# actually being sold are invisible to lsblk, lshw and MAAS alike. smartctl's
# megaraid passthrough reads them directly, which avoids needing storcli --
# storcli is not in the Ubuntu archive and would have to be fetched from
# Broadcom at commissioning time.
smart_json() {
    # $@ -> smartctl args. Emits the smartctl JSON document, or nothing.
    local out
    out=$(smartctl -j "$@" 2>/dev/null) || true
    [[ -n "$out" ]] && jq -e . <<<"$out" >/dev/null 2>&1 && echo "$out"
}

drive_record() {
    # $1 = the /dev node as the OS sees it, $2 = smartctl -d value or "",
    # $3 = how this drive was reached. Reads the fields the Server Report's
    # component sheet asks for and normalises the two different places
    # smartctl reports power-on hours (ATA vs NVMe).
    local dev="$1" dflag="$2" via="$3" doc
    if [[ -n "$dflag" ]]; then
        doc=$(smart_json -a -d "$dflag" "$dev")
    else
        doc=$(smart_json -a "$dev")
    fi
    [[ -z "$doc" ]] && return 1

    # A megaraid slot that holds no drive still answers, but with no identity.
    local model
    model=$(jq -r '.model_name // .scsi_model_name // empty' <<<"$doc")
    [[ -z "$model" ]] && return 1

    jq --arg dev "$dev" --arg via "$via" --arg dflag "$dflag" '{
        device: $dev,
        reached_via: $via,
        smartctl_d: (if $dflag == "" then null else $dflag end),
        model: (.model_name // .scsi_model_name // null),
        vendor: (.scsi_vendor // .model_family // null),
        serial_number: (.serial_number // null),
        firmware: (.firmware_version // .scsi_revision // null),
        capacity_bytes: (.user_capacity.bytes // .nvme_total_capacity // null),
        rotation_rate: (.rotation_rate // null),
        protocol: (.device.protocol // null),
        power_on_hours: (.power_on_time.hours
                         // .nvme_smart_health_information_log.power_on_hours
                         // null),
        power_cycles: (.power_cycle_count
                       // .nvme_smart_health_information_log.power_cycles
                       // null),
        percentage_used: (.nvme_smart_health_information_log.percentage_used // null),
        smart_passed: (.smart_status.passed // null)
    }' <<<"$doc"
}

collect_drives() {
    log "=== Drives ==="

    if ! command -v smartctl >/dev/null 2>&1; then
        warn "smartctl unavailable -- skipping drive collection"
        echo '[]' > "$WORK_DIR/drives.json"
        return 0
    fi

    : > "$WORK_DIR/drives.ndjson"

    # 1. Everything the OS presents directly.
    local dev
    while read -r dev; do
        [[ -z "$dev" ]] && continue
        log "  probing /dev/$dev"
        if drive_record "/dev/$dev" "" "direct" >> "$WORK_DIR/drives.ndjson"; then
            :
        else
            warn "    /dev/$dev returned no usable SMART identity"
        fi
    done < <(lsblk -dno NAME,TYPE 2>/dev/null | awk '$2=="disk"{print $1}')

    # 2. Members behind each MegaRAID volume. The volume itself already
    #    appeared above as a "direct" device with the controller's own model
    #    string; these are the drives inside it.
    local raid_devs
    raid_devs=$(lsblk -dno NAME,MODEL 2>/dev/null \
                | grep -iE 'megaraid|MR[0-9]|PERC|LSI|AVAGO' | awk '{print $1}')

    if [[ -n "$raid_devs" ]]; then
        local rdev did found=0
        for rdev in $raid_devs; do
            log "  /dev/$rdev looks like a RAID volume -- probing members 0..$((MEGARAID_MAX_DID - 1))"
            for (( did=0; did<MEGARAID_MAX_DID; did++ )); do
                if drive_record "/dev/$rdev" "megaraid,$did" "megaraid" >> "$WORK_DIR/drives.ndjson"; then
                    log "    megaraid,$did: found a drive"
                    found=$(( found + 1 ))
                fi
            done
        done
        log "  MegaRAID members found: $found"
        [[ "$found" -eq 0 ]] && warn "  No MegaRAID members answered -- boot drive detail will be missing"
    else
        log "  No MegaRAID-backed volume detected"
    fi

    jq -s '.' "$WORK_DIR/drives.ndjson" > "$WORK_DIR/drives.json" 2>/dev/null \
        || echo '[]' > "$WORK_DIR/drives.json"
    log "  Drive records: $(jq 'length' "$WORK_DIR/drives.json")"
}

###############################################################################
# ADD-IN CARDS -- serial numbers from PCI VPD
###############################################################################
# DMI does not carry add-in card serials, which is why lshw reports the NIC
# "serial" as a MAC address and the HBA's as nothing at all. The cards
# themselves carry Vital Product Data with the board serial and part number,
# and lspci reads it as root.
collect_pci_cards() {
    log "=== Add-in cards (PCI VPD) ==="

    if ! command -v lspci >/dev/null 2>&1; then
        warn "lspci unavailable -- skipping VPD collection"
        echo '[]' > "$WORK_DIR/cards.json"
        return 0
    fi

    local dump
    dump=$(safe_run lspci -vv)
    echo "$dump" > "$WORK_DIR/lspci-vv.txt"

    echo "$dump" | awk '
        BEGIN { print "["; n = 0 }
        # A device header starts at column 0.
        /^[0-9a-fA-F]{2}:/ {
            flush()
            bdf = $1
            desc = $0
            sub(/^[^ ]+ /, "", desc)
            cls = desc
            sub(/:.*/, "", cls)
            sub(/^[^:]*: */, "", desc)
            pn = ""; sn = ""; pname = ""
            next
        }
        /\[PN\] Part number:/     { pn    = trim(substr($0, index($0, ":") + 1)) }
        /\[SN\] Serial number:/   { sn    = trim(substr($0, index($0, ":") + 1)) }
        /Product Name:/           { if (pname == "") pname = trim(substr($0, index($0, ":") + 1)) }
        END { flush(); print "]" }
        function trim(s) { gsub(/^[ \t]+|[ \t]+$/, "", s); gsub(/"/, "", s); return s }
        function flush() {
            if (bdf == "") return
            # Only cards that actually published VPD are worth a row.
            if (pn == "" && sn == "") { bdf = ""; return }
            if (n++) print ","
            printf "{\"pci_address\":\"%s\",\"class\":\"%s\",\"description\":\"%s\",", bdf, esc(cls), esc(desc)
            printf "\"part_number\":\"%s\",\"serial_number\":\"%s\",\"vpd_product_name\":\"%s\"}", esc(pn), esc(sn), esc(pname)
            bdf = ""
        }
        function esc(s) { gsub(/\\/, "", s); gsub(/"/, "", s); return s }
    ' > "$WORK_DIR/cards.json" 2>/dev/null || echo '[]' > "$WORK_DIR/cards.json"

    if jq -e . "$WORK_DIR/cards.json" >/dev/null 2>&1; then
        normalise_blanks "$WORK_DIR/cards.json"
    else
        warn "VPD block did not parse into valid JSON"
        echo '[]' > "$WORK_DIR/cards.json"
    fi
    # A dual-port adapter publishes identical VPD on every PCI function, so
    # 09:00.0 and 09:00.1 are one card with one board serial, not two cards.
    # Collapse on serial and keep the functions it answered on.
    local dedup="$WORK_DIR/cards.dedup.json"
    if jq 'group_by(.serial_number)
           | map(.[0] + {pci_functions: (map(.pci_address))})
           | map(del(.pci_address))' "$WORK_DIR/cards.json" > "$dedup" 2>/dev/null; then
        mv "$dedup" "$WORK_DIR/cards.json"
    else
        rm -f "$dedup"
    fi

    local c
    c=$(jq 'length' "$WORK_DIR/cards.json")
    log "  Distinct cards publishing VPD: $c"
    [[ "$c" -eq 0 ]] && log "  (no VPD on this platform -- add-in card serials stay a label check)"
}

###############################################################################
# CHASSIS IDENTITY
###############################################################################
collect_identity() {
    log "=== Chassis identity (SMBIOS) ==="

    if ! command -v dmidecode >/dev/null 2>&1; then
        echo '{}' > "$WORK_DIR/identity.json"
        return 0
    fi

    local sys_mfr sys_prod sys_ser sys_ver bb_ser bb_prod ch_ser ch_asset bb_asset
    local bios_ver bios_date bios_vendor
    sys_mfr=$(safe_run  dmidecode -s system-manufacturer   | head -1)
    sys_prod=$(safe_run dmidecode -s system-product-name   | head -1)
    sys_ser=$(safe_run  dmidecode -s system-serial-number  | head -1)
    sys_ver=$(safe_run  dmidecode -s system-version        | head -1)
    bb_prod=$(safe_run  dmidecode -s baseboard-product-name| head -1)
    bb_ser=$(safe_run   dmidecode -s baseboard-serial-number | head -1)
    bb_asset=$(safe_run dmidecode -s baseboard-asset-tag   | head -1)
    ch_ser=$(safe_run   dmidecode -s chassis-serial-number | head -1)
    ch_asset=$(safe_run dmidecode -s chassis-asset-tag     | head -1)

    # BIOS version is reported under Schedule 4 paragraph 1(d), and on this
    # platform it also explains the PSU count: SMBIOS type 39 lists all four
    # bays only from BIOS 1003 onward, two below it. Capturing it here keeps
    # the two facts in one record instead of hand-copied into the workbook.
    bios_ver=$(safe_run    dmidecode -s bios-version     | head -1)
    bios_date=$(safe_run   dmidecode -s bios-release-date| head -1)
    bios_vendor=$(safe_run dmidecode -s bios-vendor      | head -1)

    # ASUS ships these as literal placeholder text. Treat them as absent, so
    # the Server Report does not print "To be filled by O.E.M." as an asset tag.
    [[ "$bb_asset" =~ ^(To be filled by O\.E\.M\.|Default string|Unknown|N/A)$ ]] && bb_asset=""
    [[ "$ch_asset" =~ ^(To be filled by O\.E\.M\.|Default string|Unknown|N/A)$ ]] && ch_asset=""

    log "  System serial:     ${sys_ser:-unknown}"
    log "  Chassis serial:    ${ch_ser:-unknown}"
    log "  Baseboard serial:  ${bb_ser:-unknown}"
    log "  BIOS:              ${bios_ver:-unknown} (${bios_date:-date unknown})"

    jq -n \
        --argjson a "$(jstr "$sys_mfr")"  --argjson b "$(jstr "$sys_prod")" \
        --argjson c "$(jstr "$sys_ser")"  --argjson d "$(jstr "$sys_ver")" \
        --argjson e "$(jstr "$bb_prod")"  --argjson f "$(jstr "$bb_ser")" \
        --argjson g "$(jstr "$ch_ser")"   --argjson h "$(jstr "$ch_asset")" \
        --argjson i "$(jstr "$bb_asset")" \
        --argjson j "$(jstr "$bios_ver")"  --argjson k "$(jstr "$bios_date")" \
        --argjson l "$(jstr "$bios_vendor")" \
        '{system_manufacturer:$a, system_product:$b, system_serial:$c,
          system_version:$d, baseboard_product:$e, baseboard_serial:$f,
          chassis_serial:$g, chassis_asset_tag:$h, baseboard_asset_tag:$i,
          bios_version:$j, bios_release_date:$k, bios_vendor:$l}' \
        > "$WORK_DIR/identity.json"
}

###############################################################################
# CPUs -- SMBIOS type 4
###############################################################################
# Schedule 4 paragraph 1(c) asks for make, model and quantity of each CPU, not
# a serial number -- only the GPUs (limb 1(b)) need serials. This is collected
# anyway so the Server Report's claim about CPU serials is evidenced from the
# machine rather than inferred: DMI type 4 carries Serial Number, Asset Tag and
# Part Number fields, and on these ASUS boards the serial comes back literally
# "Unknown". Recording that is materially different from leaving the cell blank,
# which would read as "this part has no serial".
collect_cpus() {
    log "=== CPUs (SMBIOS type 4) ==="
    if ! command -v dmidecode >/dev/null 2>&1; then
        echo '[]' > "$WORK_DIR/cpus.json"
        return 0
    fi
    local dump
    dump=$(safe_run dmidecode -t 4)
    if [[ -z "$dump" ]]; then
        warn "dmidecode -t 4 returned nothing"
        echo '[]' > "$WORK_DIR/cpus.json"
        return 0
    fi
    log "---------- dmidecode -t 4 ----------"
    echo "$dump" >&2
    log "---------- end dmidecode -t 4 ----------"

    echo "$dump" | awk '
        BEGIN { print "["; n = 0 }
        /^Processor Information/ { flush(); inblk = 1
            sock=""; ver=""; mfr=""; ser=""; part=""; asset=""; status=""; core=""; thr=""; next }
        inblk && /^$/ { flush(); next }
        inblk {
            line = $0; sub(/^[ \t]+/, "", line)
            p = index(line, ":"); if (p == 0) next
            k = substr(line, 1, p-1); v = substr(line, p+1)
            sub(/^[ \t]+/, "", v); sub(/[ \t]+$/, "", v); gsub(/"/, "", v)
            if (k == "Socket Designation")   sock  = v
            else if (k == "Version")         ver   = v
            else if (k == "Manufacturer")    mfr   = v
            else if (k == "Serial Number")   ser   = v
            else if (k == "Part Number")     part  = v
            else if (k == "Asset Tag")       asset = v
            else if (k == "Status")          status = v
            else if (k == "Core Count")      core  = v
            else if (k == "Thread Count")    thr   = v
        }
        END { flush(); print "]" }
        function flush() {
            if (!inblk) return
            inblk = 0
            if (sock == "" && ver == "") return
            if (n++) print ","
            printf "{\"socket\":\"%s\",\"version\":\"%s\",\"manufacturer\":\"%s\",", sock, ver, mfr
            printf "\"serial_number\":\"%s\",\"part_number\":\"%s\",\"asset_tag\":\"%s\",", ser, part, asset
            printf "\"status\":\"%s\",\"core_count\":\"%s\",\"thread_count\":\"%s\"}", status, core, thr
        }
    ' > "$WORK_DIR/cpus.json" 2>/dev/null || echo '[]' > "$WORK_DIR/cpus.json"

    if jq -e . "$WORK_DIR/cpus.json" >/dev/null 2>&1; then
        # "Unknown", "Not Specified" and the O.E.M. placeholders are what the
        # BIOS writes when it has nothing. Fold them to null so the report can
        # say "not populated by this BIOS" rather than printing the placeholder.
        jq 'map(map_values(
              if . == "" or . == "Unknown" or . == "Not Specified"
                 or test("^To Be Filled|^To be filled"; "i") then null else . end))' \
           "$WORK_DIR/cpus.json" > "$WORK_DIR/cpus.tmp" 2>/dev/null \
            && mv "$WORK_DIR/cpus.tmp" "$WORK_DIR/cpus.json" || rm -f "$WORK_DIR/cpus.tmp"
    else
        warn "CPU block did not parse into valid JSON"
        echo '[]' > "$WORK_DIR/cpus.json"
    fi
    log "  CPU sockets: $(jq 'length' "$WORK_DIR/cpus.json"), serials populated: $(jq '[.[]|select(.serial_number)]|length' "$WORK_DIR/cpus.json")"
}

###############################################################################
# REPORT
###############################################################################
assemble_report() {
    log "=== Assembling report ==="

    local issues="[]" overall="PASS"

    # Roll the drive hours up into the power_on object. Schedule 4 asks for
    # server power-on hours in one cell, and on a chassis whose BMC exposes no
    # such sensor the drives are the measurement that exists. Where every drive
    # agrees and the spread is small they were fitted from new, so the figure
    # is the chassis's own; the spread is published so a reader can see that
    # rather than take it on trust. Cross-check it against sel_span_days: on a
    # machine that has been powered most of its life the two nearly meet.
    local dh_max dh_min dh_n
    dh_max=$(jq '[.[].power_on_hours | numbers] | max // null' "$WORK_DIR/drives.json")
    dh_min=$(jq '[.[].power_on_hours | numbers] | min // null' "$WORK_DIR/drives.json")
    dh_n=$(jq '[.[].power_on_hours | numbers] | length' "$WORK_DIR/drives.json")
    if [[ "$dh_max" != "null" ]]; then
        jq --argjson mx "$dh_max" --argjson mn "$dh_min" --argjson n "$dh_n" \
           '. + {drive_hours_max:$mx, drive_hours_min:$mn, drives_reporting_hours:$n,
                 drive_hours_spread:($mx - $mn)}' \
           "$WORK_DIR/poh.json" > "$WORK_DIR/poh.tmp" 2>/dev/null \
            && mv "$WORK_DIR/poh.tmp" "$WORK_DIR/poh.json" \
            || rm -f "$WORK_DIR/poh.tmp"
        log "  Drive power-on hours: max $dh_max, min $dh_min across $dh_n drive(s)"
    fi

    local bmc_ok drives_n cards_n psus_n poh
    bmc_ok=$(jq -r '.available' "$WORK_DIR/bmc.json")
    drives_n=$(jq 'length' "$WORK_DIR/drives.json")
    cards_n=$(jq 'length' "$WORK_DIR/cards.json")
    psus_n=$(jq '[.[] | select(.present)] | length' "$WORK_DIR/psus.json")
    poh=$(jq -r '.hours // "null"' "$WORK_DIR/poh.json")

    add_issue() {
        issues=$(echo "$issues" | jq --arg i "$1" --arg s "$2" --arg d "$3" \
            '. + [{issue:$i, severity:$s, details:$d}]')
        [[ "$2" == "warning" && "$overall" == "PASS" ]] && overall="WARN"
        return 0
    }

    [[ "$bmc_ok" != "true" ]] && \
        add_issue "BMC firmware revision not collected" "warning" \
                  "$(jq -r '.reason // "unknown"' "$WORK_DIR/bmc.json")"
    [[ "$poh" == "null" ]] && \
        add_issue "No chassis power-on-hours sensor exposed over IPMI" "info" \
                  "Per-drive power_on_hours is collected instead"
    [[ "$drives_n" -eq 0 ]] && \
        add_issue "No drive records collected" "warning" "smartctl found no identifiable drive"
    [[ "$cards_n" -eq 0 ]] && \
        add_issue "No add-in card published PCI VPD" "info" \
                  "Add-in card serial numbers remain a physical label check"
    local sens_bays=0
    [[ -s "$WORK_DIR/psu_sensors.json" ]] && \
        sens_bays=$(jq -r '.bays // 0' "$WORK_DIR/psu_sensors.json" 2>/dev/null || echo 0)
    [[ "$sens_bays" =~ ^[0-9]+$ ]] || sens_bays=0
    [[ "$sens_bays" -gt 0 && "$psus_n" -gt 0 && "$sens_bays" -ne "$psus_n" ]] && \
        add_issue "PSU count disagrees between SMBIOS and the BMC" "info" \
                  "SMBIOS records $psus_n, the BMC sees $sens_bays bay(s); serials exist only for the SMBIOS entries"
    [[ -n "$TOOLS_MISSING" ]] && \
        add_issue "Some tools could not be installed" "warning" "$TOOLS_MISSING"

    # Build to a file, validate, then emit -- the JSON is the deliverable, and
    # a truncated one read as collected-and-empty is worse than a clean fail.
    jq -n \
        --arg v "$SCRIPT_VERSION" \
        --arg ts "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" \
        --argjson dur "$(( $(date +%s) - SCRIPT_START ))" \
        --argjson overall "$(jstr "$overall")" \
        --argjson issues "$issues" \
        --slurpfile identity "$WORK_DIR/identity.json" \
        --slurpfile bmc "$WORK_DIR/bmc.json" \
        --slurpfile fru "$WORK_DIR/fru.json" \
        --slurpfile poh "$WORK_DIR/poh.json" \
        --slurpfile psus "$WORK_DIR/psus.json" \
        --slurpfile psusens "$WORK_DIR/psu_sensors.json" \
        --slurpfile chassis "$WORK_DIR/chassis_idle.json" \
        --slurpfile cpus "$WORK_DIR/cpus.json" \
        --slurpfile drives "$WORK_DIR/drives.json" \
        --slurpfile cards "$WORK_DIR/cards.json" \
        '{
            report_metadata: {
                script_version: $v,
                script_name: "hw-inventory",
                generated_at: $ts,
                duration_seconds: $dur
            },
            verdict: { overall: $overall, issues: $issues },
            identity: $identity[0],
            bmc: $bmc[0],
            fru: $fru[0],
            power_on: $poh[0],
            psus: $psus[0],
            psu_sensors: $psusens[0],
            chassis_idle: $chassis[0],
            cpus: $cpus[0],
            drives: $drives[0],
            add_in_cards: $cards[0]
        }' > "$WORK_DIR/report.json" || {
            err "Could not assemble the JSON report"
            return 1
        }

    if ! jq -e . "$WORK_DIR/report.json" >/dev/null 2>&1; then
        err "Assembled report is not valid JSON"
        return 1
    fi

    cat "$WORK_DIR/report.json"

    log "  PSU bays populated:  $psus_n"
    log "  Drive records:       $drives_n"
    log "  Cards with VPD:      $cards_n"
    log "  Verdict:             $overall"
    return 0
}

###############################################################################
# MAIN
###############################################################################
main() {
    SCRIPT_START=$(date +%s)
    TOOLS_MISSING=""

    log "=========================================="
    log "NexGen Chassis & Component Inventory v${SCRIPT_VERSION}"
    log "=========================================="

    if ! install_packages; then
        err "Cannot continue without jq"
        exit 1
    fi

    collect_identity
    collect_cpus
    collect_bmc
    collect_fru
    collect_power_on_hours
    collect_psus
    collect_drives
    collect_pci_cards

    local ok=true
    assemble_report || ok=false

    rm -rf "$WORK_DIR"

    log "=========================================="
    log "Inventory complete. Total time: $(( $(date +%s) - SCRIPT_START ))s"
    log "=========================================="

    [[ "$ok" == "false" ]] && exit 1
    exit 0
}

main "$@"
