#!/bin/bash
# --- Start MAAS Metadata ---
# name: 80-nexgen-network-cabling-verify
# title: NexGen Network Cabling Verification
# description: Verifies physical network cabling matches NetBox by
#   identifying the host (by DMI serial, fallback hostname), pulling
#   the planned per-interface cable destinations from NetBox 4.x,
#   and comparing them against live LLDP neighbours on every
#   physical NIC. Runs before the GPU scripts so misbonded hosts
#   are caught before ~2h of driver/stress work. Fails commissioning
#   on any mismatch, missing link, or undocumented live link.
# script_type: commissioning
# hardware_type: network
# timeout: 00:05:00
# destructive: false
# may_reboot: false
# --- End MAAS Metadata ---

set -o pipefail
trap 'warn "Command failed at line $LINENO (exit code $?)"' ERR

###############################################################################
# CONFIG -- override via env before `maas commissioning-scripts create` or
# by editing this file prior to upload. Defaults marked CHANGEME must be
# replaced for the script to be useful.
###############################################################################
SCRIPT_VERSION="1.0.0"
WORK_DIR="/tmp/cabling-verify-$$"

NETBOX_URL="${NETBOX_URL:-https://netbox.example.com}"
NETBOX_TOKEN="${NETBOX_TOKEN:-CHANGEME}"
LLDP_WAIT_SECONDS="${LLDP_WAIT_SECONDS:-45}"
REQUIRE_ALL_PLANNED="${REQUIRE_ALL_PLANNED:-true}"
ALLOW_EXTRA_LINKS="${ALLOW_EXTRA_LINKS:-false}"
NAME_MATCH_STRICT="${NAME_MATCH_STRICT:-false}"
IGNORE_IFNAMES="${IGNORE_IFNAMES:-}"
# When true, match planned cables to observed LLDP by (switch, port) endpoint
# only -- don't require the local NIC name to equal the NetBox interface name.
# This keeps the check meaningful when predictable naming (enpXsYfZ vs eth0)
# makes ifname correspondence unreliable.
MATCH_BY_ENDPOINT="${MATCH_BY_ENDPOINT:-true}"
# NetBox interface names matching this regex are treated as BMC/IPMI
# interfaces that the host OS cannot observe via LLDP. They're reported
# as NOT_CHECKED (info) rather than critical failures.
BMC_IFNAME_PATTERN="${BMC_IFNAME_PATTERN:-^(ipmi|bmc|mgmt|management)$}"

###############################################################################
# LOGGING
###############################################################################
log()  { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [INFO]  $*" >&2; }
warn() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [WARN]  $*" >&2; }
err()  { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [ERROR] $*" >&2; }

mkdir -p "$WORK_DIR"

safe_run() { local o; if o=$("$@" 2>/dev/null); then echo "$o"; else echo ""; fi; }

###############################################################################
# URL-encode a string via jq
###############################################################################
urlenc() { jq -rn --arg s "$1" '$s|@uri'; }

###############################################################################
# Emit a FAIL verdict JSON and exit. Used for fatal preflight/NetBox errors
# where we can't produce a full comparison report.
###############################################################################
emit_fatal() {
    local msg="$1"
    err "$msg"
    jq -n --arg v "$SCRIPT_VERSION" --arg m "$msg" \
        --arg ts "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" \
        '{
            report_metadata:{script_version:$v, script_name:"network-cabling-verify", generated_at:$ts},
            verdict:{overall:"FAIL", issues:[{"issue":$m, "severity":"critical"}]}
        }'
    exit 1
}

###############################################################################
# Interface-name normalisation. Lowercase, strip common prefixes,
# keep only digits/slash/dash/dot. Used on both switch and server sides.
###############################################################################
normalise_ifname() {
    local n="${1,,}"
    # Strip longest prefixes first so "gigabitethernet" isn't eaten as "gi"+"gabit...".
    local prefixes=(
        gigabitethernet tengigabitethernet hundredgigabitethernet fortygigabitethernet
        tengige hundredgige fortygige twentyfivegige fiftygige
        ethernet management mgmt
        enp eno ens em
        swp
        eth xe et te fo hu gi
    )
    local p
    for p in "${prefixes[@]}"; do
        if [[ "$n" == "$p"* ]]; then
            n="${n#"$p"}"
            break
        fi
    done
    # Keep only [0-9/.\-]
    echo "$n" | tr -cd '0-9/.\-'
}

###############################################################################
# Compare two normalised port identifiers. Exact match wins; otherwise one
# being a suffix of the other passes unless NAME_MATCH_STRICT is true.
###############################################################################
ports_match() {
    local a_raw="$1" b_raw="$2"
    local a b
    a=$(normalise_ifname "$a_raw")
    b=$(normalise_ifname "$b_raw")
    [[ -z "$a" || -z "$b" ]] && return 1
    [[ "$a" == "$b" ]] && return 0
    if [[ "$NAME_MATCH_STRICT" != "true" ]]; then
        [[ "$a" == *"$b" || "$b" == *"$a" ]] && return 0
    fi
    return 1
}

###############################################################################
# Switch hostname match. Exact (case-insensitive) or planned is a prefix of
# observed (handles FQDN vs short hostname).
###############################################################################
switches_match() {
    local planned="${1,,}" observed="${2,,}"
    [[ -z "$planned" || -z "$observed" ]] && return 1
    [[ "$planned" == "$observed" ]] && return 0
    [[ "$observed" == "$planned".* ]] && return 0
    return 1
}

###############################################################################
# PREFLIGHT
###############################################################################
preflight() {
    log "=== Preflight checks ==="

    local missing=""
    command -v jq        &>/dev/null || missing+=" jq"
    command -v curl      &>/dev/null || missing+=" curl"
    command -v ip        &>/dev/null || missing+=" ip"
    command -v dmidecode &>/dev/null || missing+=" dmidecode"

    if [[ -n "$missing" ]]; then
        emit_fatal "Missing required tools:$missing"
    fi

    if [[ "$NETBOX_TOKEN" == "CHANGEME" || -z "$NETBOX_TOKEN" ]]; then
        emit_fatal "NETBOX_TOKEN not configured -- edit this script before uploading to MAAS"
    fi
    if [[ "$NETBOX_URL" == "https://netbox.example.com" || -z "$NETBOX_URL" ]]; then
        emit_fatal "NETBOX_URL not configured -- edit this script before uploading to MAAS"
    fi
    NETBOX_URL="${NETBOX_URL%/}"

    log "Tools OK, NetBox=${NETBOX_URL}"
}

###############################################################################
# Install lldpd if missing. Starts the daemon and waits for neighbour discovery.
###############################################################################
ensure_lldpd() {
    if ! command -v lldpctl &>/dev/null; then
        log "lldpd not installed -- installing via apt"
        export DEBIAN_FRONTEND=noninteractive
        apt-get update -qq >&2 || warn "apt-get update failed -- trying install anyway"
        if ! apt-get install -y lldpd >&2; then
            emit_fatal "Failed to install lldpd -- cannot observe LLDP neighbours"
        fi
    fi

    log "lldpd version: $(lldpd -v 2>&1 | head -1 || echo '?')"

    # Start the daemon. Ignore failure if already running.
    local start_rc=0
    if command -v systemctl &>/dev/null && systemctl list-unit-files lldpd.service &>/dev/null; then
        systemctl start lldpd >&2 || start_rc=$?
        log "systemctl start lldpd -> rc=$start_rc"
        systemctl is-active lldpd >&2 || warn "lldpd is not active per systemctl"
    else
        service lldpd start >&2 || start_rc=$?
        log "service lldpd start -> rc=$start_rc"
    fi

    if pgrep -x lldpd >/dev/null; then
        log "lldpd process present (pid=$(pgrep -x lldpd | tr '\n' ' '))"
    else
        warn "lldpd process NOT running after start attempt"
    fi

    log "Waiting ${LLDP_WAIT_SECONDS}s for LLDP neighbour discovery"
    # Kick lldpd to TX immediately so adjacent switches reply quickly.
    if command -v lldpcli &>/dev/null; then
        lldpcli update >&2 2>/dev/null || warn "lldpcli update failed (daemon not ready?)"
    fi
    sleep "$LLDP_WAIT_SECONDS"
}

###############################################################################
# Diagnostics -- dump NIC, lldpd, and Mellanox firmware state so failures
# can be root-caused from the report alone.
###############################################################################
lldp_diagnostics() {
    log "=== LLDP diagnostics ==="

    # 1. lldpd daemon status
    if pgrep -x lldpd >/dev/null; then
        log "lldpd: running (pid=$(pgrep -x lldpd | tr '\n' ' '))"
    else
        warn "lldpd: NOT running"
    fi

    # 2. Raw lldpctl summary (human-readable, short)
    if command -v lldpcli &>/dev/null; then
        local summary
        summary=$(lldpcli show neighbors summary 2>&1 || true)
        if [[ -n "$summary" ]]; then
            log "lldpcli show neighbors summary:"
            echo "$summary" | sed 's/^/    /' >&2
        fi
        local stats
        stats=$(lldpcli show statistics 2>&1 || true)
        if [[ -n "$stats" ]]; then
            log "lldpcli show statistics (TX/RX counters per iface):"
            echo "$stats" | sed 's/^/    /' >&2
        fi
        local cfg
        cfg=$(lldpcli show configuration 2>&1 | head -40 || true)
        if [[ -n "$cfg" ]]; then
            log "lldpcli show configuration (first 40 lines):"
            echo "$cfg" | sed 's/^/    /' >&2
        fi
    fi

    # 3. Per-interface multicast RX counter (01:80:c2:00:00:0e is LLDP mcast).
    #    If RX counter is 0 on an "up" iface, the NIC/switch isn't delivering
    #    LLDPDUs at all -- usually firmware LLDP intercept (Mellanox) or
    #    switch LLDP disabled.
    log "Per-interface multicast RX (via /proc/net/dev):"
    awk 'NR>2 {print "    " $0}' /proc/net/dev >&2 || true

    # 4. Mellanox firmware LLDP intercept check (ConnectX-5/6 bnxt/mlx5).
    #    If any mlx5 NIC exists and mlxconfig is available, dump LLDP_NB_*
    #    settings -- non-zero = firmware is eating LLDPDUs.
    local have_mlx5=false
    if jq -e '.[] | select(.driver=="mlx5_core")' "$WORK_DIR/nics.json" >/dev/null 2>&1; then
        have_mlx5=true
    fi
    if [[ "$have_mlx5" == "true" ]]; then
        # On-demand install of Mellanox Firmware Tools (mft). Provides
        # mlxconfig + mst, which are the only tools that can query the
        # NIC firmware's LLDP_NB / DCBX registers. Ubuntu main repo
        # ships mft as the 'mft' package.
        if ! command -v mlxconfig &>/dev/null; then
            log "mlxconfig not present -- installing 'mft' package for firmware LLDP check"
            export DEBIAN_FRONTEND=noninteractive
            if apt-get install -y mft >&2 2>/dev/null; then
                log "mft installed ($(mlxconfig --version 2>&1 | head -1 || echo '?'))"
            else
                warn "apt-get install mft failed -- falling back to kernel-side checks only"
            fi
        fi

        if command -v mlxconfig &>/dev/null; then
            # mst must be started so /dev/mst/* device nodes exist for mlxconfig.
            if command -v mst &>/dev/null; then
                mst start >&2 2>/dev/null || warn "mst start failed"
                mst status >&2 2>/dev/null || true
            fi
            local dev found_dev=false
            for dev in /dev/mst/mt*_pciconf* /dev/mst/mt*_pci_cr*; do
                [[ -e "$dev" ]] || continue
                found_dev=true
                log "Mellanox firmware LLDP state on $dev:"
                local mlx_out
                mlx_out=$(mlxconfig -d "$dev" q 2>/dev/null | grep -Ei 'LLDP_NB|DCBX' || true)
                if [[ -n "$mlx_out" ]]; then
                    echo "$mlx_out" | sed 's/^/    /' >&2
                else
                    log "    (no LLDP_NB/DCBX fields found in mlxconfig output)"
                fi
            done
            if [[ "$found_dev" != "true" ]]; then
                # Fallback: query by PCI address directly (works even when /dev/mst
                # isn't populated, e.g. kernel module mismatch).
                local pci
                for pci in $(jq -r '.[] | select(.driver=="mlx5_core") | .pci_addr' "$WORK_DIR/nics.json" | sort -u); do
                    [[ -z "$pci" ]] && continue
                    log "Mellanox firmware LLDP state via PCI $pci:"
                    local mlx_out
                    mlx_out=$(mlxconfig -d "$pci" q 2>/dev/null | grep -Ei 'LLDP_NB|DCBX' || true)
                    if [[ -n "$mlx_out" ]]; then
                        echo "$mlx_out" | sed 's/^/    /' >&2
                    else
                        log "    (mlxconfig -d $pci returned no LLDP_NB/DCBX fields)"
                    fi
                done
            fi
        else
            warn "Mellanox NIC present but mlxconfig still unavailable after install attempt"
        fi
        # Kernel-side: mlx5 driver exposes a privilege flag for DCBX firmware offload.
        local m
        for m in $(jq -r '.[] | select(.driver=="mlx5_core") | .ifname' "$WORK_DIR/nics.json"); do
            local flags
            flags=$(ethtool --show-priv-flags "$m" 2>/dev/null || true)
            if [[ -n "$flags" ]]; then
                log "ethtool --show-priv-flags $m:"
                echo "$flags" | sed 's/^/    /' >&2
            fi
        done
    fi

    # 5. Raw lldpctl JSON (truncated) so we can see exactly what parse saw.
    if command -v lldpctl &>/dev/null; then
        local raw
        raw=$(lldpctl -f json0 2>&1 || true)
        local rawlen=${#raw}
        log "lldpctl -f json0 raw output: ${rawlen} bytes"
        if [[ "$rawlen" -gt 0 ]]; then
            echo "$raw" | head -c 2000 | sed 's/^/    /' >&2
            [[ "$rawlen" -gt 2000 ]] && echo "    ...[truncated]" >&2
        fi
    fi
}

###############################################################################
# STAGE 1 -- identify the host
###############################################################################
identify_host() {
    log "=== Identifying host ==="

    local raw_serial
    raw_serial=$(safe_run dmidecode -s system-serial-number)
    local compact="${raw_serial//[[:space:]]/}"

    SERIAL=""
    case "${compact,,}" in
        ""|"none"|"notspecified"|"defaultstring"|"systemserialnumber"|"0123456789"|"tobefilledbyo.e.m."|"tobefilled"*)
            warn "DMI serial unusable ('$raw_serial') -- will lookup by hostname"
            ;;
        *)
            SERIAL="$raw_serial"
            ;;
    esac

    HOSTNAME_SHORT=$(hostname -s)

    if [[ -n "$SERIAL" ]]; then
        LOOKUP_KEY="serial"
    else
        LOOKUP_KEY="hostname"
    fi

    log "Host: serial='${SERIAL:-<unusable>}', hostname='${HOSTNAME_SHORT}', lookup_key=${LOOKUP_KEY}"

    jq -n \
        --arg serial "$SERIAL" \
        --arg hostname "$HOSTNAME_SHORT" \
        --arg mfg "$(safe_run dmidecode -s system-manufacturer)" \
        --arg prod "$(safe_run dmidecode -s system-product-name)" \
        --arg lookup "$LOOKUP_KEY" \
        '{serial:$serial, hostname:$hostname, manufacturer:$mfg, product:$prod, lookup_key:$lookup}' \
        > "$WORK_DIR/system.json"
}

###############################################################################
# STAGE 2 -- enumerate local physical NICs
###############################################################################
enumerate_nics() {
    log "=== Enumerating local physical NICs ==="

    local link_json
    link_json=$(ip -j link show 2>/dev/null || echo "[]")

    local nics="[]"
    local i
    for i in /sys/class/net/*; do
        local name; name=$(basename "$i")
        [[ "$name" == "lo" ]] && continue
        [[ ! -e "$i/device" ]] && continue

        local drv="unknown"
        if [[ -L "$i/device/driver" ]]; then
            drv=$(basename "$(readlink -f "$i/device/driver")" 2>/dev/null || echo "unknown")
        fi

        case "$drv" in
            virtio_net|tun|bridge|veth|bonding|tap) continue ;;
        esac

        local mac operstate speed pci_addr
        mac=$(cat "$i/address" 2>/dev/null || echo "")
        operstate=$(cat "$i/operstate" 2>/dev/null || echo "unknown")
        speed=$(cat "$i/speed" 2>/dev/null || echo "")
        pci_addr=""
        if [[ -L "$i/device" ]]; then
            pci_addr=$(basename "$(readlink -f "$i/device")" 2>/dev/null || echo "")
        fi

        nics=$(echo "$nics" | jq \
            --arg name "$name" --arg mac "$mac" --arg op "$operstate" \
            --arg drv "$drv" --arg speed "$speed" --arg pci "$pci_addr" \
            '. + [{ifname:$name, mac:$mac, operstate:$op, driver:$drv, speed_mbps:$speed, pci_addr:$pci}]')
    done

    echo "$nics" > "$WORK_DIR/nics.json"
    local count; count=$(echo "$nics" | jq 'length')
    log "Found $count physical NIC(s)"
    echo "$nics" | jq -r '.[] | "  \(.ifname)  mac=\(.mac)  state=\(.operstate)  driver=\(.driver)"' >&2
}

###############################################################################
# STAGE 3 -- collect LLDP neighbours
###############################################################################
collect_lldp() {
    log "=== Collecting LLDP neighbours ==="

    local raw
    if ! raw=$(lldpctl -f json0 2>/dev/null); then
        warn "lldpctl returned no data -- assuming no neighbours"
        echo "[]" > "$WORK_DIR/lldp.json"
        return
    fi

    # Normalise lldpctl's json0 into a flat array, one row per local iface.
    # lldpd 1.0.x json0 wraps every scalar in [{"value": "..."}] arrays and
    # stores ifname as a direct .name property on each interface entry:
    #   {"lldp":[{"interface":[{"name":"enp33s0f0np0",
    #      "chassis":[{"name":[{"value":"sw1"}],"id":[{"type":"mac","value":"..."}]}],
    #      "port":[{"id":[{"type":"ifname","value":"Ethernet1/4"}],"descr":[{"value":"..."}]}]
    #   }]}]}
    # Older lldpd used object-keyed layout {"interface":{"eth0":{...}}} with
    # unwrapped scalars. We support both formats via the helpers below.
    local flat
    flat=$(echo "$raw" | jq '
        def unwrap_arr: if type == "array" then (.[0] // null) else . end;
        # Extract a scalar value from lldpd 1.0 arrays-of-{value} OR older direct forms.
        def sval:
            if . == null then null
            elif type == "array" then ((.[0] // {}).value // .[0] // null)
            elif type == "object" then (.value // null)
            else . end;
        (.lldp | unwrap_arr) as $l |
        (if $l == null then [] else ($l.interface // []) end) as $ifs_raw |
        (
          if ($ifs_raw | type) == "array" then
            # 1.0.x: list of {name, chassis:[...], port:[...]} objects
            $ifs_raw | map(
              . as $e |
              ($e.chassis | unwrap_arr) as $ch |
              ($e.port    | unwrap_arr) as $po |
              {
                ifname:       ($e.name // ""),
                chassis_name: ((($ch // {}).name) | sval),
                chassis_id:   ((($ch // {}).id)   | sval),
                port_id:      ((($po // {}).id)   | sval),
                port_descr:   ((($po // {}).descr) | sval)
              }
            )
          else
            # older layout: object keyed by ifname
            $ifs_raw | to_entries | map(
              .value as $v |
              (($v.chassis // {}) | to_entries | .[0]) as $ch |
              {
                ifname:       .key,
                chassis_name: ($ch.key // null),
                chassis_id:   ($ch.value.id.value // null),
                port_id:      ($v.port.id.value // null),
                port_descr:   ($v.port.descr // null)
              }
            )
          end
        )
    ' 2>/dev/null || echo "[]")

    echo "$flat" > "$WORK_DIR/lldp.json"
    local count; count=$(echo "$flat" | jq 'length')
    log "LLDP neighbours on $count interface(s)"
    echo "$flat" | jq -r '.[] | "  \(.ifname) -> chassis=\(.chassis_name // "?") port_id=\(.port_id // "?") port_descr=\(.port_descr // "?")"' >&2

    # If no neighbours were observed, dump a diagnostics block so we can
    # tell timing/config issues apart from firmware LLDP intercept apart
    # from switch-side LLDP being off. Only runs on the empty case so
    # normal successful runs stay quiet.
    if [[ "$count" == "0" ]]; then
        lldp_diagnostics
    fi
}

###############################################################################
# STAGE 4 -- fetch planned cabling from NetBox 4.x
###############################################################################
nb_get() {
    local url="$1"
    local body http_code
    local tmp="$WORK_DIR/nb_body.$$"
    http_code=$(curl -sS -o "$tmp" -w '%{http_code}' \
        -H "Authorization: Token $NETBOX_TOKEN" \
        -H "Accept: application/json" \
        "$url" 2>>"$WORK_DIR/nb_errors.log") || http_code="000"
    body=$(cat "$tmp" 2>/dev/null || true)
    rm -f "$tmp"
    if [[ "$http_code" != "200" ]]; then
        err "NetBox GET $url returned HTTP $http_code"
        [[ -n "$body" ]] && err "  body: $(echo "$body" | head -c 500)"
        return 1
    fi
    echo "$body"
}

fetch_planned() {
    log "=== Fetching planned cabling from NetBox ==="

    local device_resp device_count device_id device_name

    if [[ -n "$SERIAL" ]]; then
        log "  Looking up device by serial=${SERIAL}"
        device_resp=$(nb_get "$NETBOX_URL/api/dcim/devices/?serial=$(urlenc "$SERIAL")") \
            || emit_fatal "NetBox lookup by serial failed -- see log"
        device_count=$(echo "$device_resp" | jq '.count // 0')
    else
        device_count=0
        device_resp='{"count":0,"results":[]}'
    fi

    if [[ "$device_count" == "0" ]]; then
        log "  Serial lookup returned 0 results -- falling back to hostname=${HOSTNAME_SHORT}"
        device_resp=$(nb_get "$NETBOX_URL/api/dcim/devices/?name=$(urlenc "$HOSTNAME_SHORT")") \
            || emit_fatal "NetBox lookup by hostname failed -- see log"
        device_count=$(echo "$device_resp" | jq '.count // 0')
    fi

    if [[ "$device_count" == "0" ]]; then
        emit_fatal "Device not found in NetBox by serial='${SERIAL}' or hostname='${HOSTNAME_SHORT}'"
    fi
    if [[ "$device_count" != "1" ]]; then
        warn "NetBox returned $device_count devices -- using first result"
    fi

    device_id=$(echo "$device_resp" | jq -r '.results[0].id')
    device_name=$(echo "$device_resp" | jq -r '.results[0].name')
    log "  Matched NetBox device id=$device_id name=$device_name"

    jq -n --arg url "$NETBOX_URL" --argjson id "$device_id" --arg name "$device_name" \
        '{url:$url, device_id:$id, device_name:$name}' > "$WORK_DIR/netbox.json"

    # Pull all cabled interfaces, paginating via ?next.
    local next="$NETBOX_URL/api/dcim/interfaces/?device_id=${device_id}&cabled=true&limit=500"
    local all="[]"
    while [[ -n "$next" && "$next" != "null" ]]; do
        local page
        page=$(nb_get "$next") || emit_fatal "NetBox interfaces fetch failed"
        all=$(jq -s '.[0] + (.[1].results // [])' <(echo "$all") <(echo "$page"))
        next=$(echo "$page" | jq -r '.next // ""')
        [[ "$next" == "null" ]] && next=""
    done

    # Filter to interfaces whose connected endpoint is another interface (skip
    # console/power), extract (ifname, switch, switch_port).
    local planned
    planned=$(echo "$all" | jq '
        [ .[]
          | select(.connected_endpoints_type == "dcim.interface")
          | select((.connected_endpoints // []) | length > 0)
          | . as $iface
          | ($iface.connected_endpoints | length) as $n
          | ($iface.connected_endpoints[0]) as $ep
          | {
              ifname: $iface.name,
              netbox_iface_id: $iface.id,
              switch: ($ep.device.name // null),
              switch_port: ($ep.name // null),
              multi_endpoint: ($n > 1)
            }
        ]
    ')

    echo "$planned" > "$WORK_DIR/planned.json"
    local pc; pc=$(echo "$planned" | jq 'length')
    log "  $pc planned cable(s) found"

    # Warn on any breakout/multi-endpoint cables we're simplifying.
    local multi
    multi=$(echo "$planned" | jq '[.[] | select(.multi_endpoint)] | length')
    if [[ "$multi" -gt 0 ]]; then
        warn "  $multi interface(s) have multiple connected endpoints; using first"
    fi
}

###############################################################################
# STAGE 5/6/7 -- match planned interface to local NIC, then compare vs LLDP
###############################################################################
local_nic_for_netbox_name() {
    local nb_name="$1"
    local nb_lc="${nb_name,,}"
    # Primary: exact case-insensitive match.
    local hit
    hit=$(jq -r --arg n "$nb_lc" '.[] | select((.ifname|ascii_downcase) == $n) | .ifname' "$WORK_DIR/nics.json" | head -1)
    if [[ -n "$hit" ]]; then
        echo "$hit"
        return 0
    fi
    # Secondary: exact match of normalised forms (no suffix trick on the
    # server side -- enp1s0f0 vs enp2s0f0 would both normalise too loosely).
    local nb_norm
    nb_norm=$(normalise_ifname "$nb_name")
    [[ -z "$nb_norm" ]] && return 1
    local cand
    while IFS= read -r cand; do
        local cand_norm
        cand_norm=$(normalise_ifname "$cand")
        [[ -z "$cand_norm" ]] && continue
        if [[ "$cand_norm" == "$nb_norm" ]]; then
            echo "$cand"
            return 0
        fi
    done < <(jq -r '.[].ifname' "$WORK_DIR/nics.json")
    return 1
}

lldp_for_ifname() {
    local ifn="$1"
    jq -c --arg n "$ifn" '.[] | select(.ifname == $n)' "$WORK_DIR/lldp.json" | head -1
}

is_ignored_ifname() {
    local ifn="$1"
    [[ -z "$IGNORE_IFNAMES" ]] && return 1
    local entry
    for entry in ${IGNORE_IFNAMES//,/ }; do
        [[ "$entry" == "$ifn" ]] && return 0
    done
    return 1
}

is_bmc_ifname() {
    local n="${1,,}"
    [[ "$n" =~ $BMC_IFNAME_PATTERN ]]
}

# Find any LLDP observation whose (chassis, port_id|port_descr) matches
# the planned (switch, switch_port). Returns matched local ifname + chassis
# + port_id + port_descr as a single TSV line, or empty if no match.
lldp_find_endpoint() {
    local p_switch="$1" p_port="$2"
    jq -r '.[] | [.ifname, (.chassis_name // ""), (.port_id // ""), (.port_descr // "")] | @tsv' \
        "$WORK_DIR/lldp.json" \
    | while IFS=$'\t' read -r ifn sw pid pdescr; do
        switches_match "$p_switch" "$sw" || continue
        if ports_match "$p_port" "$pid" || ports_match "$p_port" "$pdescr"; then
            printf '%s\t%s\t%s\t%s\n' "$ifn" "$sw" "$pid" "$pdescr"
            return
        fi
      done
}

compare_and_build() {
    log "=== Comparing planned vs actual (mode: endpoint=$MATCH_BY_ENDPOINT) ==="

    local comparison="[]"
    local issues="[]"
    local matched_ifnames=""

    local planned_count
    planned_count=$(jq 'length' "$WORK_DIR/planned.json")

    local i
    for (( i=0; i<planned_count; i++ )); do
        local row
        row=$(jq -c ".[$i]" "$WORK_DIR/planned.json")
        local p_name p_switch p_port
        p_name=$(echo "$row"   | jq -r '.ifname')
        p_switch=$(echo "$row" | jq -r '.switch // ""')
        p_port=$(echo "$row"   | jq -r '.switch_port // ""')

        local local_nic="" result="" reason="" a_chassis="" a_port=""
        local severity="critical"

        # BMC/IPMI interfaces are on a separate management network and not
        # visible to the host OS -- skip cleanly with an info-level note
        # rather than producing a spurious critical failure.
        if is_bmc_ifname "$p_name"; then
            result="NOT_CHECKED"
            reason="'$p_name' is a BMC/IPMI interface; host OS cannot observe its LLDP neighbour"
            severity="info"
        elif [[ "$MATCH_BY_ENDPOINT" == "true" ]]; then
            # Endpoint-based: any LLDP observation matching (switch, port) is OK,
            # regardless of which host NIC it shows up on. Ignores ifname drift
            # between NetBox's "eth0/eth1" and Linux's predictable "enpXsYfZ".
            local hit
            hit=$(lldp_find_endpoint "$p_switch" "$p_port")
            if [[ -n "$hit" ]]; then
                local a_descr a_id
                IFS=$'\t' read -r local_nic a_chassis a_id a_descr <<<"$hit"
                matched_ifnames+=" $local_nic"
                a_port="$a_id"
                [[ -z "$a_port" || "$a_port" == "null" ]] && a_port="$a_descr"
                result="OK"
                reason=""
            else
                # Build a diagnostic reason so the operator can tell at a glance
                # whether the fault is likely the cable (NIC down), the switch
                # port (NIC up but no LLDP), or just wrong cabling (LLDP seen
                # on an unexpected endpoint).
                result="MISSING_LINK"
                local down_nics up_no_lldp observed
                down_nics=$(jq -r '[.[] | select(.operstate!="up") | .ifname] | join(", ")' "$WORK_DIR/nics.json")
                up_no_lldp=$(jq -r --slurpfile l "$WORK_DIR/lldp.json" \
                    '[.[] | select(.operstate=="up") | .ifname
                        | select(. as $n | ($l[0] | map(.ifname) | index($n)) | not)]
                     | join(", ")' "$WORK_DIR/nics.json")
                observed=$(jq -r '[.[] | "\(.ifname)->\(.chassis_name)/\(.port_id)"] | join(", ")' "$WORK_DIR/lldp.json")
                reason="Planned link $p_switch/$p_port not observed."
                [[ -n "$down_nics" ]]   && reason+=" Down NICs (possible unplugged cable or dead optic): $down_nics."
                [[ -n "$up_no_lldp" ]]  && reason+=" Up NICs with no LLDP neighbour (switch-side LLDP off or wrong port?): $up_no_lldp."
                [[ -n "$observed" ]]   && reason+=" Observed elsewhere: $observed."
            fi
        else
            # Strict ifname-based matching (legacy behaviour).
            if ! local_nic=$(local_nic_for_netbox_name "$p_name"); then
                result="LOCAL_NIC_NOT_FOUND"
                reason="NetBox interface '$p_name' has no matching NIC on host"
                local_nic=""
            else
                matched_ifnames+=" $local_nic"
                local lldp_row
                lldp_row=$(lldp_for_ifname "$local_nic")
                if [[ -z "$lldp_row" ]]; then
                    result="MISSING_LINK"
                    reason="No LLDP neighbour observed on $local_nic"
                else
                    a_chassis=$(echo "$lldp_row" | jq -r '.chassis_name // ""')
                    local a_descr a_id
                    a_descr=$(echo "$lldp_row" | jq -r '.port_descr // ""')
                    a_id=$(echo "$lldp_row" | jq -r '.port_id // ""')
                    a_port="$a_id"
                    [[ -z "$a_port" || "$a_port" == "null" ]] && a_port="$a_descr"
                    if ! switches_match "$p_switch" "$a_chassis"; then
                        result="WRONG_SWITCH"
                        reason="planned switch '$p_switch' != observed '$a_chassis'"
                    elif ports_match "$p_port" "$a_id" || ports_match "$p_port" "$a_descr"; then
                        result="OK"
                        reason=""
                    else
                        result="WRONG_PORT"
                        reason="planned port '$p_port' != observed id='$a_id' descr='$a_descr' on $a_chassis"
                    fi
                fi
            fi
        fi

        comparison=$(echo "$comparison" | jq \
            --arg nb "$p_name" --arg nic "$local_nic" \
            --arg ps "$p_switch" --arg pp "$p_port" \
            --arg ac "$a_chassis" --arg ap "$a_port" \
            --arg r "$result" --arg why "$reason" \
            '. + [{
                netbox_ifname:$nb,
                local_ifname:$nic,
                planned:{switch:$ps, switch_port:$pp},
                actual:{chassis:$ac, port:$ap},
                result:$r, reason:$why
            }]')

        case "$result" in
            OK) : ;;
            NOT_CHECKED)
                issues=$(echo "$issues" | jq \
                    --arg m "[$p_name] $result: $reason" --arg s "$severity" \
                    '. + [{issue:$m, severity:$s}]') ;;
            *)  issues=$(echo "$issues" | jq \
                    --arg m "[$p_name] $result: $reason" --arg s "$severity" \
                    '. + [{issue:$m, severity:$s}]') ;;
        esac
    done

    # --- Walk every local NIC not touched by a planned cable ---
    local nic_names
    nic_names=$(jq -r '.[].ifname' "$WORK_DIR/nics.json")
    local nic
    for nic in $nic_names; do
        if [[ " $matched_ifnames " == *" $nic "* ]]; then
            continue
        fi
        if is_ignored_ifname "$nic"; then
            log "  Ignoring $nic (in IGNORE_IFNAMES)"
            continue
        fi

        local lldp_row a_chassis="" a_port="" result reason
        lldp_row=$(lldp_for_ifname "$nic")
        if [[ -n "$lldp_row" ]]; then
            a_chassis=$(echo "$lldp_row" | jq -r '.chassis_name // ""')
            local a_descr a_id
            a_descr=$(echo "$lldp_row" | jq -r '.port_descr // ""')
            a_id=$(echo "$lldp_row" | jq -r '.port_id // ""')
            a_port="$a_id"
            [[ -z "$a_port" || "$a_port" == "null" ]] && a_port="$a_descr"
            result="UNPLANNED_LINK"
            reason="NIC $nic is live (neighbour $a_chassis/$a_port) but has no cable in NetBox"
        else
            result="UNPLANNED_IDLE"
            reason="NIC $nic present on host but not in NetBox (no LLDP neighbour either)"
        fi

        comparison=$(echo "$comparison" | jq \
            --arg nb "" --arg nic "$nic" \
            --arg ac "$a_chassis" --arg ap "$a_port" \
            --arg r "$result" --arg why "$reason" \
            '. + [{
                netbox_ifname:$nb,
                local_ifname:$nic,
                planned:{switch:null, switch_port:null},
                actual:{chassis:$ac, port:$ap},
                result:$r, reason:$why
            }]')

        # UNPLANNED_IDLE on a down interface isn't actionable -- skip it in
        # endpoint mode where we're deliberately relaxing ifname pedantry.
        if [[ "$result" == "UNPLANNED_IDLE" && "$MATCH_BY_ENDPOINT" == "true" ]]; then
            continue
        fi

        if [[ "$ALLOW_EXTRA_LINKS" == "true" ]]; then
            issues=$(echo "$issues" | jq \
                --arg m "[$nic] $result (allowed): $reason" --arg s "info" \
                '. + [{issue:$m, severity:$s}]')
        else
            issues=$(echo "$issues" | jq \
                --arg m "[$nic] $result: $reason" --arg s "critical" \
                '. + [{issue:$m, severity:$s}]')
        fi
    done

    echo "$comparison" > "$WORK_DIR/comparison.json"
    echo "$issues"     > "$WORK_DIR/issues.json"
}

###############################################################################
# STAGE 8 -- ASCII table to stderr
###############################################################################
print_table() {
    echo >&2
    echo >&2 "=== Planned vs Actual Cabling ==="
    printf '%-10s  %-36s  %-36s  %s\n' "NIC" "PLANNED" "ACTUAL" "RESULT" >&2
    printf '%-10s  %-36s  %-36s  %s\n' "----------" "------------------------------------" "------------------------------------" "--------------------" >&2

    # For planned rows, prefer the NetBox-side ifname so the column reads
    # "eth0 -> leaf254a/Eth1/4 OK" instead of "enp33s0f1n -> ...". Host-side
    # ifname drift is intentional (endpoint-based matching), so we don't
    # surface it in the table. Unplanned rows still show local ifname,
    # since there's no NetBox counterpart.
    jq -r '.[] | [
        (if (.netbox_ifname // "") != "" then .netbox_ifname else .local_ifname end),
        (if (.planned.switch // "") == "" then "<not in NetBox>"
         else "\(.planned.switch // "?") / \(.planned.switch_port // "?")"
         end),
        (if (.actual.chassis // "") == "" then "<no LLDP neighbour>"
         else "\(.actual.chassis // "?") / \(.actual.port // "?")"
         end),
        .result
    ] | @tsv' "$WORK_DIR/comparison.json" \
    | while IFS=$'\t' read -r nic plan actual result; do
        printf '%-10.10s  %-36.36s  %-36.36s  %s\n' "$nic" "$plan" "$actual" "$result" >&2
    done
    echo >&2
}

###############################################################################
# STAGE 9 -- ASSEMBLE & OUTPUT FINAL JSON
###############################################################################
assemble_report() {
    log "=== Assembling report ==="

    local test_end dur overall issues_len ok_count bad_count
    test_end=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
    dur=$(( $(date +%s) - SCRIPT_START ))

    ok_count=$(jq '[.[] | select(.result=="OK")] | length' "$WORK_DIR/comparison.json")
    bad_count=$(jq '[.[] | select(.result!="OK" and .result!="UNPLANNED_IDLE" and .result!="NOT_CHECKED")] | length' "$WORK_DIR/comparison.json")
    issues_len=$(jq 'length' "$WORK_DIR/issues.json")
    local critical_len
    critical_len=$(jq '[.[] | select(.severity=="critical")] | length' "$WORK_DIR/issues.json")

    # Overall verdict is driven by *critical* issues only; info-severity
    # entries (e.g. NOT_CHECKED IPMI) are recorded but don't fail the run.
    if [[ "$critical_len" == "0" ]]; then
        overall="PASS"
    else
        overall="FAIL"
    fi

    local hash
    hash=$(cat "$WORK_DIR"/*.json 2>/dev/null | sha256sum | awk '{print $1}')

    jq -n \
        --arg ver "$SCRIPT_VERSION" --arg name "network-cabling-verify" \
        --arg ts "$test_end" --argjson dur "$dur" \
        --arg verdict "$overall" \
        --arg hash "$hash" \
        --argjson ok "$ok_count" --argjson bad "$bad_count" \
        --slurpfile sys "$WORK_DIR/system.json" \
        --slurpfile nb  "$WORK_DIR/netbox.json" \
        --slurpfile plan "$WORK_DIR/planned.json" \
        --slurpfile nics "$WORK_DIR/nics.json" \
        --slurpfile lldp "$WORK_DIR/lldp.json" \
        --slurpfile cmp "$WORK_DIR/comparison.json" \
        --slurpfile iss "$WORK_DIR/issues.json" \
        '{
            report_metadata:{
                script_version:$ver, script_name:$name, generated_at:$ts,
                test_duration_seconds:$dur, data_hash_sha256:$hash,
                links_ok:$ok, links_failing:$bad
            },
            verdict:{overall:$verdict, issues:$iss[0]},
            system:$sys[0],
            netbox:$nb[0],
            planned:$plan[0],
            actual:{nics:$nics[0], lldp:$lldp[0]},
            comparison:$cmp[0]
        }'

    log "=== VERIFY COMPLETE -- Verdict: $overall (ok=$ok_count, failing=$bad_count) ==="
    [[ "$overall" == "FAIL" ]] && return 1
    return 0
}

###############################################################################
# MAIN
###############################################################################
main() {
    SCRIPT_START=$(date +%s)

    log "=========================================="
    log "NexGen Network Cabling Verify v${SCRIPT_VERSION}"
    log "=========================================="

    preflight
    identify_host
    enumerate_nics
    ensure_lldpd
    collect_lldp
    fetch_planned
    compare_and_build
    print_table

    local ok=true
    if ! assemble_report; then
        ok=false
    fi

    rm -rf "$WORK_DIR"

    log "=========================================="
    log "Verify complete. Total time: $(( $(date +%s) - SCRIPT_START ))s"
    log "=========================================="

    if [[ "$ok" == "false" ]]; then
        exit 1
    fi
}

main "$@"
