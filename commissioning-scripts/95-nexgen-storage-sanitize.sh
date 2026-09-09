#!/bin/bash
# --- Start MAAS Metadata ---
# name: 95-nexgen-storage-sanitize
# title: NexGen Storage Sanitisation & Evidence
# description: Sanitises every storage device in a host that is leaving NexGen
#   custody, and produces the serial-number-level evidence that it did.
#   Implements sections 6 to 10 of the Remove Host for Sale runbook: attests
#   that it is running in a trusted maintenance environment, inventories all
#   media including drives hidden behind a RAID controller, selects a
#   purge-capable method per device, executes it, adjudicates completion on the
#   device's numeric status rather than log wording, and emits the whole
#   evidence register as JSON. Runs standalone; needs no other script.
#   DEFAULTS TO AUDIT MODE -- it inventories and reports and destroys nothing
#   until SANITIZE_CONFIRM is set in this file. Outputs structured JSON.
# script_type: commissioning
# hardware_type: node
# timeout: 04:00:00
# destructive: true
# may_reboot: false
# --- End MAAS Metadata ---

set -o pipefail
trap 'warn "Command failed at line $LINENO (exit code $?)"' ERR

###############################################################################
# CONFIG
###############################################################################
# Defaults marked CHANGEME must be replaced for the destructive phase to run.
# MAAS injects no per-script environment, so the default in this file IS the
# setting: edit it before uploading, and re-upload to change it.
WORK_DIR="/tmp/storage-sanitize-$$"
SCRIPT_VERSION="1.0.0"

# The destructive opt-in. Must read exactly the phrase below for anything to be
# erased. Any other value -- including the CHANGEME sentinel -- leaves the run
# in audit mode: full inventory, capability audit and method plan, and not one
# destructive command issued. That is the default deliberately, so this script
# uploaded alongside 89-99 cannot wipe a machine nobody meant to wipe.
SANITIZE_CONFIRM="${SANITIZE_CONFIRM:-CHANGEME}"
SANITIZE_PHRASE="SANITIZE ALL MEDIA"

# Optional target pin: one or more DMI system serials, separated by whitespace
# or commas. When set, the chassis this runs on has to be one of them or the run
# refuses before touching anything -- not the media, and not the RAID
# controller either. Cheap insurance against erasing the wrong box, and a list
# is what makes it usable for a batch of machines rather than one at a time.
#
#   EXPECT_SYSTEM_SERIAL="R9S0MD0000AA"
#   EXPECT_SYSTEM_SERIAL="R9S0MD0000AA R9S0MD0000AB R9S0MD0000AC"
#   EXPECT_SYSTEM_SERIAL="R9S0MD0000AA,R9S0MD0000AB"
EXPECT_SYSTEM_SERIAL="${EXPECT_SYSTEM_SERIAL:-}"

# Recorded into the evidence so the certificate can carry them. MAAS cannot
# supply them, so they are edited here or they stay null; the reviewer is a
# human step under section 14 and is never filled in by this script.
JIRA_TICKET="${JIRA_TICKET:-}"
ENGINEER="${ENGINEER:-}"

# How many MegaRAID device slots to probe behind each controller. The members
# are not enumerated anywhere we can read, so they are found by trying. Same
# default and same reason as script 89.
MEGARAID_MAX_DID="${MEGARAID_MAX_DID:-32}"

# Polling bounds. The declared MAAS timeout is documentation only -- Script.timeout
# defaults to 0, meaning no timeout -- so these are the real protection against a
# drive that starts a sanitise and never finishes it.
SANITIZE_POLL_INTERVAL="${SANITIZE_POLL_INTERVAL:-30}"
# Clamped, because the bound below is a deadline and a zero-second poll would
# spin against it rather than respect it.
[[ "$SANITIZE_POLL_INTERVAL" =~ ^[0-9]+$ ]] && (( SANITIZE_POLL_INTERVAL < 1 )) && SANITIZE_POLL_INTERVAL=1
[[ "$SANITIZE_POLL_INTERVAL" =~ ^[0-9]+$ ]] || SANITIZE_POLL_INTERVAL=30
SANITIZE_MAX_WAIT="${SANITIZE_MAX_WAIT:-3600}"
OVERWRITE_MAX_SECONDS="${OVERWRITE_MAX_SECONDS:-7200}"

# Set true to attempt a full-device overwrite on media with no purge-capable
# method. An overwrite is a NIST Clear, not a Purge: on flash it reaches only
# user-addressable blocks, leaving over-provisioned capacity, retired blocks and
# unmapped FTL copies untouched. Off by default because it takes hours and does
# not reach the level NIST indicates for media leaving organisational control --
# and because the drives it would be used on are usually purge-capable once
# they can be reached. See the remediation register.
ALLOW_OVERWRITE_FALLBACK="${ALLOW_OVERWRITE_FALLBACK:-false}"

# Whether a verified NIST Clear counts as sufficient. It does not by default.
# SP 800-88r1 keys the required level on whether the media leaves organisational
# control: Clear is aimed at media reused inside the organisation, and Purge is
# what is indicated for media that leaves. A host being sold leaves, so Purge is
# the target for every drive and a Clear leaves the verdict failing.
ACCEPT_NIST_CLEAR="${ACCEPT_NIST_CLEAR:-false}"

# Permission to reconfigure the RAID controller, which is what it takes to reach
# a drive inside a virtual disk: a controller-issued sanitise needs the drive
# Unconfigured Good, so the volume has to be deleted first.
#
# Its own gate, deliberately separate from SANITIZE_CONFIRM. Confirming that the
# media should be wiped is not the same decision as consenting to delete and
# rebuild an array, and one should not imply the other.
#
# The array is recreated afterwards from the configuration captured before the
# deletion, so the buyer receives the same RAID 1 with nothing on it. Set
# RAID_RESTORE_ARRAY=false to leave the drives unconfigured instead.
ALLOW_RAID_RECONFIG="${ALLOW_RAID_RECONFIG:-false}"
RAID_RESTORE_ARRAY="${RAID_RESTORE_ARRAY:-true}"

# Written for a live operator at the console. Note this does NOT survive the
# maintenance environment -- in a MAAS ephemeral boot /root is RAM. The copy
# that lasts is audit_log_b64 in the JSON, which MAAS stores.
LOG_DIR="${LOG_DIR:-/root/nexgen-sanitize-evidence}"

# Packages this script needs, all from the Ubuntu archive. storcli64/perccli64
# are deliberately absent: they are not in the archive and fetching a Broadcom
# binary mid-commissioning is not something this script will do. If one has been
# staged on PATH it gets used; otherwise the gap is reported.
REQUIRED_PKGS="${REQUIRED_PKGS:-nvme-cli smartmontools sg3-utils hdparm util-linux pciutils coreutils curl unzip jq}"

# storcli, fetched at run time.
#
# The commissioning environment is a RAM image PXE-booted for the run, so a
# storcli installed on the deployed OS is not there -- and storcli is the only
# way to reach a drive inside a RAID volume. It is not in the Ubuntu archive, so
# it is fetched, and that is a supply-chain step: the .deb is verified against a
# pinned hash before anything is installed, and a mismatch refuses rather than
# installs. Point STORCLI_DEB_URL at an internal mirror to avoid depending on
# Broadcom from the commissioning VLAN, which is what production should do.
INSTALL_STORCLI="${INSTALL_STORCLI:-true}"
STORCLI_ZIP_URL="${STORCLI_ZIP_URL:-https://docs.broadcom.com/doc/1232745587}"
STORCLI_DEB_URL="${STORCLI_DEB_URL:-}"
STORCLI_SHA256="${STORCLI_SHA256:-5ddf6651e683b95577b42bbb89e28177fa76053e854f3b12a295ec350c16e685}"
STORCLI_INSTALL_PATH="${STORCLI_INSTALL_PATH:-/opt/MegaRAID/storcli/storcli64}"
STORCLI_FETCH_TIMEOUT="${STORCLI_FETCH_TIMEOUT:-180}"

# Linux caps a single argv entry at MAX_ARG_STRLEN (32 pages = 131072 bytes), so
# base64 evidence reaches jq through --rawfile, never --arg. See script 99.
ARTIFACT_MAX_BYTES="${ARTIFACT_MAX_BYTES:-131072}"

###############################################################################
# LOGGING
###############################################################################
# Everything human goes to stderr. stdout carries the JSON document and nothing
# else -- the report generator parses it, and a stray line ahead of it is a bug.
log()  { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [INFO]  $*" >&2; }
warn() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [WARN]  $*" >&2; }
err()  { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [ERROR] $*" >&2; }

mkdir -p "$WORK_DIR"

safe_run() { local o; if o=$("$@" 2>/dev/null); then echo "$o"; else echo ""; fi; }

# Emit a JSON string, or null for the empty string. "" and "not collected" are
# different claims -- a blank serial in an evidence register reads as "this part
# has no serial", which is a statement of fact we cannot make.
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

# Bounded base64 of $2 into $1. Always creates $1 so --rawfile has a file to
# read even when the source is missing or empty.
b64_file_bounded() {
    local dest="$1" src="$2" max="${3:-$ARTIFACT_MAX_BYTES}"
    : > "$dest"
    [[ -s "$src" ]] || return 0
    tr -s '\r' '\n' < "$src" 2>/dev/null | tail -c "$max" | base64 2>/dev/null \
        | tr -d '\n' > "$dest" || true
}

###############################################################################
# The audit log. Every destructive command, its timestamp and its outcome are
# appended here as well as being logged, because this file is what gets embedded
# in the evidence and read by whoever reviews the decommission.
###############################################################################
AUDIT_LOG="$WORK_DIR/audit.log"
: > "$AUDIT_LOG"

audit() {
    printf '%s  %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$*" >> "$AUDIT_LOG"
    log "$*"
}

###############################################################################
# Emit a FAIL verdict JSON and exit. Used for fatal preflight errors where no
# evidence register can be produced.
###############################################################################
emit_fatal() {
    local msg="$1"
    err "$msg"
    local ts
    ts=$(date -u '+%Y-%m-%dT%H:%M:%SZ')

    # Deliberately not routed through jq. The commonest fatal case is jq itself
    # being absent, and a failure handler built on the missing tool dies with
    # "command not found" and emits nothing at all -- leaving MAAS with a
    # failure and no verdict to explain it. Hand-rolled JSON is uglier and
    # always works; the message is escaped so a quote in it cannot break the
    # document.
    local esc="${msg//\\/\\\\}"
    esc="${esc//\"/\\\"}"
    esc="${esc//$'\n'/ }"
    esc="${esc//$'\t'/ }"

    printf '{"report_metadata":{"script_version":"%s","script_name":"storage-sanitize","generated_at":"%s","mode":"aborted"},' \
        "$SCRIPT_VERSION" "$ts"
    printf '"verdict":{"overall":"FAIL","issues":[{"issue":"%s","severity":"critical"}]}}\n' "$esc"
    exit 1
}

###############################################################################
# ISSUES
###############################################################################
ISSUES='[]'
OVERALL="PASS"

# Findings that mean the run must not erase anything at all -- as distinct from
# findings that merely mean the host is not releasable yet.
STOP_CONDITIONS=0

add_issue() {
    # $1 = sentence, $2 = critical|warning|info, $3 = details
    ISSUES=$(echo "$ISSUES" | jq --arg i "$1" --arg s "$2" --arg d "$3" \
        '. + [{issue:$i, severity:$s, details:$d}]')
    case "$2" in
        critical) OVERALL="FAIL" ;;
        warning)  [[ "$OVERALL" == "PASS" ]] && OVERALL="WARN" ;;
    esac
    return 0
}

# Recorded, but not a judgement on the media: an operational note. It appears in
# the evidence and does not move the verdict, because the verdict answers
# whether the data was destroyed and these do not speak to that.
add_note() {
    ISSUES=$(echo "$ISSUES" | jq --arg i "$1" --arg d "$2" \
        '. + [{issue:$i, severity:"advisory", details:$d}]')
    return 0
}

# A critical finding that additionally forbids erasing anything: sections 6 and 7
# both gate section 9, because a host that is not understood must not be wiped.
add_stop() {
    add_issue "$1" "critical" "$2"
    STOP_CONDITIONS=$(( STOP_CONDITIONS + 1 ))
    return 0
}

###############################################################################
# PACKAGES
###############################################################################
install_packages() {
    log "=== Installing sanitisation tools ==="
    export DEBIAN_FRONTEND=noninteractive

    apt-get update -qq 2>&1 | tail -3 >&2 || warn "apt-get update failed, trying the install anyway"
    # shellcheck disable=SC2086
    apt-get install -y -qq $REQUIRED_PKGS 2>&1 | tail -5 >&2 \
        || warn "apt-get install returned non-zero; checking tools individually"

    local tool missing=""
    for tool in nvme smartctl lsblk findmnt hdparm sg_sanitize dmidecode jq; do
        if command -v "$tool" >/dev/null 2>&1; then
            log "  $tool: $(command -v "$tool")"
        else
            warn "  $tool: NOT AVAILABLE -- the media it reaches cannot be sanitised here"
            missing="$missing $tool"
        fi
    done

    # jq assembles the evidence. Without it there is no deliverable at all, and
    # emitting nothing is better than emitting a half-formed object that the
    # generator would read as collected-and-empty.
    if ! command -v jq >/dev/null 2>&1; then
        err "jq is unavailable -- cannot assemble the evidence register"
        return 1
    fi
    TOOLS_MISSING="${missing# }"
    return 0
}

###############################################################################
# SECTION 6 -- TRUSTED MAINTENANCE ENVIRONMENT
###############################################################################
# The runbook forbids three things, and each becomes a hard gate rather than a
# warning:
#
#   * running from a customer-provided operating system
#   * running from an operating system installed on a drive being sanitised
#   * running before the device inventory has been recorded and reviewed
#
# The first two are decided here. The third is structural: this function runs
# first, the inventory second, and the destructive phase cannot start until both
# have produced a clean result.
#
# The findings are recorded as well as checked. An attestation that the erase
# was performed from a trusted environment is part of the evidence, not merely
# a precondition for producing it.
###############################################################################
ROOT_SOURCE=""
ROOT_FSTYPE=""
ROOT_DISK=""
IS_EPHEMERAL="false"

# Resolve a partition or mapper node back to the whole disk it lives on, so a
# root filesystem on /dev/nvme0n1p2 is understood to occupy /dev/nvme0.
parent_disk_of() {
    local src="$1" pk
    [[ -b "$src" ]] || { echo ""; return 0; }
    pk=$(lsblk -no PKNAME "$src" 2>/dev/null | head -1 | tr -d ' ')
    [[ -z "$pk" ]] && pk=$(lsblk -no KNAME "$src" 2>/dev/null | head -1 | tr -d ' ')
    [[ -n "$pk" ]] && echo "/dev/$pk" || echo ""
}

collect_environment() {
    log "=== Section 6: maintenance environment ==="

    local rootinfo
    rootinfo=$(safe_run findmnt -no SOURCE,FSTYPE /)
    ROOT_SOURCE=$(awk '{print $1}' <<<"$rootinfo")
    ROOT_FSTYPE=$(awk '{print $2}' <<<"$rootinfo")

    # An ephemeral root lives in RAM or on a read-only image, so nothing we are
    # about to erase is holding the running system up.
    case "$ROOT_FSTYPE" in
        overlay|squashfs|tmpfs|rootfs|ramfs|overlayfs) IS_EPHEMERAL="true" ;;
        *)                                            IS_EPHEMERAL="false" ;;
    esac

    ROOT_DISK=$(parent_disk_of "$ROOT_SOURCE")

    log "  root source:  ${ROOT_SOURCE:-unknown}"
    log "  root fstype:  ${ROOT_FSTYPE:-unknown}"
    log "  root disk:    ${ROOT_DISK:-none (not a block device)}"
    log "  ephemeral:    $IS_EPHEMERAL"

    # MAAS's own commissioning environment leaves these behind. Their presence
    # corroborates a NexGen-controlled image; their absence does not by itself
    # condemn the run, because a NexGen live image need not carry cloud-init.
    local maas_ind=""
    [[ -d /run/cloud-init ]]                  && maas_ind="$maas_ind cloud-init"
    [[ -e /run/cloud-init/instance-data.json ]] && maas_ind="$maas_ind instance-data"
    [[ -d /etc/cloud ]]                       && maas_ind="$maas_ind etc-cloud"
    [[ -n "${MAAS_BASE_URL:-}" ]]             && maas_ind="$maas_ind MAAS_BASE_URL"
    maas_ind="${maas_ind# }"

    local nvme_ver nvme_json=null
    nvme_ver=$(safe_run nvme version | head -1)
    log "  nvme-cli:     ${nvme_ver:-not present}"
    # Whether the sanitise log can be read as JSON is settled in collect_nvme by
    # asking a real controller for one, not by reading a usage string. Recorded
    # as null until then so an unfinished probe cannot read as a negative.

    local sysserial
    sysserial=$(safe_run dmidecode -s system-serial-number | grep -v '^#' | head -1)
    SYSTEM_SERIAL="$sysserial"

    jq -n \
        --argjson root_source "$(jstr "$ROOT_SOURCE")" \
        --argjson root_fstype "$(jstr "$ROOT_FSTYPE")" \
        --argjson root_disk "$(jstr "$ROOT_DISK")" \
        --argjson ephemeral "$IS_EPHEMERAL" \
        --argjson kernel "$(jstr "$(uname -r)")" \
        --argjson hostname "$(jstr "$(hostname -f 2>/dev/null || hostname)")" \
        --argjson system_serial "$(jstr "$sysserial")" \
        --argjson nvme_version "$(jstr "$nvme_ver")" \
        --argjson nvme_json "null" \
        --argjson maas "$(jstr "$maas_ind")" \
        --argjson smartctl "$(jstr "$(safe_run smartctl --version | head -1)")" \
        --argjson storcli "$(jstr "$(find_raid_cli)")" \
        '{
            root_source: $root_source,
            root_fstype: $root_fstype,
            root_backing_disk: $root_disk,
            is_ephemeral: $ephemeral,
            kernel: $kernel,
            hostname: $hostname,
            system_serial: $system_serial,
            nvme_cli_version: $nvme_version,
            nvme_sanitize_log_json: $nvme_json,
            smartctl_version: $smartctl,
            raid_cli: $storcli,
            maas_indicators: $maas,
            attestation: (if $ephemeral then
                "Running from an ephemeral root (\($root_fstype)); no sanitised device holds the running system."
            else
                "Root filesystem is on \($root_source) (\($root_fstype)), which is persistent storage. Section 6 forbids erasing from an operating system installed on a drive being sanitised."
            end)
        }' > "$WORK_DIR/environment.json" 2>/dev/null \
        || echo '{"is_ephemeral":false,"attestation":"environment could not be recorded"}' > "$WORK_DIR/environment.json"

    audit "environment: root=${ROOT_SOURCE:-unknown} (${ROOT_FSTYPE:-unknown}) ephemeral=$IS_EPHEMERAL"

    if [[ "$IS_EPHEMERAL" != "true" ]]; then
        add_stop "Not running in a trusted maintenance environment" \
            "Root is ${ROOT_SOURCE:-unknown} (${ROOT_FSTYPE:-unknown}), which is persistent storage. Section 6 requires a live or rescue image."
    fi
}

# Ask a real controller for a JSON sanitise log. This is a read, and it is the
# only honest way to know: the answer decides whether section 9 adjudicates on a
# parsed number or on a number scraped out of prose.
probe_sanitize_log_json() {
    local ctrl="$1"
    NVME_JSON_OK="false"
    if [[ -n "$ctrl" ]] \
       && nvme sanitize-log -o json "$ctrl" 2>/dev/null | jq -e '.sstat != null' >/dev/null 2>&1; then
        NVME_JSON_OK="true"
    fi
    log "  JSON sanitize-log: $NVME_JSON_OK (probed on ${ctrl:-no controller})"

    jq --argjson j "$NVME_JSON_OK" '. + {nvme_sanitize_log_json:$j}' \
        "$WORK_DIR/environment.json" > "$WORK_DIR/environment.tmp" 2>/dev/null \
        && mv "$WORK_DIR/environment.tmp" "$WORK_DIR/environment.json" \
        || rm -f "$WORK_DIR/environment.tmp"

    if [[ "$NVME_JSON_OK" != "true" ]]; then
        add_note "The sanitise log cannot be read as JSON on this image" \
            "$(safe_run nvme version | head -1) does not return a JSON sanitise log, so completion is adjudicated from the numeric status parsed out of the text log. Still a number, not the log's wording -- but a current nvme-cli would remove the parsing step."
    fi
}

###############################################################################
# SECTION 7 -- INVENTORY ALL STORAGE MEDIA
###############################################################################
# The host must be checked for all storage media, not only visible NVMe
# namespaces: NVMe SSDs, SATA and SAS drives, drives behind a RAID or HBA
# controller, M.2 boot modules, and internal USB or SD boot media.
#
# Five discovery paths feed one media[] register. Each record carries every
# column the runbook's section 10 evidence table asks for, so the register maps
# onto that table one for one.
#
# The stop condition is the important part: if a physical drive is missing from
# the inventory, inaccessible or failed, the destructive phase does not start.
# An absent device is not an empty one.
###############################################################################
MEDIA="$WORK_DIR/media.ndjson"
: > "$MEDIA"

LSBLK_FIELDS="NAME,PATH,TYPE,SIZE,MODEL,SERIAL,WWN,TRAN,ROTA,HOTPLUG,FSTYPE,MOUNTPOINTS"

smart_json() {
    # $@ -> smartctl args. Emits the smartctl JSON document, or nothing.
    # Exit status is deliberately ignored: a MegaRAID member answers non-zero
    # because the controller cannot pass the SMART status read through, while
    # returning model, serial and firmware perfectly well. Treating that as a
    # failure would discard the only record of the drives actually being sold.
    local out
    out=$(smartctl -j "$@" 2>/dev/null) || true
    [[ -n "$out" ]] && jq -e . <<<"$out" >/dev/null 2>&1 && echo "$out"
}

###############################################################################
# Capability probes. All read-only -- they decide which method section 8 picks,
# and they run before anything is erased.
###############################################################################
ata_caps() {
    # hdparm -I is the canonical source for the ATA SANITIZE feature set.
    # smartctl reports health, not which purge commands the drive implements.
    local dev="$1" out
    out=$(safe_run hdparm -I "$dev")
    local crypto=false block=false over=false sec_sup=false frozen=false
    grep -q 'CRYPTO_SCRAMBLE_EXT' <<<"$out" && crypto=true
    grep -q 'BLOCK_ERASE_EXT'     <<<"$out" && block=true
    grep -q 'OVERWRITE_EXT'       <<<"$out" && over=true
    grep -qE '^[[:space:]]+supported$' <<<"$out" && sec_sup=true
    grep -qE '^[[:space:]]+frozen$'    <<<"$out" && frozen=true
    jq -n --argjson c "$crypto" --argjson b "$block" --argjson o "$over" \
          --argjson s "$sec_sup" --argjson f "$frozen" \
        '{ata_sanitize_crypto:$c, ata_sanitize_block:$b, ata_sanitize_overwrite:$o,
          ata_security_supported:$s, ata_security_frozen:$f}'
}

scsi_sanitize_supported() {
    # SANITIZE is opcode 0x48. sg_opcodes reports the command set the device
    # actually implements, which is the only honest way to know before trying.
    local dev="$1"
    safe_run sg_opcodes "$dev" | grep -qi 'sanitize' && echo true || echo false
}

# The vendor CLI, if an operator has staged one.
#
# `command -v` alone is not enough: the Broadcom package installs to
# /opt/MegaRAID/storcli/storcli64 and puts nothing on PATH, so a probe that only
# consults PATH reports "no CLI staged" on a machine where it is installed. The
# known install paths are checked too, and RAID_CLI overrides the lot.
RAID_CLI="${RAID_CLI:-}"

find_raid_cli() {
    if [[ -n "$RAID_CLI" ]]; then
        [[ -x "$RAID_CLI" ]] && { echo "$RAID_CLI"; return 0; }
        warn "RAID_CLI is set to $RAID_CLI but that is not executable"
    fi
    local c
    for c in storcli64 storcli perccli64 perccli; do
        if command -v "$c" >/dev/null 2>&1; then command -v "$c"; return 0; fi
    done
    for c in /opt/MegaRAID/storcli/storcli64 /opt/MegaRAID/storcli/storcli \
             /opt/MegaRAID/perccli/perccli64 /opt/MegaRAID/perccli/perccli \
             /opt/storcli/storcli64 /usr/local/sbin/storcli64 /usr/local/bin/storcli64; do
        [[ -x "$c" ]] && { echo "$c"; return 0; }
    done
    echo ""
    return 0
}

# Map a unit serial number to its /dev/sg node, so a drive that is invisible as
# a block device may still be reachable for a SCSI SANITIZE.
build_sg_map() {
    : > "$WORK_DIR/sgmap.txt"
    command -v sg_inq >/dev/null 2>&1 || return 0
    local sg s
    for sg in /dev/sg[0-9]*; do
        [[ -e "$sg" ]] || continue
        s=$(safe_run sg_inq "$sg" | sed -n 's/^ *Unit serial number: *//p' | head -1)
        [[ -n "$s" ]] && printf '%s\t%s\n' "$s" "$sg" >> "$WORK_DIR/sgmap.txt"
    done
    log "  sg nodes with a readable serial: $(wc -l < "$WORK_DIR/sgmap.txt" 2>/dev/null || echo 0)"
}

sg_for_serial() {
    [[ -s "$WORK_DIR/sgmap.txt" ]] || return 0
    awk -F'\t' -v s="$1" '$1==s{print $2; exit}' "$WORK_DIR/sgmap.txt" 2>/dev/null
}

###############################################################################
# Path 1 -- everything the kernel presents as a block device.
###############################################################################
collect_lsblk() {
    log "  lsblk block-device inventory"
    if ! lsblk -e7 -J -b -o "$LSBLK_FIELDS" >/dev/null 2>&1; then
        # MOUNTPOINTS is util-linux 2.37+. Older releases carry MOUNTPOINT.
        LSBLK_FIELDS="NAME,PATH,TYPE,SIZE,MODEL,SERIAL,WWN,TRAN,ROTA,HOTPLUG,FSTYPE,MOUNTPOINT"
        warn "    lsblk does not know MOUNTPOINTS; falling back to MOUNTPOINT"
    fi

    safe_run lsblk -e7 -o "$LSBLK_FIELDS" > "$WORK_DIR/lsblk.txt"
    if ! lsblk -e7 -J -b -o "$LSBLK_FIELDS" > "$WORK_DIR/lsblk-raw.json" 2>/dev/null; then
        warn "    lsblk produced no JSON -- block-device inventory will be empty"
        echo '{"blockdevices":[]}' > "$WORK_DIR/lsblk-raw.json"
    fi

    # Flatten to one record per whole disk, carrying every mountpoint found
    # anywhere in its subtree: a mounted partition makes the whole disk a stop
    # condition, not just that partition.
    jq '[ .blockdevices[]? | select(.type == "disk") | {
            name: .name,
            path: (.path // ("/dev/" + .name)),
            size: .size,
            model: .model,
            serial: .serial,
            wwn: .wwn,
            tran: .tran,
            rota: .rota,
            hotplug: .hotplug,
            mounts: ([ .. | objects | (.mountpoints? // [(.mountpoint? // null)]) ]
                     | flatten | map(select(. != null and . != "")) | unique),
            fstypes: ([ .. | objects | .fstype? ]
                      | map(select(. != null and . != "")) | unique)
        } ]' "$WORK_DIR/lsblk-raw.json" > "$WORK_DIR/disks.json" 2>/dev/null \
        || echo '[]' > "$WORK_DIR/disks.json"

    log "    whole disks seen by the kernel: $(jq 'length' "$WORK_DIR/disks.json" 2>/dev/null || echo 0)"
}

###############################################################################
# Path 2 and 3 -- NVMe controllers and their capabilities.
###############################################################################
collect_nvme() {
    log "  NVMe controllers"
    command -v nvme >/dev/null 2>&1 || {
        warn "    nvme is unavailable -- no NVMe device can be inventoried or sanitised"
        echo '[]' > "$WORK_DIR/nvme-ctrls.json"
        return 0
    }

    safe_run nvme list > "$WORK_DIR/nvme-list.txt"
    safe_run nvme list -o json > "$WORK_DIR/nvme-list.json"
    jq -e . "$WORK_DIR/nvme-list.json" >/dev/null 2>&1 || echo '{}' > "$WORK_DIR/nvme-list.json"

    # The controller device, not the namespace: sanitise is issued against
    # /dev/nvme0, never /dev/nvme0n1. The runbook's own enumeration is the
    # /dev scan; the union with what `nvme list` declares is deliberate. A
    # controller that nvme-cli knows about but which has no character device
    # is not a controller to skip -- it is a device this host cannot account
    # for, and section 7 is explicit that an absent device is not an empty one.
    local ctrls scanned declared
    scanned=$(find /dev -maxdepth 1 -type c -regextype posix-extended \
                   -regex '/dev/nvme[0-9]+' 2>/dev/null)
    declared=$(jq -r '[.. | objects | select(has("DevicePath")) | .DevicePath]
                      | map(select(test("^/dev/nvme[0-9]+n[0-9]+$")))
                      | map(sub("n[0-9]+$"; "")) | unique | .[]' \
                  "$WORK_DIR/nvme-list.json" 2>/dev/null)
    ctrls=$(printf '%s\n%s\n' "$scanned" "$declared" | sed '/^$/d' | sort -V -u)

    local n_scanned n_declared
    n_scanned=$(printf '%s' "$scanned" | sed '/^$/d' | wc -l)
    n_declared=$(printf '%s' "$declared" | sed '/^$/d' | wc -l)
    if [[ "$n_scanned" -gt 0 && "$n_declared" -gt 0 && "$n_scanned" -ne "$n_declared" ]]; then
        warn "    /dev lists $n_scanned controller(s), nvme list implies $n_declared"
    fi

    : > "$WORK_DIR/nvme-ctrls.ndjson"
    local ctrl
    for ctrl in $ctrls; do
        log "    $ctrl"
        local idj idh sn mn fr sanicap fna nn cap
        idj=$(safe_run nvme id-ctrl -o json "$ctrl")
        idh=$(safe_run nvme id-ctrl -H "$ctrl")
        printf '===== %s =====\n%s\n' "$ctrl" "$idh" >> "$WORK_DIR/idctrl.txt"

        if ! jq -e . <<<"$idj" >/dev/null 2>&1; then
            # No identity means no evidence. The runbook's stop condition
            # applies: do not assume an unreadable controller holds no data.
            warn "      id-ctrl returned nothing usable"
            jq -n --arg d "$ctrl" '{controller:$d, identified:false}' \
                >> "$WORK_DIR/nvme-ctrls.ndjson"
            continue
        fi

        sn=$(jq -r '.sn // "" | gsub("^ +| +$";"")' <<<"$idj")
        mn=$(jq -r '.mn // "" | gsub("^ +| +$";"")' <<<"$idj")
        fr=$(jq -r '.fr // "" | gsub("^ +| +$";"")' <<<"$idj")
        sanicap=$(jq -r '.sanicap // 0' <<<"$idj")
        fna=$(jq -r '.fna // 0' <<<"$idj")
        nn=$(jq -r '.nn // 0' <<<"$idj")

        # SANICAP bit 0 crypto erase, bit 1 block erase, bit 2 overwrite.
        # FNA bit 2 says Format NVM can do a cryptographic erase.
        # Decoded numerically on purpose: the -H text is kept as evidence but
        # never adjudicated on, because its wording varies by nvme-cli version
        # and by vendor. That is the flaw the runbook's own script warns about.
        [[ "$sanicap" =~ ^[0-9]+$ ]] || sanicap=0
        [[ "$fna" =~ ^[0-9]+$ ]] || fna=0
        local cer ber ows fnacrypto
        cer=$(( sanicap & 1 ));        [[ "$cer" -eq 1 ]] && cer=true || cer=false
        ber=$(( (sanicap >> 1) & 1 )); [[ "$ber" -eq 1 ]] && ber=true || ber=false
        ows=$(( (sanicap >> 2) & 1 )); [[ "$ows" -eq 1 ]] && ows=true || ows=false
        fnacrypto=$(( (fna >> 2) & 1 )); [[ "$fnacrypto" -eq 1 ]] && fnacrypto=true || fnacrypto=false

        log "      sn=$sn mn=$mn fr=$fr sanicap=$sanicap (crypto=$cer block=$ber overwrite=$ows) fna=$fna (crypto=$fnacrypto)"

        cap=$(jq -n --argjson c "$cer" --argjson b "$ber" --argjson o "$ows" \
                    --argjson f "$fnacrypto" --argjson sc "$sanicap" --argjson fn "$fna" \
            '{sanicap_raw:$sc, fna_raw:$fn, nvme_sanitize_crypto:$c,
              nvme_sanitize_block:$b, nvme_sanitize_overwrite:$o,
              nvme_format_crypto:$f}')

        # Namespaces belonging to this controller, tolerant of the two shapes
        # nvme-cli has used for `list -o json` across versions.
        local nsl
        nsl=$(jq --arg c "$(basename "$ctrl")" \
                 '[ .. | objects | select(has("DevicePath")) | .DevicePath
                    | select(startswith("/dev/" + $c + "n")) ] | unique' \
                 "$WORK_DIR/nvme-list.json" 2>/dev/null)
        [[ -z "$nsl" ]] && nsl='[]'

        jq -n --arg d "$ctrl" --arg sn "$sn" --arg mn "$mn" --arg fr "$fr" \
              --argjson nn "$(jnum "$nn")" --argjson cap "$cap" --argjson ns "$nsl" \
            '{controller:$d, identified:true, serial:$sn, model:$mn, firmware:$fr,
              namespace_count:$nn, namespaces:$ns, capabilities:$cap}' \
            >> "$WORK_DIR/nvme-ctrls.ndjson"
    done

    jq -s '.' "$WORK_DIR/nvme-ctrls.ndjson" > "$WORK_DIR/nvme-ctrls.json" 2>/dev/null \
        || echo '[]' > "$WORK_DIR/nvme-ctrls.json"
    log "    NVMe controllers found: $(jq 'length' "$WORK_DIR/nvme-ctrls.json" 2>/dev/null || echo 0)"

    probe_sanitize_log_json "$(printf '%s\n' $ctrls | head -1)"
}

###############################################################################
# One record per device, carrying every column of the section 10 evidence
# table. The sanitisation fields start null and are filled in by section 9.
###############################################################################
emit_media() {
    local dev="$1" ctrl="$2" via="$3" dtype="$4" tran="$5" vendor="$6" model="$7" \
          serial="$8" wwn="$9" fw="${10}" capacity="${11}" mounts="${12}" \
          caps="${13}" physical="${14}"

    [[ -z "$mounts" ]] && mounts='[]'
    [[ -z "$caps" ]] && caps='{}'
    [[ -z "$physical" ]] && physical=false

    jq -n \
        --arg dev "$dev" --arg ctrl "$ctrl" --arg via "$via" --arg dtype "$dtype" \
        --arg tran "$tran" --arg vendor "$vendor" --arg model "$model" \
        --arg serial "$serial" --arg wwn "$wwn" --arg fw "$fw" \
        --argjson capacity "$(jnum "$capacity")" \
        --argjson mounts "$mounts" \
        --argjson caps "$caps" \
        --argjson physical "$physical" \
        '{
            device:        $dev,
            controller:    (if $ctrl   == "" then null else $ctrl   end),
            reached_via:   $via,
            device_type:   $dtype,
            transport:     (if $tran   == "" then null else $tran   end),
            manufacturer:  (if $vendor == "" then null else $vendor end),
            model:         (if $model  == "" then null else $model  end),
            serial_number: (if $serial == "" then null else $serial end),
            wwn:           (if $wwn    == "" then null else $wwn    end),
            firmware:      (if $fw     == "" then null else $fw     end),
            capacity_bytes: $capacity,
            mountpoints:   $mounts,
            capabilities:  $caps,
            physical_media: $physical,
            method:         null,
            method_level:   null,
            method_reason:  null,
            command:        null,
            started_at:     null,
            completed_at:   null,
            duration_seconds: null,
            status_code:    null,
            result:         "not attempted",
            verification:   null
        }' >> "$MEDIA"
}

###############################################################################
# Path 4 and 5 -- non-NVMe media, and the drives a RAID controller hides.
###############################################################################
RAID_VOLUMES=""

collect_block_media() {
    log "  non-NVMe block media"

    local n
    n=$(jq 'length' "$WORK_DIR/disks.json" 2>/dev/null || echo 0)
    local i
    for (( i=0; i<n; i++ )); do
        local path model serial wwn tran size mounts rota
        path=$(jq -r ".[$i].path // \"\"" "$WORK_DIR/disks.json")
        model=$(jq -r ".[$i].model // \"\"" "$WORK_DIR/disks.json")
        serial=$(jq -r ".[$i].serial // \"\"" "$WORK_DIR/disks.json")
        wwn=$(jq -r ".[$i].wwn // \"\"" "$WORK_DIR/disks.json")
        tran=$(jq -r ".[$i].tran // \"\"" "$WORK_DIR/disks.json")
        size=$(jq -r ".[$i].size // \"\"" "$WORK_DIR/disks.json")
        rota=$(jq -r ".[$i].rota // false" "$WORK_DIR/disks.json")
        mounts=$(jq -c ".[$i].mounts // []" "$WORK_DIR/disks.json")
        local fstypes
        fstypes=$(jq -c ".[$i].fstypes // []" "$WORK_DIR/disks.json")

        # NVMe namespaces are covered by their controller; recording them again
        # would double-count the same physical device in the evidence.
        [[ "$tran" == "nvme" ]] && continue

        # The BMC emulates a USB disk and a USB CDROM whether or not anything is
        # attached to them. They are not media in the chassis: there is nothing
        # to remediate, and listing one as needing work makes the register
        # wrong. Measured on the ASMB11 in these nodes, they
        # appear as "Virtual HDisk0" and "Virtual CDROM0" at 0 B with sequential
        # dummy serials.
        #
        # A non-zero size means something IS attached -- an image mounted over
        # Redfish -- whose provenance this script cannot establish and cannot
        # erase, because it is not local storage. That is a stop condition: the
        # image must be detached before the host is released.
        if grep -qiE '^Virtual (HDisk|CDROM|Floppy|Removable)' <<<"$model"; then
            local attached=false
            [[ "$size" =~ ^[0-9]+$ ]] && (( size > 0 )) && attached=true
            log "    $path is BMC virtual media ($model, $([[ "$attached" == true ]] && echo "media attached" || echo "empty"))"
            emit_media "$path" "" "direct" "bmc-virtual-media" "$tran" "" "$model" \
                       "$serial" "$wwn" "" "$size" "$mounts" \
                       "$(jq -n --argjson a "$attached" \
                            '{note:"emulated by the BMC; not media present in the chassis",
                              media_attached:$a}')" false
            if [[ "$attached" == "true" ]]; then
                add_stop "An image is attached to the BMC's virtual media" \
                    "$path ($model) reports $size bytes, so something is mounted over it. This script cannot establish what it is or erase it, because it is not local storage. Detach it in the BMC before the host is released."
            fi
            continue
        fi

        # A hardware RAID volume presents one SCSI device and hides its members.
        # It is not physical media -- the drives inside it are, and they are
        # inventoried separately below.
        if grep -qiE 'megaraid|MR[0-9]|PERC|LSI|AVAGO|ServeRAID|SmartHBA|Smart Array' <<<"$model"; then
            log "    $path is a RAID volume ($model) -- its members are recorded separately"
            RAID_VOLUMES="$RAID_VOLUMES $path"
            emit_media "$path" "" "direct" "raid-volume" "$tran" "" "$model" \
                       "$serial" "$wwn" "" "$size" "$mounts" \
                       "$(jq -n --argjson fs "$fstypes" \
                            '{note:"virtual disk presented by a RAID controller",
                              filesystem_signatures:$fs}')" false
            continue
        fi

        local doc vendor fw dtype caps sgdev scsi_san
        doc=$(smart_json -a "$path")
        [[ -z "$doc" ]] && doc='{}'
        vendor=$(jq -r '.scsi_vendor // .model_family // ""' <<<"$doc" 2>/dev/null)
        fw=$(jq -r '.firmware_version // .scsi_revision // ""' <<<"$doc" 2>/dev/null)
        [[ -z "$model" ]] && model=$(jq -r '.model_name // .scsi_model_name // ""' <<<"$doc" 2>/dev/null)
        [[ -z "$serial" ]] && serial=$(jq -r '.serial_number // ""' <<<"$doc" 2>/dev/null)

        # lsblk reports no TRAN for a drive a RAID controller presents in JBOD,
        # which left two SATA SSDs recorded as "unknown" in a document whose
        # whole job is to say what was erased. The drive's own protocol, which
        # smartctl read from it, is the better answer.
        local proto
        proto=$(jq -r '.device.protocol // ""' <<<"$doc" 2>/dev/null)
        case "$tran" in
            sata|ata) dtype="sata" ;;
            sas|scsi) dtype="sas" ;;
            usb)      dtype="usb" ;;
            mmc)      dtype="sd" ;;
            "")       case "$proto" in
                          ATA|ata)   dtype="sata" ;;
                          SCSI|scsi) dtype="sas" ;;
                          NVMe|nvme) dtype="nvme" ;;
                          *)         dtype="unknown" ;;
                      esac ;;
            *)        dtype="$tran" ;;
        esac
        [[ "$rota" == "true" && "$dtype" == "sata" ]] && dtype="sata-hdd"

        scsi_san=$(scsi_sanitize_supported "$path")
        sgdev=$(sg_for_serial "$serial")
        local atasec
        atasec=$(jq -c '.ata_security // null' <<<"$doc" 2>/dev/null)
        [[ -z "$atasec" ]] && atasec=null
        caps=$(ata_caps "$path" | jq --argjson s "$scsi_san" --arg sg "${sgdev:-}" \
                                     --argjson sec "$atasec" --argjson fs "$fstypes" \
            '. + {scsi_sanitize:$s, sg_node:(if $sg == "" then null else $sg end),
                  ata_security:$sec, filesystem_signatures:$fs}')

        log "    $path  $dtype  model=${model:-?} serial=${serial:-?}"
        [[ -z "$serial" ]] && warn "      no serial readable -- section 7 stop condition"

        emit_media "$path" "" "direct" "$dtype" "$tran" "$vendor" "$model" \
                   "$serial" "$wwn" "$fw" "$size" "$mounts" "$caps" true
    done
}

collect_raid_members() {
    [[ -n "$RAID_VOLUMES" ]] || { log "  no RAID-backed volume detected"; return 0; }
    command -v smartctl >/dev/null 2>&1 || {
        warn "  smartctl unavailable -- drives behind the RAID controller cannot be inventoried"
        return 0
    }

    log "  drives behind the RAID controller"
    local rdev did found=0
    for rdev in $RAID_VOLUMES; do
        log "    probing $rdev members 0..$((MEGARAID_MAX_DID - 1))"
        for (( did=0; did<MEGARAID_MAX_DID; did++ )); do
            local doc model serial fw vendor cap sgdev caps scsi_san
            doc=$(smart_json -a -d "megaraid,$did" "$rdev")
            [[ -z "$doc" ]] && continue
            # A slot that holds no drive still answers, but with no identity.
            model=$(jq -r '.model_name // .scsi_model_name // empty' <<<"$doc")
            [[ -z "$model" ]] && continue

            serial=$(jq -r '.serial_number // ""' <<<"$doc")
            fw=$(jq -r '.firmware_version // .scsi_revision // ""' <<<"$doc")
            vendor=$(jq -r '.scsi_vendor // .model_family // ""' <<<"$doc")
            cap=$(jq -r '.user_capacity.bytes // ""' <<<"$doc")

            # The one route to a purge that does not need a Broadcom binary: if
            # the member answers on its own /dev/sg node, SCSI SANITIZE can be
            # issued directly. Checked by serial, so it cannot be confused with
            # another drive.
            sgdev=$(sg_for_serial "$serial")
            scsi_san=false
            [[ -n "$sgdev" ]] && scsi_san=$(scsi_sanitize_supported "$sgdev")
            local atasec
            atasec=$(jq -c '.ata_security // null' <<<"$doc" 2>/dev/null)
            [[ -z "$atasec" ]] && atasec=null
            caps=$(jq -n --arg sg "${sgdev:-}" --argjson s "$scsi_san" \
                        --argjson sec "$atasec" \
                        --arg cli "$(find_raid_cli)" \
                '{sg_node:(if $sg == "" then null else $sg end),
                  scsi_sanitize:$s,
                  ata_security:$sec,
                  ata_security_supported:($sec.supported // null),
                  raid_cli:(if $cli == "" then null else $cli end)}')

            log "      megaraid,$did  model=$model serial=${serial:-?} sg=${sgdev:-none}"
            found=$(( found + 1 ))
            emit_media "$rdev" "$rdev" "megaraid,$did" "raid-member" "" \
                       "$vendor" "$model" "$serial" "" "$fw" "$cap" '[]' "$caps" true
        done
    done
    log "    RAID members found: $found"
    if [[ "$found" -eq 0 ]]; then
        add_stop "A RAID volume is present but no member drive answered" \
            "Probed ${MEGARAID_MAX_DID} slots behind${RAID_VOLUMES}. The drives inside the volume are unaccounted for, and section 7 forbids assuming an absent device holds no data."
    fi
}

###############################################################################
# Cross-checks. Each disagreement is a stop condition, because the thing it
# would hide is a drive that never got erased.
###############################################################################
check_inventory_consistency() {
    log "  cross-checking the inventory"

    jq -s '.' "$MEDIA" > "$WORK_DIR/media.json" 2>/dev/null || echo '[]' > "$WORK_DIR/media.json"

    local ctrl_n ns_declared ns_seen lsblk_nvme phys_n noserial
    ctrl_n=$(jq 'length' "$WORK_DIR/nvme-ctrls.json" 2>/dev/null || echo 0)
    ns_declared=$(jq '[.[].namespaces | length] | add // 0' "$WORK_DIR/nvme-ctrls.json" 2>/dev/null || echo 0)
    ns_seen=$(jq '[.[] | select(.tran == "nvme")] | length' "$WORK_DIR/disks.json" 2>/dev/null || echo 0)
    lsblk_nvme="$ns_seen"
    phys_n=$(jq '[.[] | select(.physical_media)] | length' "$WORK_DIR/media.json" 2>/dev/null || echo 0)
    noserial=$(jq '[.[] | select(.physical_media and (.serial_number == null))] | length' \
                  "$WORK_DIR/media.json" 2>/dev/null || echo 0)

    local unidentified
    unidentified=$(jq '[.[] | select(.identified == false)] | length' \
                      "$WORK_DIR/nvme-ctrls.json" 2>/dev/null || echo 0)

    audit "inventory: $phys_n physical device(s); $ctrl_n NVMe controller(s), $ns_declared namespace(s) declared, $lsblk_nvme in the kernel"

    if [[ "$unidentified" -gt 0 ]]; then
        add_stop "$unidentified NVMe controller(s) returned no identity" \
            "nvme id-ctrl produced nothing usable. A controller that cannot be identified cannot be evidenced as sanitised."
    fi
    if [[ "$ns_declared" -ne "$lsblk_nvme" ]]; then
        add_stop "NVMe namespace count disagrees between nvme list and lsblk" \
            "nvme list declares $ns_declared namespace(s); the kernel presents $lsblk_nvme. A namespace missing from one of them is a device this run cannot account for."
    fi
    if [[ "$noserial" -gt 0 ]]; then
        add_stop "$noserial physical device(s) have no readable serial number" \
            "Section 10 requires a unique physical-media identifier per device. Without one the evidence cannot be tied to the drive."
    fi
    if [[ "$phys_n" -eq 0 ]]; then
        add_stop "No physical storage media were inventoried" \
            "Either the host has no readable storage or the inventory tools failed. Either way there is nothing to evidence."
    fi

    local swapdevs
    swapdevs=$(jq -r '[.[] | select((.capabilities.filesystem_signatures // []) | index("swap"))
                       | .device] | join(", ")' "$WORK_DIR/media.json" 2>/dev/null)
    if [[ -n "$swapdevs" && "$swapdevs" != "null" ]]; then
        add_issue "A swap signature was found on storage being sanitised" "info" \
            "$swapdevs -- recorded because swap can hold pages written out from any process on the host, so a volume carrying it cannot be assumed to have held no user data. This does not change the required level: every device here has to reach NIST Purge."
    fi

    local mounted
    mounted=$(jq -r '[.[] | select(.physical_media and ((.mountpoints // []) | length > 0)) | .device] | join(", ")' \
                 "$WORK_DIR/media.json" 2>/dev/null)
    if [[ -n "$mounted" && "$mounted" != "null" ]]; then
        add_stop "Storage targeted for sanitisation is mounted" \
            "Mounted: $mounted. Section 6 forbids erasing from a running operating system, and a mounted filesystem may be a customer's."
    fi

    if [[ -n "$ROOT_DISK" ]]; then
        local conflict
        conflict=$(jq -r --arg d "$ROOT_DISK" \
            '[.[] | select(.physical_media and (.device == $d or .controller == $d)) | .device] | join(", ")' \
            "$WORK_DIR/media.json" 2>/dev/null)
        if [[ -n "$conflict" && "$conflict" != "null" ]]; then
            add_stop "The running root filesystem is on a device targeted for sanitisation" \
                "Root is on $ROOT_DISK, which appears in the media register as $conflict. Section 6 forbids this outright."
        fi
    fi
}

###############################################################################
# Fetch and install storcli, if a RAID controller needs one and none is staged.
#
# Every step is recorded: where it came from, what hash it had, whether that
# matched, and whether the install took. An erase performed with a binary this
# script fetched has to be evidenced as such.
###############################################################################
STORCLI_INSTALL='{"attempted":false}'

install_storcli() {
    local existing
    existing=$(find_raid_cli)
    if [[ -n "$existing" ]]; then
        STORCLI_INSTALL=$(jq -n --arg p "$existing" \
            '{attempted:false, reason:"already present", path:$p}')
        return 0
    fi
    if [[ -z "$RAID_VOLUMES" ]]; then
        STORCLI_INSTALL=$(jq -n '{attempted:false, reason:"no RAID-backed volume on this host, so no controller CLI is needed"}')
        return 0
    fi
    if [[ "$INSTALL_STORCLI" != "true" ]]; then
        STORCLI_INSTALL=$(jq -n '{attempted:false, reason:"INSTALL_STORCLI is not true"}')
        return 0
    fi

    log "=== Fetching storcli (no controller CLI in this image) ==="
    local deb="" http="" src=""

    if [[ -n "$STORCLI_DEB_URL" ]]; then
        src="$STORCLI_DEB_URL"
        log "  from $src"
        http=$(curl -fsSL --max-time "$STORCLI_FETCH_TIMEOUT" -o "$WORK_DIR/storcli.deb" \
                    -w '%{http_code}' "$src" 2>/dev/null) || http="${http:-fetch-failed}"
        [[ -s "$WORK_DIR/storcli.deb" ]] && deb="$WORK_DIR/storcli.deb"
    else
        src="$STORCLI_ZIP_URL"
        log "  from $src"
        http=$(curl -fsSL --max-time "$STORCLI_FETCH_TIMEOUT" -o "$WORK_DIR/storcli.zip" \
                    -w '%{http_code}' "$src" 2>/dev/null) || http="${http:-fetch-failed}"
        if [[ -s "$WORK_DIR/storcli.zip" ]]; then
            log "  unpacking $(stat -c %s "$WORK_DIR/storcli.zip" 2>/dev/null) bytes"
            ( cd "$WORK_DIR" && unzip -oq storcli.zip 2>/dev/null ) || true
            local inner
            inner=$(find "$WORK_DIR" -name 'Unified_storcli_all_os.zip' 2>/dev/null | head -1)
            [[ -n "$inner" ]] && ( cd "$(dirname "$inner")" && unzip -oq "$(basename "$inner")" 2>/dev/null ) || true
            deb=$(find "$WORK_DIR" -name 'storcli_*_all.deb' 2>/dev/null | head -1)
        fi
    fi

    if [[ -z "$deb" || ! -s "$deb" ]]; then
        warn "  could not obtain a storcli package (http=$http)"
        STORCLI_INSTALL=$(jq -n --arg s "$src" --arg h "$http" \
            '{attempted:true, source:$s, http:$h, installed:false,
              reason:"no package could be downloaded or unpacked -- the commissioning network may not reach it. Stage the binary in the image, or serve it from an internal mirror with STORCLI_DEB_URL."}')
        add_issue "storcli could not be fetched in this environment" "warning" \
            "Tried $src (http=$http). Drives behind the RAID controller therefore have no purge path from this run."
        return 0
    fi

    local got
    got=$(sha256sum "$deb" 2>/dev/null | awk '{print $1}')
    log "  package sha256 $got"
    if [[ -n "$STORCLI_SHA256" && "$got" != "$STORCLI_SHA256" ]]; then
        err "  sha256 does not match the pinned value -- refusing to install"
        STORCLI_INSTALL=$(jq -n --arg s "$src" --arg g "$got" --arg w "$STORCLI_SHA256" \
            '{attempted:true, source:$s, sha256:$g, expected_sha256:$w,
              verified:false, installed:false,
              reason:"the downloaded package does not match the pinned hash, so it was not installed. Either the upstream package changed -- the URL serves whatever is current -- or the download is not what it should be. Verify the new package and update STORCLI_SHA256 deliberately."}')
        add_issue "The storcli package did not match its pinned hash" "critical" \
            "Downloaded $got, expected $STORCLI_SHA256. Nothing was installed, and no drive behind the controller can be purged in this run. A binary that erases disks is not run unverified. Either the upstream package changed -- the URL serves whatever is current -- or the download is not what it should be; verify it and update STORCLI_SHA256 deliberately."
        return 0
    fi

    if dpkg -i "$deb" >/dev/null 2>&1; then
        local now
        now=$(find_raid_cli)
        [[ -z "$now" && -x "$STORCLI_INSTALL_PATH" ]] && now="$STORCLI_INSTALL_PATH"
        if [[ -n "$now" ]]; then
            RAID_CLI="$now"
            log "  installed: $now"
            STORCLI_INSTALL=$(jq -n --arg s "$src" --arg g "$got" --arg p "$now" \
                '{attempted:true, source:$s, sha256:$g, verified:true, installed:true, path:$p}')
            audit "storcli fetched from $src (sha256 $got, matched the pinned value) and installed at $now"
        else
            warn "  dpkg reported success but no binary was found"
            STORCLI_INSTALL=$(jq -n --arg s "$src" --arg g "$got" \
                '{attempted:true, source:$s, sha256:$g, verified:true, installed:false,
                  reason:"dpkg succeeded but no storcli binary was found afterwards"}')
        fi
    else
        warn "  dpkg failed"
        STORCLI_INSTALL=$(jq -n --arg s "$src" --arg g "$got" \
            '{attempted:true, source:$s, sha256:$g, verified:true, installed:false,
              reason:"dpkg -i failed"}')
    fi
    return 0
}

collect_media_inventory() {
    log "=== Section 7: storage media inventory ==="
    collect_lsblk
    build_sg_map
    collect_nvme
    emit_nvme_media
    collect_block_media
    install_storcli
    collect_raid_members
    check_inventory_consistency
    # Last, because it reads the assembled media register -- and before
    # plan_methods, which reads the surveyed slot and sanitize support to
    # choose a verb.
    controller_survey
}

###############################################################################
# NVMe records are keyed on the controller, because that is what a sanitise is
# issued against. Capacity and mountpoints are rolled up from the controller's
# namespaces, so a mounted namespace makes its controller a stop condition.
###############################################################################
emit_nvme_media() {
    local n
    n=$(jq 'length' "$WORK_DIR/nvme-ctrls.json" 2>/dev/null || echo 0)
    local i
    for (( i=0; i<n; i++ )); do
        local ctrl serial model fw caps ident base cap mounts
        ctrl=$(jq -r ".[$i].controller" "$WORK_DIR/nvme-ctrls.json")
        ident=$(jq -r ".[$i].identified" "$WORK_DIR/nvme-ctrls.json")
        serial=$(jq -r ".[$i].serial // \"\"" "$WORK_DIR/nvme-ctrls.json")
        model=$(jq -r ".[$i].model // \"\"" "$WORK_DIR/nvme-ctrls.json")
        fw=$(jq -r ".[$i].firmware // \"\"" "$WORK_DIR/nvme-ctrls.json")
        caps=$(jq -c ".[$i].capabilities // {}" "$WORK_DIR/nvme-ctrls.json")

        base=$(basename "$ctrl")
        cap=$(jq --arg b "$base" '[.[] | select(.name | startswith($b + "n")) | .size]
                                  | map(select(type == "number")) | add // ""' \
                 "$WORK_DIR/disks.json" 2>/dev/null | tr -d '"')
        mounts=$(jq -c --arg b "$base" '[.[] | select(.name | startswith($b + "n")) | .mounts[]?]
                                        | unique' "$WORK_DIR/disks.json" 2>/dev/null)
        [[ -z "$mounts" ]] && mounts='[]'

        if [[ "$ident" != "true" ]]; then
            emit_media "$ctrl" "$ctrl" "nvme-controller" "nvme" "nvme" "" "" \
                       "" "" "" "$cap" "$mounts" '{"identified":false}' true
        else
            emit_media "$ctrl" "$ctrl" "nvme-controller" "nvme" "nvme" "" "$model" \
                       "$serial" "" "$fw" "$cap" "$mounts" "$caps" true
        fi
    done
}

###############################################################################
# SECTION 8 -- SELECT THE SANITISATION METHOD
###############################################################################
# One preference order per class of device, strongest first. Two rules govern
# how the result is recorded:
#
#   * The level is stated, not implied. A cryptographic erase, a block erase
#     and an approved overwrite are not the same claim. Purge-capable methods
#     are recorded as "purge"; a full-device overwrite is recorded as "clear",
#     because on an SSD it does not reach over-provisioned blocks and therefore
#     does not satisfy the runbook.
#   * The reason is recorded alongside the choice, so a reader can see what was
#     not available as well as what was used.
#
# storcli/perccli are probed for and never fetched: they are not in the Ubuntu
# archive, and pulling a vendor binary mid-commissioning is not something this
# script will do. That is why a SCSI SANITIZE on the member's own /dev/sg node
# is tried first -- sg3-utils is in the archive.
###############################################################################
raid_slot_for_serial() {
    # storcli prints a per-drive section carrying "SN = <serial>"; the enclosure
    # and slot come from the "Drive /cX/eY/sZ" header above it.
    local cli="$1" serial="$2"
    [[ -n "$cli" && -n "$serial" ]] || return 0
    safe_run "$cli" /call/eall/sall show all \
        | awk -v s="$serial" '
            /^Drive \/c[0-9]+\/e[0-9]+\/s[0-9]+/ { hdr = $2 }
            /SN[[:space:]]*=/ {
                v = $0; sub(/^.*=[[:space:]]*/, "", v); gsub(/[[:space:]]+$/, "", v)
                if (v == s && hdr != "") { print hdr; exit }
            }'
}

plan_methods() {
    log "=== Section 8: sanitisation method per device ==="

    local n
    n=$(jq 'length' "$WORK_DIR/media.json" 2>/dev/null || echo 0)
    : > "$WORK_DIR/media-planned.ndjson"

    local i
    for (( i=0; i<n; i++ )); do
        local rec dev dtype via serial phys caps
        rec=$(jq -c ".[$i]" "$WORK_DIR/media.json")
        dev=$(jq -r '.device' <<<"$rec")
        dtype=$(jq -r '.device_type' <<<"$rec")
        via=$(jq -r '.reached_via' <<<"$rec")
        serial=$(jq -r '.serial_number // ""' <<<"$rec")
        phys=$(jq -r '.physical_media' <<<"$rec")
        caps=$(jq -c '.capabilities // {}' <<<"$rec")

        local method="none" level="none" reason="" cmd="" sanact=""

        if [[ "$phys" != "true" ]]; then
            method="not-applicable"; level="n/a"
            reason="Virtual disk presented by a RAID controller. The physical drives inside it are inventoried and sanitised in their own right."

        elif [[ "$dtype" == "nvme" ]]; then
            local cer ber fnac
            cer=$(jq -r '.nvme_sanitize_crypto // false' <<<"$caps")
            ber=$(jq -r '.nvme_sanitize_block // false' <<<"$caps")
            fnac=$(jq -r '.nvme_format_crypto // false' <<<"$caps")
            if [[ "$cer" == "true" ]]; then
                method="nvme-sanitize-crypto"; level="purge"; sanact=4
                cmd="nvme sanitize $dev --sanact=4"
                reason="Controller reports SANICAP crypto erase. Preferred method."
            elif [[ "$ber" == "true" ]]; then
                method="nvme-sanitize-block"; level="purge"; sanact=2
                cmd="nvme sanitize $dev --sanact=2"
                reason="Controller does not report crypto erase; SANICAP block erase is supported. Documented fallback."
            elif [[ "$fnac" == "true" ]]; then
                # Legacy fallback, and only where FNA explicitly reports
                # cryptographic erase through Format NVM. Issued per namespace.
                local nslist
                nslist=$(jq -r --arg d "$dev" \
                    '.[] | select(.controller == $d) | .namespaces[]?' \
                    "$WORK_DIR/nvme-ctrls.json" 2>/dev/null | tr '\n' ' ')
                [[ -z "${nslist// }" ]] && nslist="${dev}n1"
                method="nvme-format-ses2"; level="purge"
                cmd=$(for ns in $nslist; do printf 'nvme format %s --ses=2 --force; ' "$ns"; done)
                cmd="${cmd%; }"
                reason="NVMe Sanitize is not supported. FNA reports cryptographic erase through Format NVM, which the runbook permits as a legacy fallback."
            elif [[ "$ALLOW_OVERWRITE_FALLBACK" == "true" ]]; then
                method="overwrite"; level="clear"
                cmd="blkdiscard -z $dev"
                reason="No purge-capable NVMe method is supported. An overwrite is a clear, not a purge, and does not satisfy section 8."
            else
                reason="This controller reports no purge-capable sanitisation capability -- neither SANICAP crypto erase nor block erase, nor cryptographic erase through Format NVM. See the remediation register for the routes to a NIST Purge."
            fi

        elif [[ "$dtype" == "raid-member" ]]; then
            local sgnode scsisan rcli slot sv sanmode sed_cap
            sgnode=$(jq -r '.sg_node // ""' <<<"$caps")
            scsisan=$(jq -r '.scsi_sanitize // false' <<<"$caps")
            rcli=$(find_raid_cli)

            # What the controller said about this exact drive, matched on serial.
            sv=$(jq -c --arg sn "$serial" \
                    '.drives[]? | select(.serial_number == $sn)' \
                    "$WORK_DIR/controller_survey.json" 2>/dev/null | head -1)
            [[ -z "$sv" ]] && sv='{}'
            slot=$(jq -r '.slot // ""' <<<"$sv")
            sed_cap=$(jq -r '.sed_capable // ""' <<<"$sv")
            local sed_secured ctl_encrypt
            sed_secured=$(jq -r '.secured // ""' <<<"$sv")
            ctl_encrypt=$(jq -r '.controller_encryption_allowed // "unknown"' \
                             "$WORK_DIR/controller_survey.json" 2>/dev/null)
            local ctl_jbod
            ctl_jbod=$(jq -r '.controller_jbod_support // "unknown"' \
                          "$WORK_DIR/controller_survey.json" 2>/dev/null)
            sanmode=""
            [[ "$(jq -r '.sanitize_block // false' <<<"$sv")" == "true" ]] && sanmode="blockErase"
            [[ "$(jq -r '.sanitize_crypto // false' <<<"$sv")" == "true" ]] && sanmode="cryptoerase"

            # What the drive advertises is not what the controller can issue.
            # Measured on an MR9560-16i: both members report Sanitize Support =
            # CryptoErase, BlockErase while the controller answers "Sanitize is
            # not supported on this controller". Planning off the drive's word
            # alone gets the command rejected, so the controller has the veto.
            local ctlsan
            ctlsan=$(jq -r '.controller_sanitize_support // "unknown"' \
                        "$WORK_DIR/controller_survey.json" 2>/dev/null)
            if [[ "$ctlsan" == "no" && -n "$sanmode" ]]; then
                log "    controller cannot issue a drive sanitize -- ignoring the drive's $sanmode"
                sanmode=""
            fi

            if [[ -n "$sgnode" && "$scsisan" == "true" ]]; then
                method="scsi-sanitize-crypto"; level="purge"
                cmd="sg_sanitize --crypto --quick --wait $sgnode"
                reason="The member answers on its own SCSI generic node ($sgnode) and implements SANITIZE, so a purge can be issued without a vendor tool."
            elif [[ -n "$rcli" && -n "$slot" && -n "$sanmode" && "$ALLOW_RAID_RECONFIG" == "true" ]]; then
                # The controller issues the drive its own SANITIZE. Reaching it
                # means the volume goes first, which is why this needs its own
                # consent -- and the array is rebuilt afterwards.
                method="raid-cli-sanitize-${sanmode}"; level="purge"
                cmd="$rcli $slot start sanitize $sanmode ause"
                reason="The controller reports Sanitize Support = $(jq -r '.sanitize_support // "?"' <<<"$sv") for this drive, and ALLOW_RAID_RECONFIG permits taking it out of the virtual disk to reach it. The array is recreated afterwards from the configuration captured before deletion."

            elif [[ -n "$rcli" && -n "$slot" && -z "$sanmode" && "$sed_cap" == "Yes" \
                    && "$sed_secured" == "Yes" && "$ctl_encrypt" == "yes" \
                    && "$ALLOW_RAID_RECONFIG" == "true" ]]; then
                method="raid-cli-secureerase"; level="purge"
                cmd="$rcli $slot secureerase force"
                reason="The controller reports no drive-level sanitize, but the drive is SED Capable and Secured to a controller that permits encryption, so an instant secure erase is the route. ALLOW_RAID_RECONFIG permits taking it out of the virtual disk to reach it."

            elif [[ -n "$rcli" && -n "$slot" && "$ctl_jbod" == "yes" \
                    && "$ALLOW_RAID_RECONFIG" == "true" ]]; then
                # The drive's own ATA SANITIZE, issued once JBOD exposes it.
                # The device node is resolved at execution time by serial:
                # names are recycled the moment the volume is deleted, and on
                # this hardware the first member took the name the volume had.
                method="ata-sanitize-via-jbod"; level="purge"
                cmd="hdparm --yes-i-know-what-i-am-doing --sanitize-crypto-scramble <resolved by serial in JBOD>"
                reason="This controller cannot issue a drive sanitise (Support Sanitize = No) and cannot do an instant secure erase (Allow Ctrl Encryption = ${ctl_encrypt}, drive Secured = ${sed_secured:-No}). It does support JBOD, which presents the drive as its own SCSI target -- and the drive reports Sanitize Support = $(jq -r '.sanitize_support // "?"' <<<"$sv"). The erase is therefore addressed to the drive itself inside a JBOD window, and the array is rebuilt afterwards."

            elif [[ -n "$rcli" && -n "$slot" && -z "$sanmode" && "$sed_cap" == "Yes" ]]; then
                # The command exists but would be refused: an instant secure
                # erase needs drive security, and this drive was never locked
                # to a controller that cannot manage it.
                method="none"; level="none"
                reason="This controller cannot issue a drive sanitize (Support Sanitize = No), and an instant secure erase is not available either: the drive reports SED Capable = Yes but Secured = ${sed_secured:-No}, and the controller reports Allow Ctrl Encryption = ${ctl_encrypt}. The drive itself does report Sanitize Support = $(jq -r '.sanitize_support // "?"' <<<"$sv") and Cryptographic Erase Capable = $(jq -r '.crypto_erase_capable // "?"' <<<"$sv"), so a Purge is reachable by addressing the drive directly -- see the remediation register."

            elif [[ -n "$rcli" && -n "$slot" && -n "$sanmode" ]]; then
                method="none"; level="none"
                reason="A Purge is reachable -- the controller reports Sanitize Support = $(jq -r '.sanitize_support // "?"' <<<"$sv") for $slot -- but reaching it means deleting the virtual disk, and ALLOW_RAID_RECONFIG is not set. See the remediation register for the sequence, or set that gate and re-run."

            elif [[ -n "$rcli" ]]; then
                slot=$(raid_slot_for_serial "$rcli" "$serial")
                if [[ -n "$slot" ]]; then
                    # Deliberately not executed yet. The drive is a virtual-disk
                    # member, and a controller-issued sanitise is documented as
                    # requiring an Unconfigured Good drive -- so reaching it means
                    # deleting the VD, which is a controller reconfiguration
                    # rather than a media erase and is not something to do behind
                    # the operator's back. The route and its commands are in the
                    # remediation register.
                    method="none"; level="none"
                    reason="A RAID CLI is staged and resolved this serial to $slot, so a Purge is reachable -- but the drive is still a virtual-disk member and a controller-issued sanitise needs it Unconfigured Good. Deleting the virtual disk is a controller reconfiguration, so it is left to the operator: see the remediation register for the exact sequence."
                else
                    reason="A RAID CLI is staged but could not resolve serial ${serial:-unknown} to an enclosure and slot, so no drive can be addressed safely."
                fi
            elif [[ "$ALLOW_OVERWRITE_FALLBACK" == "true" ]]; then
                method="overwrite"; level="clear"
                cmd="blkdiscard -z $dev"
                reason="No purge path to this member: SANITIZE does not pass through the controller and no RAID CLI is staged. An overwrite of the volume is a clear, not a purge."
            else
                reason="No purge command can be addressed to this member from here: SANITIZE does not pass through the RAID controller and no vendor CLI is staged. The drive itself is not shown to be unpurgeable -- see the remediation register and the controller survey."
            fi

        else
            # Direct-attached SATA, SAS, USB or SD.
            local ac ab scsisan sgnode
            ac=$(jq -r '.ata_sanitize_crypto // false' <<<"$caps")
            ab=$(jq -r '.ata_sanitize_block // false' <<<"$caps")
            scsisan=$(jq -r '.scsi_sanitize // false' <<<"$caps")
            sgnode=$(jq -r '.sg_node // ""' <<<"$caps")
            if [[ "$ac" == "true" ]]; then
                method="ata-sanitize-crypto"; level="purge"
                cmd="hdparm --yes-i-know-what-i-am-doing --sanitize-crypto-scramble $dev"
                reason="Drive reports the ATA SANITIZE feature set with CRYPTO_SCRAMBLE_EXT. Preferred method for ATA media."
            elif [[ "$ab" == "true" ]]; then
                method="ata-sanitize-block"; level="purge"
                cmd="hdparm --yes-i-know-what-i-am-doing --sanitize-block-erase $dev"
                reason="CRYPTO_SCRAMBLE_EXT is not supported; BLOCK_ERASE_EXT is. Documented fallback for ATA media."
            elif [[ "$scsisan" == "true" ]]; then
                method="scsi-sanitize-crypto"; level="purge"
                cmd="sg_sanitize --crypto --quick --wait ${sgnode:-$dev}"
                reason="Device implements SCSI SANITIZE."
            elif [[ "$ALLOW_OVERWRITE_FALLBACK" == "true" ]]; then
                method="overwrite"; level="clear"
                cmd="blkdiscard -z $dev"
                reason="No purge-capable method is supported. An overwrite is a clear, not a purge: on solid-state media it does not reach over-provisioned blocks."
            else
                reason="This device reports neither the ATA SANITIZE feature set nor SCSI SANITIZE in this environment. See the remediation register for what to check on a direct port."
            fi
        fi

        audit "plan: ${dev} ${via} -> ${method} (${level})${reason:+ -- $reason}"

        jq -c --arg m "$method" --arg l "$level" --arg r "$reason" --arg c "$cmd" \
              --arg sa "$sanact" \
            '. + {method:$m, method_level:$l, method_reason:$r,
                  nist_level:(if $l == "purge" then "purge"
                              elif $l == "clear" then "clear"
                              elif $l == "n/a" then null
                              else "none" end),
                  command:(if $c == "" then null else $c end),
                  sanact:(if $sa == "" then null else ($sa | tonumber) end)}' \
            <<<"$rec" >> "$WORK_DIR/media-planned.ndjson"
    done

    jq -s '.' "$WORK_DIR/media-planned.ndjson" > "$WORK_DIR/media.json" 2>/dev/null \
        || warn "could not rewrite the media register with the method plan"

    # Raised now, in audit mode too, which is the point of running an audit
    # first: a device a purge cannot reach is worth knowing about before
    # anything has been erased.
    #
    # Two findings, not one, because they are different situations. A device
    # with no method at all always blocks. A device planned for a Clear blocks
    # only while Clear has not been accepted deliberately -- otherwise the
    # option could never be exercised, since the stop condition would refuse
    # the run before the Clear could happen.
    local nomethod clearonly
    nomethod=$(jq -r '[.[] | select(.physical_media and .method_level == "none")
                      | "\(.device) \(.reached_via) \(.serial_number // "no serial")"] | join("; ")' \
                  "$WORK_DIR/media.json" 2>/dev/null)
    clearonly=$(jq -r '[.[] | select(.physical_media and .method_level == "clear")
                       | "\(.device) \(.reached_via) \(.serial_number // "no serial")"] | join("; ")' \
                   "$WORK_DIR/media.json" 2>/dev/null)

    if [[ -n "$nomethod" && "$nomethod" != "null" ]]; then
        add_issue "Media that cannot be purged from this environment" "critical" \
            "$nomethod -- no purge-capable command can be addressed to these devices from here. That is usually a reachability problem rather than an unpurgeable drive: the remediation register lists the routes to a Purge and the controller survey says what is blocking them. The host is not cleared for release until every device reaches Purge."
    fi

    if [[ -n "$clearonly" && "$clearonly" != "null" ]]; then
        if [[ "$ACCEPT_NIST_CLEAR" == "true" ]]; then
            add_issue "Media planned for NIST Clear, not Purge" "warning" \
                "$clearonly -- accepted deliberately (ACCEPT_NIST_CLEAR=true). An overwrite reaches only user-addressable blocks, so on flash media over-provisioned capacity, retired blocks and unmapped FTL copies fall outside it."
        else
            add_issue "Media planned for NIST Clear, not Purge" "critical" \
                "$clearonly -- an overwrite reaches only user-addressable blocks, so on flash it does not render laboratory recovery infeasible. NIST SP 800-88r1 indicates Purge for media leaving organisational control. The remediation register lists the routes to a Purge on these devices."
        fi
    fi
}

###############################################################################
# THE GATE
###############################################################################
# MAAS injects no per-script environment, so the confirmation cannot arrive at
# run time: it is a constant in this file, replaced before upload. The default
# leaves the run in audit mode, which is what makes this script safe to have
# sitting in a commissioning set.
#
# A machine in Commissioning or Released is pre-deployment, so an erase there is
# the expected outcome rather than an accident. The gate is here to stop the
# script running destructively in a context nobody chose -- not to second-guess
# a decommission that was.
###############################################################################
MODE="audit"
PIN_MISMATCH="false"

sanitize_gate() {
    local confirm="$SANITIZE_CONFIRM"

    # The pin comes first, and applies in every mode. Whether this run may
    # touch this machine at all is a prior question to whether an erase was
    # confirmed: the probe reconfigures the controller, and a run pinned to a
    # set of machines must not reconfigure one outside it even while looking.
    if [[ -n "$EXPECT_SYSTEM_SERIAL" ]]; then
        # Membership, not equality: the pin is a list of the chassis this run is
        # allowed to touch. A single serial is just a list of one.
        local want matched=false
        for want in ${EXPECT_SYSTEM_SERIAL//,/ }; do
            [[ -n "$SYSTEM_SERIAL" && "$want" == "$SYSTEM_SERIAL" ]] && { matched=true; break; }
        done
        if [[ "$matched" != "true" ]]; then
            PIN_MISMATCH="true"
            add_issue "Refused: this is not the machine the run was pinned to" "critical" \
                "This chassis reports ${SYSTEM_SERIAL:-no readable serial}, which is not among the serials this run is pinned to ($EXPECT_SYSTEM_SERIAL). Nothing was read from or written to the RAID controller either: a run pinned to a set of machines does not reconfigure one outside it."
            return 1
        fi
        log "Target pinned and matched: $SYSTEM_SERIAL (one of $(printf '%s' "${EXPECT_SYSTEM_SERIAL//,/ }" | wc -w) permitted)"
    fi

    # A hand-run over the console still gets the runbook's prompt. With no TTY
    # -- which is every MAAS run -- there is no prompt and no erase.
    if [[ "$confirm" == "CHANGEME" && -t 0 ]]; then
        read -r -p "Type $SANITIZE_PHRASE to continue: " confirm
    fi

    if [[ "$confirm" != "$SANITIZE_PHRASE" ]]; then
        add_issue "Audit mode -- no device was modified" "info" \
            "SANITIZE_CONFIRM does not read \"$SANITIZE_PHRASE\", so this run inventoried the media and planned a method per device without issuing a destructive command. Edit the constant in the script and re-upload to sanitise."
        return 1
    fi

    if [[ -z "$EXPECT_SYSTEM_SERIAL" ]]; then
        add_note "The target machine was not pinned by serial" \
            "EXPECT_SYSTEM_SERIAL is unset, so the script erased whichever host it ran on. Set it to the DMI system serial -- or to a whitespace- or comma-separated list of them -- to have the run refuse a machine it was not intended for."
    fi

    # Sections 6 and 7 gate section 9: a host that is not understood must not be
    # wiped, because erasing destroys the evidence needed to understand it.
    #
    # A device this environment simply cannot reach is NOT one of those. It
    # leaves the host un-releasable, and it is recorded as critical for that
    # reason -- but it must not veto erasing the devices that can be reached.
    # Refusing to purge a perfectly reachable NVMe because two SATA SSDs sit
    # behind a RAID controller is not caution, it is just no progress.
    if [[ "$STOP_CONDITIONS" -gt 0 ]]; then
        add_stop "Refused to sanitise: unresolved stop conditions" \
            "$STOP_CONDITIONS stop condition(s) from the environment attestation or the media inventory. Section 6 and section 7 both have to be clean before any device is erased."
        return 1
    fi

    MODE="sanitize"
    audit "CONFIRMED: entering sanitise mode on ${SYSTEM_SERIAL:-unknown chassis}"
    return 0
}

###############################################################################
# SECTION 9 -- RUN AND VERIFY
###############################################################################
# Completion is decided on the device's own numeric status, never on the wording
# of a log. The runbook's own script greps for "success" and warns in the same
# breath that the wording varies between nvme-cli versions and vendors; that is
# the flaw closed here. The human-readable log is captured alongside as evidence.
###############################################################################

# Numeric sanitise status from the controller. NVMe SSTAT bits 2:0:
#   0 never sanitised, 1 completed successfully, 2 in progress,
#   3 completed with failure, 4 completed with a deallocate pass.
nvme_sstat() {
    local ctrl="$1" out st
    if [[ "$NVME_JSON_OK" == "true" ]]; then
        out=$(nvme sanitize-log -o json "$ctrl" 2>/dev/null)
        st=$(jq -r '.sstat // .Sanitize_Status // empty' <<<"$out" 2>/dev/null)
    fi
    if [[ -z "$st" || "$st" == "null" ]]; then
        out=$(nvme sanitize-log "$ctrl" 2>/dev/null)
        st=$(sed -n 's/.*SSTAT)[^:]*:[[:space:]]*\([0-9a-fA-Fx]*\).*/\1/p' <<<"$out" | head -1)
    fi
    [[ -z "$st" ]] && { echo ""; return 0; }
    # Accepts 0x101 and 257 alike; anything else is not a status.
    if [[ "$st" =~ ^(0[xX][0-9a-fA-F]+|[0-9]+)$ ]]; then
        echo $(( (st) & 7 ))
    else
        echo ""
    fi
}

nvme_sprog_pct() {
    local ctrl="$1" out sp
    if [[ "$NVME_JSON_OK" == "true" ]]; then
        out=$(nvme sanitize-log -o json "$ctrl" 2>/dev/null)
        sp=$(jq -r '.sprog // empty' <<<"$out" 2>/dev/null)
    fi
    [[ -z "$sp" || "$sp" == "null" ]] && {
        sp=$(nvme sanitize-log "$ctrl" 2>/dev/null \
             | sed -n 's/.*SPROG)[^:]*:[[:space:]]*\([0-9]*\).*/\1/p' | head -1)
    }
    [[ "$sp" =~ ^[0-9]+$ ]] || { echo ""; return 0; }
    echo $(( sp * 100 / 65535 ))
}

# Poll to a real terminal state, bounded. A device that never finishes is
# recorded as a timeout and fails the run; it does not hang commissioning until
# MAAS marks the whole machine failed and takes the evidence with it.
wait_nvme_sanitize() {
    local ctrl="$1" evidence="$2" st pct waited
    local deadline=$(( $(date +%s) + SANITIZE_MAX_WAIT ))
    while :; do
        waited=$(( $(date +%s) - (deadline - SANITIZE_MAX_WAIT) ))
        nvme sanitize-log "$ctrl" >> "$evidence" 2>&1 || true
        st=$(nvme_sstat "$ctrl")
        pct=$(nvme_sprog_pct "$ctrl")

        case "$st" in
            1|4) audit "  $ctrl: SSTAT=$st -- completed successfully"; echo "$st"; return 0 ;;
            3)   audit "  $ctrl: SSTAT=3 -- completed with failure";   echo "$st"; return 1 ;;
            "")  warn  "  $ctrl: sanitise status unreadable"
                 if (( $(date +%s) >= deadline )); then echo ""; return 2; fi ;;
            *)   log   "  $ctrl: SSTAT=$st${pct:+, ${pct}% complete} -- still working (${waited}s)" ;;
        esac

        if (( $(date +%s) >= deadline )); then
            audit "  $ctrl: gave up after ${waited}s with SSTAT=${st:-unreadable}"
            echo "$st"
            return 2
        fi
        sleep "$SANITIZE_POLL_INTERVAL"
    done
}

raid_drive_serial() {
    # Once the volume is gone /dev/sda goes with it, so the drive can no longer
    # be re-identified through smartctl. The controller is the only route left.
    local cli="$1" slot="$2"
    [[ -n "$cli" && -n "$slot" ]] || return 0
    safe_run "$cli" "$slot" show all \
        | sed -n 's/^SN[[:space:]]*=[[:space:]]*//p' | head -1 | tr -d ' \r'
}

raid_sanitize_wait() {
    local cli="$1" slot="$2" evidence="$3"
    local deadline=$(( $(date +%s) + SANITIZE_MAX_WAIT ))
    local out pct seen_progress=false state
    while :; do
        out=$(safe_run "$cli" "$slot" show sanitize)
        printf '%s\n' "$out" >> "$evidence"

        pct=$(grep -oiE '[0-9]{1,3}[[:space:]]*%' <<<"$out" | head -1 | tr -dc '0-9')
        if [[ "$pct" =~ ^[0-9]+$ ]]; then
            seen_progress=true
            if (( pct >= 100 )); then
                audit "  $slot: sanitize reports 100%"
                echo "100%"; return 0
            fi
            log "  $slot: sanitize ${pct}%"
            # A percentage is the better evidence, so it decides: below 100 the
            # operation is still running, whatever the drive state says. The
            # state fallback below is only for controllers that report no
            # percentage at all.
            if (( $(date +%s) >= deadline )); then
                audit "  $slot: gave up at ${pct}%"
                echo "timeout"; return 2
            fi
            sleep "$SANITIZE_POLL_INTERVAL"
            continue
        fi

        if grep -qiE 'fail|error' <<<"$out"; then
            audit "  $slot: sanitize reported a failure"
            echo "failed"; return 1
        fi

        # No percentage. The drive state tells us whether it is still running:
        # storcli shows Sntze while a sanitize is in progress.
        state=$(safe_run "$cli" "$slot" show | grep -oE '\b(Sntze|UGood|UBad|Onln|JBOD)\b' | head -1)
        if [[ "$state" == "Sntze" ]]; then
            seen_progress=true
            log "  $slot: state Sntze -- still sanitising"
        elif [[ "$seen_progress" == "true" ]]; then
            audit "  $slot: left the Sanitize state (now ${state:-unknown})"
            echo "state:${state:-unknown}"; return 0
        elif (( $(date +%s) - (deadline - SANITIZE_MAX_WAIT) > 10 )); then
            # Ten seconds in with no progress ever observed and not in the
            # Sanitize state: the command may have completed instantly, or it
            # may never have started. Neither is evidence of a purge.
            audit "  $slot: no progress and no Sanitize state observed -- cannot evidence completion"
            echo "unverified"; return 3
        fi

        if (( $(date +%s) >= deadline )); then
            audit "  $slot: gave up waiting"
            echo "timeout"; return 2
        fi
        sleep "$SANITIZE_POLL_INTERVAL"
    done
}

# hdparm reports the ATA sanitise state in words, so the mapping is explicit and
# a state we do not recognise is never read as success.
ata_sanitize_state() {
    local dev="$1" out
    out=$(safe_run hdparm --sanitize-status "$dev")
    printf '%s' "$out"
}

wait_ata_sanitize() {
    local dev="$1" evidence="$2" out waited
    local deadline=$(( $(date +%s) + SANITIZE_MAX_WAIT ))
    while :; do
        waited=$(( $(date +%s) - (deadline - SANITIZE_MAX_WAIT) ))
        out=$(ata_sanitize_state "$dev")
        printf '%s\n' "$out" >> "$evidence"

        # The outcome is carried by the "Last Sanitize Operation" line, not by
        # the state: a drive that finished is back at SD0 Sanitize Idle. The
        # ATA states are SD0 idle, SD1 frozen, SD2 in progress, SD3 failed.
        if grep -qiE 'Completed Without Error|SD2 .*Succeed|succeed' <<<"$out"; then
            audit "  $dev: sanitise completed without error"
            echo "completed without error"; return 0
        fi
        if grep -qiE 'Completed With Error|SD3|Operation Failed|fail' <<<"$out"; then
            audit "  $dev: sanitise reported an error"
            echo "failed"; return 1
        fi
        if grep -qiE 'SD1|Frozen' <<<"$out"; then
            audit "  $dev: sanitise is frozen -- the drive will not accept it in this state"
            echo "frozen"; return 1
        fi
        if grep -qiE 'in progress|SD2' <<<"$out"; then
            log "  $dev: sanitise in progress (${waited}s)"
        else
            # Idle with no outcome line at all: nothing has ever run on this
            # drive, so there is nothing to evidence.
            audit "  $dev: idle with no completion record -- $(tr -d '\n' <<<"$out" | tr -s ' ')"
            echo "unverified"; return 1
        fi
        if (( $(date +%s) >= deadline )); then
            audit "  $dev: gave up after ${waited}s"
            echo "timeout"; return 2
        fi
        sleep "$SANITIZE_POLL_INTERVAL"
    done
}

###############################################################################
# Verification. Three separate claims, and the record says which is which:
#
#   * The serial re-read is proof of identity -- that this evidence belongs to
#     the device that was erased and not to one that was swapped or renumbered.
#   * The absence of filesystem signatures and the block hashes are
#     CORROBORATING only. A cryptographic erase leaves random-looking data by
#     design, so non-zero blocks are not a failure and zeroed blocks are not a
#     proof. The device's own status field is the proof.
###############################################################################
first_namespace_of() {
    local ctrl="$1" ns
    ns=$(jq -r --arg d "$ctrl" '.[] | select(.controller == $d) | .namespaces[0] // empty' \
            "$WORK_DIR/nvme-ctrls.json" 2>/dev/null | head -1)
    [[ -z "$ns" ]] && ns="${ctrl}n1"
    [[ -b "$ns" ]] && echo "$ns" || echo ""
}

block_hashes() {
    # First and last mebibyte. Read-only, and skipped silently when the device
    # is not readable as a block device.
    local dev="$1" size skip first="" last=""
    [[ -b "$dev" ]] || { echo '{"first_mib_sha256":null,"last_mib_sha256":null}'; return 0; }
    first=$(dd if="$dev" bs=1M count=1 status=none 2>/dev/null | sha256sum 2>/dev/null | awk '{print $1}')
    size=$(safe_run blockdev --getsize64 "$dev")
    if [[ "$size" =~ ^[0-9]+$ ]] && (( size > 1048576 )); then
        skip=$(( size / 1048576 - 1 ))
        last=$(dd if="$dev" bs=1M count=1 skip="$skip" status=none 2>/dev/null | sha256sum 2>/dev/null | awk '{print $1}')
    fi
    jq -n --argjson f "$(jstr "$first")" --argjson l "$(jstr "$last")" \
        '{first_mib_sha256:$f, last_mib_sha256:$l}'
}

verify_device() {
    local dev="$1" dtype="$2" via="$3" expect_serial="$4"
    local reread="" resid="" probe="$dev"

    case "$dtype" in
        nvme)
            reread=$(safe_run nvme id-ctrl -o json "$dev" | jq -r '.sn // "" | gsub("^ +| +$";"")' 2>/dev/null)
            probe=$(first_namespace_of "$dev")
            ;;
        raid-member)
            # Erased through a JBOD window? The drive is a plain block device
            # for as long as that window is open, and its own SMART identity is
            # the strongest re-identification available.
            if [[ -n "${JBOD_VERIFY_DEV:-}" ]]; then
                reread=$(smart_json -a "$JBOD_VERIFY_DEV" | jq -r '.serial_number // ""' 2>/dev/null)
                probe="$JBOD_VERIFY_DEV"
                JBOD_VERIFY_DEV=""
            fi

            # If the volume has been deleted, /dev/sda is gone and smartctl has
            # no route to the drive. Ask the controller instead.
            local rcli rslot
            rcli=$(find_raid_cli)
            rslot=$(jq -r --arg sn "$expect_serial" \
                       '.drives[]? | select(.serial_number == $sn) | .slot' \
                       "$WORK_DIR/controller_survey.json" 2>/dev/null | head -1)
            if [[ -z "$reread" && -n "$rcli" && -n "$rslot" ]]; then
                reread=$(raid_drive_serial "$rcli" "$rslot")
            fi
            [[ -z "$reread" ]] && reread=$(smart_json -a -d "$via" "$dev" | jq -r '.serial_number // ""' 2>/dev/null)
            probe=""
            ;;
        *)
            reread=$(smart_json -a "$dev" | jq -r '.serial_number // ""' 2>/dev/null)
            ;;
    esac

    if [[ -n "$probe" ]]; then
        resid=$(safe_run blkid -p -o value -s TYPE "$probe")
        [[ -z "$resid" ]] && resid=$(safe_run lsblk -no FSTYPE "$probe" | tr -d ' \n')
    fi

    local hashes='{"first_mib_sha256":null,"last_mib_sha256":null}'
    [[ -n "$probe" ]] && hashes=$(block_hashes "$probe")

    local matches="null"
    if [[ -n "$expect_serial" && -n "$reread" ]]; then
        [[ "$reread" == "$expect_serial" ]] && matches=true || matches=false
    fi

    jq -n --argjson reread "$(jstr "$reread")" --argjson matches "$matches" \
          --argjson resid "$(jstr "$resid")" --argjson hashes "$hashes" \
          --argjson probe "$(jstr "$probe")" \
        '{serial_reread: $reread,
          serial_matches: $matches,
          residual_filesystem_signature: $resid,
          read_back_probe: $probe,
          read_back: $hashes,
          note: "The serial re-read proves the record belongs to the device that was erased. The signature check and block hashes corroborate only: a cryptographic erase leaves random-looking data by design, so the device status field is what evidences the purge."}'
}

###############################################################################
# One device. A failure here is recorded and the sweep continues: section 14
# requires every drive to carry either a successful sanitisation record or a
# a record of what level it reached, and aborting on the first failure
# guarantees that some drives carry neither.
###############################################################################
sanitize_one() {
    local rec="$1"
    local dev dtype via method level cmd sanact serial
    dev=$(jq -r '.device' <<<"$rec")
    dtype=$(jq -r '.device_type' <<<"$rec")
    via=$(jq -r '.reached_via' <<<"$rec")
    method=$(jq -r '.method' <<<"$rec")
    level=$(jq -r '.method_level' <<<"$rec")
    cmd=$(jq -r '.command // ""' <<<"$rec")
    sanact=$(jq -r '.sanact // ""' <<<"$rec")
    serial=$(jq -r '.serial_number // ""' <<<"$rec")

    # Nothing to do: virtual disks, and anything the method plan could not
    # find a route to. Those are already recorded as blocking release.
    if [[ "$level" != "purge" && "$level" != "clear" ]]; then
        jq -c '. + {result: (if .physical_media then "no method available" else "not applicable" end)}' <<<"$rec"
        return 0
    fi

    local evidence="$WORK_DIR/evidence-$(tr -c 'A-Za-z0-9' '_' <<<"${dev}${via}").log"
    : > "$evidence"

    local started ended rc=0 result="failed" status_code="" dur
    started=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
    audit "SANITISE $dev ($via) method=$method level=$level"
    audit "  command: $cmd"
    printf '===== %s  %s =====\ncommand: %s\n' "$started" "$dev" "$cmd" >> "$evidence"

    case "$method" in
        nvme-sanitize-crypto|nvme-sanitize-block)
            if nvme sanitize "$dev" --sanact="$sanact" >> "$evidence" 2>&1; then
                status_code=$(wait_nvme_sanitize "$dev" "$evidence"); rc=$?
                case "$rc" in
                    0) result="success" ;;
                    1) result="failed" ;;
                    *) result="timeout" ;;
                esac
            else
                rc=$?
                audit "  nvme sanitize was rejected by the controller (exit $rc)"
                result="failed"
                status_code=$(nvme_sstat "$dev")
            fi
            ;;

        nvme-format-ses2)
            local ns allok=true
            for ns in $(jq -r --arg d "$dev" '.[] | select(.controller == $d) | .namespaces[]?' \
                             "$WORK_DIR/nvme-ctrls.json" 2>/dev/null); do
                audit "  nvme format $ns --ses=2 --force"
                nvme format "$ns" --ses=2 --force >> "$evidence" 2>&1 || allok=false
            done
            [[ "$allok" == "true" ]] && result="success" || result="failed"
            status_code="format"
            ;;

        ata-sanitize-crypto|ata-sanitize-block)
            if eval "$cmd" >> "$evidence" 2>&1; then
                status_code=$(wait_ata_sanitize "$dev" "$evidence"); rc=$?
                case "$rc" in
                    0) result="success" ;;
                    2) result="timeout" ;;
                    *) result="failed" ;;
                esac
            else
                rc=$?
                audit "  hdparm rejected the sanitise command (exit $rc)"
                result="failed"; status_code="rejected"
            fi
            ;;

        ata-sanitize-via-jbod)
            # The device node is resolved here, not at planning time. Names are
            # recycled the instant the volume is deleted -- measured on this
            # hardware, the first member took the name the volume had -- so the
            # serial is the only safe handle.
            local jdev
            jdev=$(device_for_serial "$serial")
            if [[ -z "$jdev" ]]; then
                audit "  $serial did not appear as a block device in the JBOD window"
                result="failed"; status_code="not exposed in JBOD"
            else
                local jcmd
                jcmd="hdparm --yes-i-know-what-i-am-doing --sanitize-crypto-scramble $jdev"
                audit "  $serial exposed as $jdev"
                audit "  command: $jcmd"
                cmd="$jcmd"
                if eval "$jcmd" >> "$evidence" 2>&1; then
                    status_code=$(wait_ata_sanitize "$jdev" "$evidence"); rc=$?
                    case "$rc" in
                        0) result="success" ;;
                        2) result="timeout" ;;
                        *) result="failed" ;;
                    esac
                else
                    rc=$?
                    audit "  hdparm rejected the sanitise (exit $rc)"
                    result="failed"; status_code="hdparm exit $rc"
                fi
                # Re-identify through the exposed device while the window is
                # still open; once it closes the node goes away again.
                JBOD_VERIFY_DEV="$jdev"
            fi
            ;;

        raid-cli-sanitize-*|raid-cli-secureerase)
            local rcli rslot
            rcli=$(find_raid_cli)
            rslot=$(jq -r --arg sn "$serial" \
                       '.drives[]? | select(.serial_number == $sn) | .slot' \
                       "$WORK_DIR/controller_survey.json" 2>/dev/null | head -1)
            if [[ -z "$rcli" || -z "$rslot" ]]; then
                result="failed"; status_code="no controller CLI or slot for this serial"
            elif eval "$cmd" >> "$evidence" 2>&1; then
                if [[ "$method" == "raid-cli-secureerase" ]]; then
                    # Instant secure erase returns when it is done, so there is
                    # no progress to poll; the identity re-read is the check.
                    result="success"; status_code="secureerase exit 0"
                else
                    status_code=$(raid_sanitize_wait "$rcli" "$rslot" "$evidence"); rc=$?
                    case "$rc" in
                        0) result="success" ;;
                        2) result="timeout" ;;
                        3) result="unverified" ;;
                        *) result="failed" ;;
                    esac
                fi
            else
                rc=$?
                audit "  the controller rejected the command (exit $rc)"
                result="failed"; status_code="controller exit $rc"
            fi
            ;;

        scsi-sanitize-crypto)
            # sg_sanitize --wait blocks until the device reports completion, so
            # its exit status is the terminal state.
            if timeout "$SANITIZE_MAX_WAIT" bash -c "$cmd" >> "$evidence" 2>&1; then
                result="success"; status_code="sg_sanitize exit 0"
            else
                rc=$?
                [[ "$rc" -eq 124 ]] && { result="timeout"; status_code="timed out after ${SANITIZE_MAX_WAIT}s"; } \
                                    || { result="failed";  status_code="sg_sanitize exit $rc"; }
                audit "  sg_sanitize did not complete (exit $rc)"
            fi
            ;;

        raid-cli-secure-erase)
            if timeout "$SANITIZE_MAX_WAIT" bash -c "$cmd" >> "$evidence" 2>&1; then
                result="success"; status_code="raid cli exit 0"
            else
                rc=$?
                [[ "$rc" -eq 124 ]] && { result="timeout"; status_code="timed out after ${SANITIZE_MAX_WAIT}s"; } \
                                    || { result="failed";  status_code="raid cli exit $rc"; }
            fi
            ;;

        overwrite)
            # A clear, not a purge. Recorded as such whatever the outcome.
            if timeout "$OVERWRITE_MAX_SECONDS" blkdiscard -z "$dev" >> "$evidence" 2>&1; then
                result="success"; status_code="blkdiscard exit 0"
            elif timeout "$OVERWRITE_MAX_SECONDS" shred -n1 -z "$dev" >> "$evidence" 2>&1; then
                result="success"; status_code="shred exit 0"
            else
                rc=$?
                [[ "$rc" -eq 124 ]] && { result="timeout"; status_code="timed out after ${OVERWRITE_MAX_SECONDS}s"; } \
                                    || { result="failed";  status_code="overwrite exit $rc"; }
            fi
            ;;

        *)
            result="failed"; status_code="unknown method $method"
            ;;
    esac

    ended=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
    dur=$(( $(date -d "$ended" +%s 2>/dev/null || date +%s) - $(date -d "$started" +%s 2>/dev/null || date +%s) ))
    [[ "$dur" =~ ^[0-9]+$ ]] || dur=0

    local verif
    verif=$(verify_device "$dev" "$dtype" "$via" "$serial")
    local smatch
    smatch=$(jq -r '.serial_matches' <<<"$verif")
    if [[ "$result" == "success" && "$smatch" == "false" ]]; then
        # The erase may well have worked, but not on the drive this record
        # names, and an evidence register that attributes it to the wrong
        # serial is worse than no register.
        audit "  $dev: serial re-read does not match the record -- refusing to call this a success"
        result="unverified"
    fi

    audit "  result: $result (${dur}s)"

    local b64="$WORK_DIR/ev-$$.b64"
    b64_file_bounded "$b64" "$evidence"

    jq -c --arg s "$started" --arg e "$ended" --argjson d "$(jnum "$dur")" \
          --arg sc "$status_code" --arg r "$result" --argjson v "$verif" \
          --arg cmd "$cmd" \
          --rawfile ev "$b64" \
        '. + {command:(if $cmd == "" then .command else $cmd end),
              started_at:$s, completed_at:$e, duration_seconds:$d,
              status_code:(if $sc == "" then null else $sc end),
              result:$r, verification:$v,
              evidence_b64:(if $ev == "" then null else $ev end)}' <<<"$rec"
    rm -f "$b64"
}

###############################################################################
# Taking the drives out of the array, and putting the array back.
#
# A controller-issued sanitise needs the drive Unconfigured Good, so the volume
# is deleted first -- and the array is expected back afterwards, so it is
# recreated from the configuration captured before the deletion. Deleting a
# virtual disk is not itself a wipe: it removes the mapping and leaves the data,
# which is exactly why the sanitise still has to happen.
###############################################################################
RAID_RECONFIG='{"performed":false}'
RAID_STEPS="$WORK_DIR/raid-steps.ndjson"

raid_step() {
    # $1 = what, $2 = command, $3 = exit status, $4 = output file
    local b64="$WORK_DIR/step.b64"
    b64_file_bounded "$b64" "${4:-/dev/null}" 8192
    jq -n --arg what "$1" --arg cmd "$2" --argjson rc "$(jnum "$3")" \
          --arg ts "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" --rawfile out "$b64" \
        '{at:$ts, step:$what, command:$cmd, exit_status:$rc,
          output_b64:(if $out == "" then null else $out end)}' >> "$RAID_STEPS"
    rm -f "$b64"
}

raid_prepare() {
    local cli planned
    cli=$(find_raid_cli)
    # Any method that needs the drive out of the volume, not only the ones the
    # controller issues itself: the JBOD route needs the same preparation.
    planned=$(jq -r '[.[] | select((.method | startswith("raid-cli-"))
                                   or .method == "ata-sanitize-via-jbod")] | length' \
                 "$WORK_DIR/media.json" 2>/dev/null || echo 0)
    [[ "$planned" -gt 0 && -n "$cli" ]] || return 0

    : > "$RAID_STEPS"
    audit "RAID reconfiguration: $planned drive(s) need to leave the virtual disk to be purged"

    # Does any member need the drives exposed to the host, rather than merely
    # taken out of the volume?
    local need_jbod=false
    [[ "$(jq -r '[.[] | select(.method == "ata-sanitize-via-jbod")] | length' \
             "$WORK_DIR/media.json" 2>/dev/null || echo 0)" -gt 0 ]] && need_jbod=true

    local vd rc out="$WORK_DIR/raid-step.out"
    while read -r vd; do
        [[ -z "$vd" ]] && continue
        audit "  deleting virtual disk $vd (removes the mapping only; the data is erased below)"
        "$cli" "$vd" del force > "$out" 2>&1; rc=$?
        raid_step "delete virtual disk" "$cli $vd del force" "$rc" "$out"
        [[ "$rc" -ne 0 ]] && warn "  $vd did not delete (exit $rc)"
    done < <(jq -r '.virtual_disks[]? | "/c0/v\(.vd)"' "$WORK_DIR/controller_survey.json" 2>/dev/null)

    # Any member left Unconfigured Bad or foreign cannot be sanitised, so nudge
    # it back to Unconfigured Good, which is the state the erase requires.
    local slot
    while read -r slot; do
        [[ -z "$slot" ]] && continue
        "$cli" "$slot" set good force > "$out" 2>&1; rc=$?
        raid_step "set drive Unconfigured Good" "$cli $slot set good force" "$rc" "$out"
    done < <(jq -r '.drives[]?.slot' "$WORK_DIR/controller_survey.json" 2>/dev/null)

    if [[ "$need_jbod" == "true" ]]; then
        # The controller-level personality call is not supported on every
        # controller -- measured exit 255 on an MR9560-16i -- and is not needed
        # there, because the per-drive call exposes the drive anyway. So its
        # failure is recorded and not treated as fatal.
        "$cli" /c0 set jbod=on > "$out" 2>&1; rc=$?
        raid_step "enable controller JBOD (optional on some controllers)" "$cli /c0 set jbod=on" "$rc" "$out"
        while read -r slot; do
            [[ -z "$slot" ]] && continue
            "$cli" "$slot" set jbod > "$out" 2>&1; rc=$?
            raid_step "set drive JBOD" "$cli $slot set jbod" "$rc" "$out"
        done < <(jq -r '.drives[]?.slot' "$WORK_DIR/controller_survey.json" 2>/dev/null)
        rescan_scsi
        audit "  drives exposed for a host-issued erase"
    fi

    "$cli" /c0/eall/sall show > "$out" 2>&1; rc=$?
    raid_step "confirm drive states" "$cli /c0/eall/sall show" "$rc" "$out"

    RAID_RECONFIG=$(jq -n --argjson performed true --arg cli "$cli" --argjson jbod "$need_jbod" \
        '{performed:$performed, cli:$cli, jbod_window:$jbod, restored:false}')
    return 0
}

raid_restore() {
    local cli
    cli=$(find_raid_cli)
    [[ "$(jq -r '.performed' <<<"$RAID_RECONFIG")" == "true" ]] || return 0
    [[ -n "$cli" ]] || return 0

    local out="$WORK_DIR/raid-step.out" rc
    # Close the JBOD window first, before any decision about the array: a drive
    # left in JBOD is a controller change this run made and must undo, whereas
    # withholding the array is a deliberate judgement about the drives.
    if [[ "$(jq -r '.jbod_window // false' <<<"$RAID_RECONFIG")" == "true" ]]; then
        local slot
        while read -r slot; do
            [[ -z "$slot" ]] && continue
            "$cli" "$slot" set good force > "$out" 2>&1; rc=$?
            raid_step "return drive to Unconfigured Good" "$cli $slot set good force" "$rc" "$out"
        done < <(jq -r '.drives[]?.slot' "$WORK_DIR/controller_survey.json" 2>/dev/null)
        "$cli" /c0 set jbod=off > "$out" 2>&1; rc=$?
        raid_step "disable controller JBOD (optional on some controllers)" "$cli /c0 set jbod=off" "$rc" "$out"
        rescan_scsi
    fi


    if [[ "$RAID_RESTORE_ARRAY" != "true" ]]; then
        audit "RAID_RESTORE_ARRAY is not true -- leaving the drives unconfigured"
        RAID_RECONFIG=$(jq '. + {restored:false, restore_skipped:"RAID_RESTORE_ARRAY is not true"}' <<<"$RAID_RECONFIG")
        return 0
    fi

    local drives_csv rtype
    drives_csv=$(jq -r '.drive_list // ""' "$WORK_DIR/controller_survey.json" 2>/dev/null)
    rtype=$(jq -r '[.virtual_disks[]?.type] | .[0] // "RAID1"' "$WORK_DIR/controller_survey.json" 2>/dev/null)
    if [[ -z "$drives_csv" || "$drives_csv" == "null" ]]; then
        warn "no drive list recorded -- cannot recreate the array"
        RAID_RECONFIG=$(jq '. + {restored:false, restore_skipped:"no drive list was captured"}' <<<"$RAID_RECONFIG")
        return 0
    fi

    # Only rebuild once every member actually reached a Purge. Putting an array
    # back over a drive that was not erased would hide exactly the thing this
    # run exists to establish.
    #
    # The probe is the exception, and deliberately so: it erased nothing, so
    # leaving the array down would be damage with no purpose. Its whole contract
    # is to put the controller back as it found it.
    local unpurged
    if [[ "$(jq -r '.probe // false' <<<"$RAID_RECONFIG")" == "true" ]]; then
        unpurged=0
        audit "  probe run: restoring the array it borrowed, nothing having been erased"
    else
        unpurged=$(jq -r '[.[] | select(.device_type == "raid-member")
                          | select(.result != "success")] | length' "$WORK_DIR/media.json" 2>/dev/null || echo 1)
    fi
    if [[ "$unpurged" -gt 0 ]]; then
        warn "  $unpurged member(s) did not reach a verified Purge -- not rebuilding the array over them"
        RAID_RECONFIG=$(jq --argjson n "$(jnum "$unpurged")" \
            '. + {restored:false, restore_skipped:("\($n) member(s) did not reach a verified Purge, so the array was left unconfigured rather than rebuilt over an unerased drive")}' <<<"$RAID_RECONFIG")
        add_issue "The array was not rebuilt" "warning" \
            "$unpurged drive(s) behind the controller did not reach a verified Purge, so the drives were left Unconfigured Good rather than an array being put back over them. Resolve the drives, then recreate the array."
        return 0
    fi

    local t="r1"
    [[ "$rtype" =~ ^RAID([0-9]+)$ ]] && t="r${BASH_REMATCH[1]}"
    audit "  recreating the array: $cli /c0 add vd type=$t drives=$drives_csv"
    "$cli" /c0 add vd "type=$t" "drives=$drives_csv" > "$out" 2>&1; rc=$?
    raid_step "recreate virtual disk" "$cli /c0 add vd type=$t drives=$drives_csv" "$rc" "$out"
    if [[ "$rc" -eq 0 ]]; then
        RAID_RECONFIG=$(jq --arg t "$t" --arg d "$drives_csv" \
            '. + {restored:true, restored_as:("type=" + $t + " drives=" + $d)}' <<<"$RAID_RECONFIG")
        audit "  array recreated, empty"
    else
        RAID_RECONFIG=$(jq '. + {restored:false, restore_skipped:"the add vd command failed"}' <<<"$RAID_RECONFIG")
        add_issue "The array could not be recreated" "warning" \
            "The drives were purged, but recreating the virtual disk failed. The drives are Unconfigured Good and the array has to be created by hand."
    fi
    return 0
}

###############################################################################
# THE JBOD PROBE -- reconfigure and look, erase nothing.
###############################################################################
# On the MR9560-16i the controller reports Support Sanitize = No and Allow Ctrl
# Encryption = No, so neither controller route to a member drive exists. The
# drives themselves report Sanitize Support = CryptoErase, BlockErase, and the
# controller reports Support JBOD = Yes -- so the open question is whether ATA
# SANITIZE survives the SCSI-to-ATA translation the controller applies in JBOD.
# That is firmware-dependent and not something to assume either way.
#
# This answers it on the real hardware without erasing anything: capture the
# geometry, drop the volume, expose the drives, read what they will accept, put
# it all back. It runs only in audit mode with ALLOW_RAID_RECONFIG set, so
# "reconfigure and look" cannot be confused with "wipe".
###############################################################################
JBOD_PROBE='{"attempted":false}'
JBOD_VERIFY_DEV=""

rescan_scsi() {
    local h
    for h in /sys/class/scsi_host/host*/scan; do
        [[ -w "$h" ]] && echo '- - -' > "$h" 2>/dev/null
    done
    udevadm settle --timeout=10 >/dev/null 2>&1 || sleep 3
}

# The block device a serial is now exposed as, or nothing.
device_for_serial() {
    local want="$1" name ser
    while read -r name ser; do
        [[ "$ser" == "$want" && -n "$name" ]] && { echo "/dev/$name"; return 0; }
    done < <(lsblk -dno NAME,SERIAL 2>/dev/null)
    echo ""
}

raid_probe_jbod() {
    local cli
    cli=$(find_raid_cli)
    [[ -n "$cli" ]] || return 0

    # Any drive behind the controller is worth probing. The earlier guard asked
    # whether a member was planned as unreachable, which stopped making sense
    # once the JBOD route existed: a member with a plan is exactly the one whose
    # plan an audit should test before anyone relies on it.
    local members
    members=$(jq -r '[.[] | select(.device_type == "raid-member")] | length' \
                 "$WORK_DIR/media.json" 2>/dev/null || echo 0)
    [[ "$members" -gt 0 ]] || return 0

    log "=== JBOD probe: establishing whether the drives accept a purge when exposed ==="
    audit "JBOD PROBE: reconfiguring the controller to read drive capability. No erase is issued."
    : > "$RAID_STEPS"

    local out="$WORK_DIR/raid-step.out" rc vd slot
    while read -r vd; do
        [[ -z "$vd" ]] && continue
        audit "  deleting $vd (geometry already captured; this is not a wipe)"
        "$cli" "$vd" del force > "$out" 2>&1; rc=$?
        raid_step "delete virtual disk (probe)" "$cli $vd del force" "$rc" "$out"
    done < <(jq -r '.virtual_disks[]? | "/c0/v\(.vd)"' "$WORK_DIR/controller_survey.json" 2>/dev/null)

    "$cli" /c0 set jbod=on > "$out" 2>&1; rc=$?
    raid_step "enable controller JBOD" "$cli /c0 set jbod=on" "$rc" "$out"
    while read -r slot; do
        [[ -z "$slot" ]] && continue
        "$cli" "$slot" set jbod > "$out" 2>&1; rc=$?
        raid_step "set drive JBOD" "$cli $slot set jbod" "$rc" "$out"
    done < <(jq -r '.drives[]?.slot' "$WORK_DIR/controller_survey.json" 2>/dev/null)

    rescan_scsi

    # Read what each drive will now accept. All read-only.
    local findings="[]" serial dev caps scsisan
    while read -r slot serial; do
        [[ -z "$serial" ]] && continue
        dev=$(device_for_serial "$serial")
        if [[ -n "$dev" ]]; then
            log "  $slot ($serial) is exposed as $dev"
            caps=$(ata_caps "$dev")
            scsisan=$(scsi_sanitize_supported "$dev")
            safe_run hdparm -I "$dev" >> "$WORK_DIR/jbod-probe.txt"
            safe_run sg_opcodes "$dev" >> "$WORK_DIR/jbod-probe.txt"
        else
            warn "  $slot ($serial) did not appear as a block device in JBOD"
            caps='{}'; scsisan=false
        fi
        findings=$(jq --arg sl "$slot" --arg sn "$serial" --arg dv "${dev:-}" \
                      --argjson c "$caps" --argjson ss "$scsisan" \
            '. + [{slot:$sl, serial_number:$sn,
                   exposed_as:(if $dv == "" then null else $dv end),
                   ata: $c, scsi_sanitize: $ss,
                   purge_reachable: (($c.ata_sanitize_crypto // false)
                                     or ($c.ata_sanitize_block // false)
                                     or $ss),
                   would_use: (if ($c.ata_sanitize_crypto // false) then "hdparm --sanitize-crypto-scramble"
                               elif ($c.ata_sanitize_block // false) then "hdparm --sanitize-block-erase"
                               elif $ss then "sg_sanitize --crypto"
                               else null end)}]' <<<"$findings")
    done < <(jq -r '.drives[]? | "\(.slot) \(.serial_number)"' "$WORK_DIR/controller_survey.json" 2>/dev/null)

    # Put the controller back exactly as it was.
    while read -r slot; do
        [[ -z "$slot" ]] && continue
        "$cli" "$slot" set good force > "$out" 2>&1; rc=$?
        raid_step "return drive to Unconfigured Good" "$cli $slot set good force" "$rc" "$out"
    done < <(jq -r '.drives[]?.slot' "$WORK_DIR/controller_survey.json" 2>/dev/null)
    "$cli" /c0 set jbod=off > "$out" 2>&1; rc=$?
    raid_step "disable controller JBOD" "$cli /c0 set jbod=off" "$rc" "$out"

    RAID_RECONFIG=$(jq -n --arg cli "$cli" '{performed:true, cli:$cli, restored:false, probe:true}')
    raid_restore
    rescan_scsi
    # Every controller command the probe issued, with its exit status -- the
    # evidence that it put things back as well as that it looked.
    if [[ -s "$RAID_STEPS" ]]; then
        RAID_RECONFIG=$(jq --slurpfile st "$RAID_STEPS" '. + {steps:$st}' <<<"$RAID_RECONFIG" 2>/dev/null || echo "$RAID_RECONFIG")
    fi

    b64_file_bounded "$WORK_DIR/jbodprobe.b64" "$WORK_DIR/jbod-probe.txt"
    JBOD_PROBE=$(jq -n --argjson f "$findings" \
                      --rawfile raw "$WORK_DIR/jbodprobe.b64" \
        '{attempted:true, drives:$f,
          raw_b64:(if $raw == "" then null else $raw end),
          conclusion: (
            if ($f | length) == 0 then "no drive was surveyed"
            elif ($f | all(.exposed_as == null)) then
              "no drive appeared as a block device in JBOD, so the host cannot address them even when the controller is told to expose them. This route is closed on this controller."
            elif ($f | all(.purge_reachable)) then
              "every drive accepts a purge when exposed in JBOD, so ATA SANITIZE does survive the translation this controller applies. The erase can be automated."
            else
              "some drives were exposed but did not report a purge-capable command, so ATA SANITIZE does not survive the translation for all of them."
            end)}')
    audit "JBOD PROBE conclusion: $(jq -r '.conclusion' <<<"$JBOD_PROBE")"

    if [[ "$(jq -r '[.drives[] | select(.purge_reachable)] | length' <<<"$JBOD_PROBE")" == "0" ]]; then
        add_issue "The JBOD probe found no purge-capable command on the exposed drives" "critical" \
            "$(jq -r '.conclusion' <<<"$JBOD_PROBE")"
    else
        add_issue "The JBOD probe found a purge route" "info" \
            "$(jq -r '.conclusion' <<<"$JBOD_PROBE") Commands: $(jq -r '[.drives[] | "\(.slot) -> \(.would_use // "none")"] | join("; ")' <<<"$JBOD_PROBE")"
    fi
    return 0
}

sanitize_media() {
    log "=== Section 9: sanitisation ==="
    raid_prepare
    local n i rec
    n=$(jq 'length' "$WORK_DIR/media.json" 2>/dev/null || echo 0)
    : > "$WORK_DIR/media-done.ndjson"

    for (( i=0; i<n; i++ )); do
        rec=$(jq -c ".[$i]" "$WORK_DIR/media.json")
        sanitize_one "$rec" >> "$WORK_DIR/media-done.ndjson"
    done

    jq -s '.' "$WORK_DIR/media-done.ndjson" > "$WORK_DIR/media.json" 2>/dev/null \
        || warn "could not rewrite the media register with the sanitisation results"

    raid_restore
    if [[ -s "$RAID_STEPS" ]]; then
        RAID_RECONFIG=$(jq --slurpfile st "$RAID_STEPS" '. + {steps:$st}' <<<"$RAID_RECONFIG" 2>/dev/null || echo "$RAID_RECONFIG")
    fi

    local bad
    bad=$(jq -r '[.[] | select(.physical_media and (.result | IN("success","not applicable") | not))
                 | "\(.device) \(.reached_via) \(.serial_number // "no serial"): \(.result)"] | join("; ")' \
             "$WORK_DIR/media.json" 2>/dev/null)
    if [[ -n "$bad" && "$bad" != "null" ]]; then
        add_issue "Sanitisation did not complete successfully on every device" "critical" \
            "$bad -- a sanitise that fails or cannot be verified once is not evidence that the drive cannot be purged. The remediation register names the routes to retry it or to reach the device another way."
    fi

    local cleared
    cleared=$(jq -r '[.[] | select(.method_level == "clear" and .result == "success")
                     | "\(.device) \(.serial_number // "no serial")"] | join("; ")' \
                 "$WORK_DIR/media.json" 2>/dev/null)
    if [[ -n "$cleared" && "$cleared" != "null" ]]; then
        # NIST SP 800-88r1 reaches for Purge when media leaves organisational
        # control, and a host being sold leaves it. A verified Clear is recorded
        # honestly and, unless accepted deliberately, leaves the verdict failing
        # rather than passing quietly.
        if [[ "$ACCEPT_NIST_CLEAR" == "true" ]]; then
            add_issue "Some media reached NIST Clear, not Purge" "warning" \
                "$cleared -- accepted deliberately (ACCEPT_NIST_CLEAR=true). An overwrite reaches only user-addressable blocks, so over-provisioned capacity, retired blocks and unmapped FTL copies on flash are outside it. Recorded as Clear so the residual risk is visible to whoever signs the release off."
        else
            add_issue "Some media reached NIST Clear, not Purge" "critical" \
                "$cleared -- an overwrite reaches only user-addressable blocks, so on flash it does not render laboratory recovery infeasible. NIST SP 800-88r1 indicates Purge for media leaving organisational control. The remediation register lists the routes to a Purge on these devices."
        fi
    fi
}

###############################################################################
# THE REMEDIATION REGISTER
###############################################################################
# What to do about a drive this run could not purge -- and it is never "destroy
# it". A drive that resists sanitisation from inside a RAID volume is almost
# always a reachability problem, not an unpurgeable drive: the SATA SSDs in
# these chassis implement ATA SANITIZE perfectly well, and the controller is
# what stands between the host and the command.
#
# So each entry states the NIST level actually reached, what blocked a Purge,
# and the routes available to reach one, strongest first, with the commands.
# A NIST Clear appears last and is labelled as what it is: on flash it reaches
# only user-addressable blocks, so it does not render laboratory recovery
# infeasible and it is not a substitute for a Purge on a host leaving custody.
###############################################################################
build_remediation() {
    local cli vdtarget
    cli=$(find_raid_cli)
    # The real VD id, where the survey found one. Falls back to a placeholder
    # rather than to v0, because v0 may well be somebody else's volume.
    vdtarget=$(jq -r '[.virtual_disks[]? | "/c0/v\(.vd)"] | if length > 0 then join(" ") else "/c0/vX" end' \
                  "$WORK_DIR/controller_survey.json" 2>/dev/null)
    [[ -z "$vdtarget" || "$vdtarget" == "null" ]] && vdtarget="/c0/vX"

    local drives_csv
    drives_csv=$(jq -r '.drive_list // "252:0,252:1"' "$WORK_DIR/controller_survey.json" 2>/dev/null)
    [[ -z "$drives_csv" || "$drives_csv" == "null" ]] && drives_csv="252:0,252:1"

    jq --arg cli "$cli" --arg vd "$vdtarget" --arg drives_csv "$drives_csv" \
       '($cli | if . == "" then "storcli64" else . end) as $cli_name | [ .[]
          | select(.physical_media)
          # A device with a Purge planned that simply was not attempted -- an
          # audit run, or a run that refused -- needs no remediation. What
          # belongs here is a device that cannot reach Purge from this
          # environment, or whose attempt did not end in a verified success.
          | select((.nist_level != "purge")
                   or (.result | IN("failed", "timeout", "unverified")))
          | {device, reached_via, device_type, model, serial_number, firmware,
             method, nist_level_reached: (.nist_level // "none"), result,
             blocker: (
               if .device_type == "raid-member" then
                 "The drive sits inside a RAID volume. Its identity is readable through the controller but SANITIZE is not passed through, and no vendor CLI was staged in the maintenance image, so no purge command can be addressed to it from here."
               elif .result == "timeout" then
                 "The sanitise operation did not report completion within the bounded wait, so completion cannot be evidenced."
               elif .result == "unverified" then
                 "The device could not be re-identified after the operation, so the result cannot be attributed to this serial."
               elif (.result | IN("failed")) then
                 "The device rejected or failed the sanitise operation."
               elif .device_type == "usb" or .device_type == "sd" then
                 "The device reports neither the ATA SANITIZE feature set nor SCSI SANITIZE."
               else
                 "No purge-capable command is supported by this device in this environment."
               end),
             ata_security_supported: (.capabilities.ata_security.supported
                                      // .capabilities.ata_security_supported // null),
             routes: (
               if .device_type == "raid-member" then
                 [ { order: 1,
                     action: "Have the controller issue the sanitize command to the drive",
                     reaches: "purge",
                     requires: (if $cli == "" then "storcli64 staged in the maintenance image, and firmware that supports drive sanitize. It is a Broadcom download, not a package -- apt install will not find it, and megacli is not a substitute on a tri-mode 95xx controller" else "already present: " + $cli end),
                     survey_first: "\(if $cli == "" then "storcli64" else $cli end) /c0/eall/sall show all   # read Sanitize support and SED capability per slot",
                     command: "\($cli_name) /c0/eX/sY start sanitize blockErase ause   # then: show sanitize, to poll to 100%",
                     sequence: [
                       "\($cli_name) /c0/vall show all            # record the array geometry -- deleting the VD loses it",
                       "\($cli_name) \($vd) del force              # not a wipe: removes the mapping, leaves the data",
                       "\($cli_name) /c0/eall/sall show           # both drives must now read UGood",
                       "\($cli_name) /c0/eX/sY start sanitize blockErase ause    # per drive; cryptoerase if the firmware takes it",
                       "\($cli_name) /c0/eX/sY show sanitize      # poll each to 100%",
                       "\($cli_name) /c0 add vd type=r1 drives=\($drives_csv) name=RAID   # give the buyer the mirror back, empty",
                       "re-run this script to evidence the result"
                     ],
                     prerequisite: "The drive has to leave the virtual disk first: \($cli_name) \($vd) del force, then confirm the drive reads UGood. A controller erase is documented as UG-only and unsupported on JBOD drives, so this is not the same preparation as the JBOD route below. Deleting the virtual disk is not itself a wipe -- it removes the mapping and leaves the data in place.",
                     note: "The array is deleted and recreated, not kept: a controller sanitise needs the drive Unconfigured Good, so there is no way to purge the members with the volume in place. The configuration is not data -- it is recorded first and rebuilt afterwards, so the buyer receives the same RAID 1 with nothing on it. The controller issues SANITIZE to the drive itself, so nothing depends on ATA commands surviving a translation layer, and `show sanitize` reports completion as a percentage -- machine-readable evidence. Pass `ause` (Allow Unrestricted Sanitize Exit) or a failed sanitize leaves the drive Unconfigured Bad. `cryptoerase` is the stronger sub-option but Broadcom documents it as not allowed on FDE and ISE drives, which most SATA SSDs are -- read Sanitize Support in the survey and prefer blockErase unless cryptoerase is confirmed." },
                   { order: 2,
                     action: "Cryptographic erase through the controller, for self-encrypting drives",
                     reaches: "purge",
                     requires: "storcli64 (a Broadcom download, not in any Ubuntu repository), and drives reported as SED Capable",
                     survey_first: "\(if $cli == "" then "storcli64" else $cli end) /c0/eall/sall show all   # confirm SED Capable = Yes before relying on this",
                     command: "\(if $cli == "" then "storcli64" else $cli end) /c0/eX/sY secureerase force",
                     prerequisite: "SED Capable must read Yes in the survey, and the drive has to be out of the virtual disk.",
                     note: "Broadcom documents this command as SED-only. Do not reach for `start erase simple|normal|thorough` instead: that is controller-driven pattern writing, so on flash it leaves over-provisioned and remapped NAND untouched and is a Clear, not a Purge." },
                   { order: 3,
                     action: "Set the drives to JBOD so the host can address them, then issue ATA SANITIZE",
                     reaches: "purge",
                     requires: "storcli64 to drop the virtual disk and set JBOD -- or the MegaRAID configuration utility in the BIOS, which can do the same at the console; and SANITIZE has to survive the SCSI-ATA translation the controller applies, which is firmware-dependent",
                     survey_first: "\(if $cli == "" then "storcli64" else $cli end) /c0 show all   # confirm JBOD support; then hdparm -I /dev/sdX once exposed",
                     command: "hdparm --yes-i-know-what-i-am-doing --sanitize-crypto-scramble /dev/sdX   # then --sanitize-status until it reports success",
                     note: "Note the two states are not interchangeable: a controller-issued erase wants the drive Unconfigured Good, while the host can only address it in JBOD. Deleting the virtual disk is a prerequisite for both and is not itself a wipe -- it removes the mapping and leaves the data in place." },
                   { order: 4,
                     action: "Move the drive to a direct SATA port and erase it there",
                     reaches: "purge",
                     requires: "no vendor tooling and no firmware assumptions -- a cable move, and this script re-run",
                     survey_first: "this script, which will then see the drive as direct-attached and read its ATA SANITIZE support with hdparm -I",
                     command: "hdparm --yes-i-know-what-i-am-doing --sanitize-crypto-scramble /dev/sdX",
                     note: "The route with the fewest unknowns, and the only one needing no Broadcom download: re-running this script then evidences the erase itself. Erasing from the BIOS configuration utility instead leaves no machine-readable status to record, and section 10 is explicit that a screenshot is not evidence. The drive is kept, not destroyed." },
                   { order: 5,
                     action: "NIST Clear by full-device overwrite of the volume",
                     reaches: "clear",
                     requires: "ALLOW_OVERWRITE_FALLBACK=true in this script",
                     survey_first: null,
                     command: "blkdiscard -z /dev/sda",
                     caveat: "A Clear, not a Purge. On flash an overwrite reaches only user-addressable blocks, so over-provisioned capacity, retired blocks and unmapped FTL copies are outside it. NIST SP 800-88r1 indicates Purge for media leaving organisational control, so this does not on its own clear the host for sale." } ]
               elif .device_type == "usb" or .device_type == "sd" then
                 [ { order: 1,
                     action: "Re-read the device on a direct port and check its feature set",
                     reaches: "purge",
                     requires: "nothing; some bridges hide the ATA feature set that the drive itself implements",
                     survey_first: "hdparm -I /dev/sdX",
                     command: "hdparm --yes-i-know-what-i-am-doing --sanitize-crypto-scramble /dev/sdX" },
                   { order: 2,
                     action: "NIST Clear by full-device overwrite",
                     reaches: "clear",
                     requires: "ALLOW_OVERWRITE_FALLBACK=true in this script",
                     survey_first: null,
                     command: "blkdiscard -z /dev/sdX",
                     caveat: "A Clear, not a Purge. Sufficient only where the data confidentiality and the destination make Clear the agreed level." } ]
               elif .result == "timeout" or .result == "failed" or .result == "unverified" then
                 [ { order: 1,
                     action: "Re-run the sanitise on this device and read its status again",
                     reaches: "purge",
                     requires: "nothing; a sanitise that fails once is not evidence that the drive cannot be purged",
                     survey_first: "nvme sanitize-log \(.device)   # or hdparm --sanitize-status for ATA",
                     command: .command },
                   { order: 2,
                     action: "Erase the drive on another host or controller",
                     reaches: "purge",
                     requires: "a working path to the device; a controller or backplane fault presents as an unpurgeable drive",
                     survey_first: null,
                     command: .command } ]
               else
                 [ { order: 1,
                     action: "Check the device on a direct port for a feature set this environment could not see",
                     reaches: "purge",
                     requires: "nothing",
                     survey_first: "hdparm -I /dev/sdX; nvme id-ctrl -H /dev/nvmeX",
                     command: null } ]
               end)
          } ]' "$WORK_DIR/media.json" > "$WORK_DIR/remediation.json" 2>/dev/null \
        || echo '[]' > "$WORK_DIR/remediation.json"

    local n
    n=$(jq 'length' "$WORK_DIR/remediation.json" 2>/dev/null || echo 0)
    [[ "$n" -gt 0 ]] && log "  devices needing remediation to reach NIST Purge: $n"
    return 0
}

###############################################################################
# A READ-ONLY SURVEY OF WHAT IS BLOCKING THE RAID MEMBERS
###############################################################################
# Nothing here modifies anything. The point is to answer, with evidence rather
# than assumption, whether the drives inside the volume are purge-capable and
# what it would take to reach them -- and to name the read-only commands that
# would finish the picture where a vendor CLI is needed to ask.
###############################################################################
# Parse `storcli /cN/eall/sall show all` into one record per slot.
#
# The real output, from an MR9560-16i, repeats the slot in a header line for each
# section and puts the fields we want as "Key = Value" beneath:
#
#   Drive /c0/e252/s0 State :
#   SN = S664NE0R123456
#   SED Capable = Yes
#   Sanitize Support = CryptoErase, BlockErase
#
# So fields are keyed on slot and merged across sections. Sanitize Support is
# the field that decides which verb reaches a Purge, which is why this is worth
# parsing rather than reading by eye.
parse_raid_drive_survey() {
    local f="$1"
    [[ -s "$f" ]] || { echo '[]'; return 0; }
    awk '
        /^Drive \/c[0-9]+\/e[0-9]+\/s[0-9]+/ { slot = $2; if (!(slot in seen)) { seen[slot]=1; order[++n]=slot } next }
        slot == "" { next }
        /^SN[[:space:]]*=/                { sn[slot]    = val() ; next }
        /^SED Capable[[:space:]]*=/       { sed[slot]   = val() ; next }
        /^SED Enabled[[:space:]]*=/       { sede[slot]  = val() ; next }
        /^Secured[[:space:]]*=/           { secd[slot]  = val() ; next }
        /^Sanitize Support[[:space:]]*=/  { san[slot]   = val() ; next }
        /^Cryptographic Erase Capable[[:space:]]*=/ { cec[slot] = val() ; next }
        /^Firmware Revision[[:space:]]*=/ { fw[slot]    = val() ; next }
        function val() { v = $0; sub(/^[^=]*=[[:space:]]*/, "", v); gsub(/[[:space:]]+$/, "", v); return v }
        END {
            printf "["
            for (i = 1; i <= n; i++) {
                sl = order[i]
                if (i > 1) printf ","
                printf "{\"slot\":\"%s\",\"serial_number\":\"%s\",\"sed_capable\":\"%s\",",
                       sl, sn[sl], sed[sl]
                printf "\"sed_enabled\":\"%s\",\"secured\":\"%s\",\"firmware\":\"%s\",",
                       sede[sl], secd[sl], fw[sl]
                printf "\"crypto_erase_capable\":\"%s\",", cec[sl]
                printf "\"sanitize_support\":\"%s\"}", san[sl]
            }
            printf "]\n"
        }' "$f" 2>/dev/null | jq 'map(map_values(if . == "" then null else . end))
                                   | map(. + {
                                       sanitize_crypto: ((.sanitize_support // "") | test("CryptoErase"; "i")),
                                       sanitize_block:  ((.sanitize_support // "") | test("BlockErase"; "i"))
                                     })' 2>/dev/null || echo '[]'
}

# Virtual disks, as "DG/VD TYPE State ... Size Name" rows.
parse_raid_vds() {
    local f="$1"
    [[ -s "$f" ]] || { echo '[]'; return 0; }
    awk '
        /^[0-9]+\/[0-9]+[[:space:]]+RAID/ {
            split($1, a, "/")
            if (n++) printf ","
            else printf "["
            printf "{\"dg\":%s,\"vd\":%s,\"type\":\"%s\",\"state\":\"%s\"}", a[1], a[2], $2, $3
        }
        END { if (n) printf "]\n"; else printf "[]\n" }' "$f" 2>/dev/null | jq . 2>/dev/null || echo '[]'
}

controller_survey() {
    local cli
    cli=$(find_raid_cli)

    local ctl_pci
    ctl_pci=$(safe_run lspci -nn | grep -iE 'RAID bus controller|Serial Attached SCSI' | head -5)

    local surveyed='[]' drives='[]' vds='[]'
    local ctl_sanitize="unknown" ctl_jbod="unknown" ctl_encrypt="unknown"
    if [[ -n "$cli" ]]; then
        log "  RAID CLI present ($cli) -- running the read-only survey"
        safe_run "$cli" /c0 show all      > "$WORK_DIR/raid-ctrl.txt"
        safe_run "$cli" /c0/eall/sall show all > "$WORK_DIR/raid-drives.txt"
        safe_run "$cli" /c0/eall/sall show     > "$WORK_DIR/raid-drives-brief.txt"
        safe_run "$cli" /c0/vall show          > "$WORK_DIR/raid-vds.txt"
        safe_run "$cli" /c0/vall show all      > "$WORK_DIR/raid-vds-full.txt"
        surveyed=$(jq -n '["/c0 show all", "/c0/eall/sall show all", "/c0/eall/sall show",
                           "/c0/vall show", "/c0/vall show all"]')
        # Ask the controller what IT can do, not only what the drives claim.
        if grep -qiE '^[[:space:]]*Support Sanitize[[:space:]]*=[[:space:]]*Yes' "$WORK_DIR/raid-ctrl.txt" 2>/dev/null; then
            ctl_sanitize="yes"
        elif grep -qiE '^[[:space:]]*Support Sanitize[[:space:]]*=' "$WORK_DIR/raid-ctrl.txt" 2>/dev/null; then
            ctl_sanitize="no"
        else
            # Not in `show all` on every firmware. Ask directly: the query
            # answers "Sanitize is not supported on this controller" where the
            # feature is absent, which is the same thing we need to know.
            if safe_run "$cli" /c0/eall/sall show sanitize \
               | grep -qiE 'not supported on this controller'; then
                ctl_sanitize="no"
            fi
        fi
        grep -qiE '^[[:space:]]*Support JBOD[[:space:]]*=[[:space:]]*Yes' "$WORK_DIR/raid-ctrl.txt" 2>/dev/null \
            && ctl_jbod="yes" || ctl_jbod="no"
        grep -qiE '^[[:space:]]*Allow Ctrl Encryption[[:space:]]*=[[:space:]]*Yes' "$WORK_DIR/raid-ctrl.txt" 2>/dev/null \
            && ctl_encrypt="yes" || ctl_encrypt="no"
        log "  controller: sanitize=$ctl_sanitize jbod=$ctl_jbod encryption=$ctl_encrypt"

        drives=$(parse_raid_drive_survey "$WORK_DIR/raid-drives.txt")
        vds=$(parse_raid_vds "$WORK_DIR/raid-vds.txt")
        log "  virtual disks: $(jq -r '[.[] | "/c0/v\(.vd) \(.type) \(.state)"] | join(", ")' <<<"$vds")"
        log "  drives surveyed: $(jq 'length' <<<"$drives")"
        local sl
        while read -r sl; do [[ -n "$sl" ]] && log "    $sl"; done < <(
            jq -r '.[] | "\(.slot) \(.serial_number // "?") SED=\(.sed_capable // "?") sanitize=\(.sanitize_support // "none")"' <<<"$drives" 2>/dev/null)
    else
        log "  no RAID CLI staged -- member feature sets cannot be interrogated from here"
    fi

    b64_file_bounded "$WORK_DIR/raidsurvey.b64" "$WORK_DIR/raid-drives.txt"
    b64_file_bounded "$WORK_DIR/raidvds.b64"    "$WORK_DIR/raid-vds-full.txt"

    # The drive list in the EID:Slt form `add vd` wants, derived from the slots
    # the survey found, so the array can be recreated on the same drives.
    local drivelist
    drivelist=$(jq -r '[.[] | .slot | capture("/e(?<e>[0-9]+)/s(?<s>[0-9]+)") | "\(.e):\(.s)"]
                       | join(",")' <<<"$drives" 2>/dev/null)
    [[ -z "$drivelist" || "$drivelist" == "null" ]] && drivelist="252:0,252:1"

    jq -n \
        --arg cli "$cli" \
        --arg pci "$ctl_pci" \
        --argjson surveyed "$surveyed" \
        --argjson install "$STORCLI_INSTALL" \
        --argjson drives "$drives" \
        --argjson vds "$vds" \
        --arg ctlsan "$ctl_sanitize" \
        --arg ctljbod "$ctl_jbod" \
        --arg ctlenc "$ctl_encrypt" \
        --rawfile raidraw "$WORK_DIR/raidsurvey.b64" \
        --rawfile raidvds "$WORK_DIR/raidvds.b64" \
        --arg drivelist "$drivelist" \
        --slurpfile media "$WORK_DIR/media.json" \
        '{
            raid_cli: (if $cli == "" then null else $cli end),
            raid_cli_provenance: $install,
            raid_controllers_pci: (if $pci == "" then null else $pci end),
            commands_run: $surveyed,
            drives: $drives,
            virtual_disks: $vds,
            controller_sanitize_support: $ctlsan,
            controller_jbod_support: $ctljbod,
            controller_encryption_allowed: $ctlenc,
            raw_survey_b64: (if $raidraw == "" then null else $raidraw end),
            virtual_disk_config_b64: (if $raidvds == "" then null else $raidvds end),
            drive_list: $drivelist,
            config_capture_note: "The full virtual-disk configuration is captured here before anything is deleted, because deleting the volume destroys its geometry and the array is expected back afterwards. The array configuration is not data: the drives are purged and the same RAID 1 is then recreated empty.",
            purge_verb: (
              if ($drives | length) == 0 then null
              elif $ctlsan == "no" then
                (if ($drives | all(.sed_capable == "Yes")) then
                   "secureerase force -- the controller reports that it cannot issue a drive sanitize, so start sanitize is not a route here however the drives advertise themselves. Every surveyed drive is SED Capable, so a cryptographic erase through the controller is."
                 else
                   "the controller cannot issue a drive sanitize, and not every drive is SED Capable, so no purge can be addressed to them through this controller."
                 end)
              elif ($drives | all(.sanitize_crypto)) then
                "start sanitize cryptoerase ause -- every surveyed drive reports CryptoErase. Broadcom documents cryptoerase as not allowed on FDE and ISE drives, and these report SED Capable, so fall back to blockErase if the firmware refuses it."
              elif ($drives | all(.sanitize_block)) then
                "start sanitize blockErase ause -- CryptoErase is not reported by every drive; BlockErase is, and is equally a NIST Purge."
              else
                "no drive-level sanitize is reported by the controller; secureerase applies only where SED Capable is Yes."
              end),
            members_seen: ([$media[0][] | select(.device_type == "raid-member")] | length),
            member_ata_security: [ $media[0][]
                | select(.device_type == "raid-member")
                | {serial_number,
                   ata_security_supported: (.capabilities.ata_security.supported
                                            // .capabilities.ata_security_supported // null),
                   ata_security: (.capabilities.ata_security // null)} ],
            note: "Drive identity traverses the controller passthrough because it comes from IDENTIFY DEVICE; the SANITIZE command does not. Where ata_security_supported is true, the drive implements the ATA Security feature set and an erase issued with the drive presented directly would reach NIST Purge. Where it is null, smartctl could not read that word through this controller and the capability is unproven rather than absent.",
            to_complete_the_picture: (if $cli == "" then
                ["storcli64 /c0 show all                 # controller model, firmware, JBOD support",
                 "storcli64 /c0/eall/sall show all       # per slot: SED capability, sanitize support, media type",
                 "Both are read-only. storcli is not in the Ubuntu archive, so staging it in the maintenance image is what makes this answerable in band."]
              else [] end)
        }' > "$WORK_DIR/controller_survey.json" 2>"$WORK_DIR/survey.err" \
        || {
            warn "could not assemble the controller survey"
            [[ -s "$WORK_DIR/survey.err" ]] && head -3 "$WORK_DIR/survey.err" >&2
            echo '{}' > "$WORK_DIR/controller_survey.json"
        }
    if ! jq -e . "$WORK_DIR/controller_survey.json" >/dev/null 2>&1; then
        warn "controller survey is not valid JSON -- recording it as absent"
        echo '{}' > "$WORK_DIR/controller_survey.json"
    fi
    return 0
}

###############################################################################
# SECTION 10 -- THE EVIDENCE REGISTER
###############################################################################
assemble_report() {
    log "=== Section 10: evidence ==="

    # A copy for a live operator at the console. This does NOT survive the
    # maintenance environment: in a MAAS ephemeral boot /root is RAM. The copy
    # that lasts is audit_log_b64 below, which MAAS stores with the run.
    if mkdir -p "$LOG_DIR" 2>/dev/null; then
        cp "$AUDIT_LOG" "$LOG_DIR/$(hostname)-sanitize-$(date -u +%Y%m%dT%H%M%SZ).log" 2>/dev/null \
            || warn "could not write the operator copy of the audit log to $LOG_DIR"
    fi

    b64_file_bounded "$WORK_DIR/audit.b64"  "$AUDIT_LOG"
    b64_file_bounded "$WORK_DIR/lsblk.b64"  "$WORK_DIR/lsblk.txt"
    b64_file_bounded "$WORK_DIR/nvmel.b64"  "$WORK_DIR/nvme-list.txt"
    b64_file_bounded "$WORK_DIR/idctrl.b64" "$WORK_DIR/idctrl.txt"

    local counts
    counts=$(jq '{
        media_records:     length,
        physical_media:    ([.[] | select(.physical_media)] | length),
        nvme_controllers:  ([.[] | select(.device_type == "nvme")] | length),
        raid_members:      ([.[] | select(.device_type == "raid-member")] | length),
        other_media:       ([.[] | select(.physical_media and .device_type != "nvme" and .device_type != "raid-member")] | length),
        purge_planned:     ([.[] | select(.method_level == "purge")] | length),
        purged:            ([.[] | select(.method_level == "purge" and .result == "success")] | length),
        no_method:         ([.[] | select(.physical_media and .method_level == "none")] | length)
    }' "$WORK_DIR/media.json" 2>/dev/null || echo '{}')

    local hash
    hash=$(cat "$WORK_DIR"/media.json "$WORK_DIR"/environment.json 2>/dev/null \
           | sha256sum 2>/dev/null | awk '{print $1}')

    jq -n \
        --arg v "$SCRIPT_VERSION" \
        --arg ts "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" \
        --argjson dur "$(( $(date +%s) - SCRIPT_START ))" \
        --arg mode "$MODE" \
        --arg accept_clear "$ACCEPT_NIST_CLEAR" \
        --arg hash "${hash:-}" \
        --argjson overall "$(jstr "$OVERALL")" \
        --argjson issues "$ISSUES" \
        --argjson counts "$counts" \
        --argjson raid "$RAID_RECONFIG" \
        --argjson jbod "$JBOD_PROBE" \
        --arg ticket "$JIRA_TICKET" \
        --arg engineer "$ENGINEER" \
        --arg phrase "$SANITIZE_PHRASE" \
        --slurpfile env "$WORK_DIR/environment.json" \
        --slurpfile media "$WORK_DIR/media.json" \
        --slurpfile remediation "$WORK_DIR/remediation.json" \
        --slurpfile survey "$WORK_DIR/controller_survey.json" \
        --rawfile auditlog "$WORK_DIR/audit.b64" \
        --rawfile lsblkraw "$WORK_DIR/lsblk.b64" \
        --rawfile nvmelist "$WORK_DIR/nvmel.b64" \
        --rawfile idctrl "$WORK_DIR/idctrl.b64" \
        '{
            report_metadata: {
                script_version: $v,
                script_name: "storage-sanitize",
                generated_at: $ts,
                duration_seconds: $dur,
                mode: $mode,
                runbook: "Remove Host for Sale - Secure Decommissioning Procedure, sections 6 to 10",
                standard: "NIST SP 800-88r1. Levels: Clear sanitises all user-addressable locations against simple non-invasive recovery; Purge renders recovery infeasible using state-of-the-art laboratory techniques. On flash media an overwrite reaches only user-addressable blocks, so it is a Clear -- over-provisioned capacity, retired blocks and unmapped FTL copies are outside it. Purge on flash is a firmware operation: ATA or NVMe SANITIZE, or a cryptographic erase.",
                required_level: "purge",
                accept_clear: ($accept_clear == "true"),
                data_hash_sha256: (if $hash == "" then null else $hash end)
            },
            verdict: { overall: $overall, issues: $issues },
            environment: $env[0],
            inventory: {
                counts: $counts,
                lsblk_b64: (if $lsblkraw == "" then null else $lsblkraw end),
                nvme_list_b64: (if $nvmelist == "" then null else $nvmelist end),
                nvme_id_ctrl_b64: (if $idctrl == "" then null else $idctrl end)
            },
            media: $media[0],
            raid_reconfiguration: $raid,
            jbod_probe: $jbod,
            remediation: $remediation[0],
            controller_survey: $survey[0],
            attestation: {
                hostname: ($env[0].hostname // null),
                system_serial: ($env[0].system_serial // null),
                jira_ticket: (if $ticket == "" then null else $ticket end),
                engineer: (if $engineer == "" then null else $engineer end),
                reviewer: null,
                confirmation_phrase: $phrase,
                out_of_scope: [
                    "Non-disk persistent configuration -- BMC, BIOS/UEFI, TPM and RAID controller settings -- is not covered by this script. Sanitising the drives does not sanitise the server.",
                    "An independent reviewer must confirm this evidence. This script cannot be its own second engineer, so reviewer is null."
                ]
            },
            audit_log_b64: (if $auditlog == "" then null else $auditlog end)
        }' > "$WORK_DIR/report.json" || {
            err "Could not assemble the evidence register"
            return 1
        }

    if ! jq -e . "$WORK_DIR/report.json" >/dev/null 2>&1; then
        err "Assembled evidence register is not valid JSON"
        return 1
    fi

    cat "$WORK_DIR/report.json"

    log "  Mode:                $MODE"
    log "  Physical media:      $(jq -r '.physical_media // 0' <<<"$counts")"
    log "  Purge planned:       $(jq -r '.purge_planned // 0' <<<"$counts")"
    log "  Purged successfully: $(jq -r '.purged // 0' <<<"$counts")"
    log "  Needing remediation: $(jq 'length' "$WORK_DIR/remediation.json" 2>/dev/null || echo 0)"
    log "  Verdict:             $OVERALL"
    return 0
}

###############################################################################
# MAIN
###############################################################################
main() {
    SCRIPT_START=$(date +%s)
    TOOLS_MISSING=""
    NVME_JSON_OK="false"
    SYSTEM_SERIAL=""

    log "=========================================="
    log "NexGen Storage Sanitisation & Evidence v${SCRIPT_VERSION}"
    log "=========================================="

    if [[ "$(id -u)" != "0" ]]; then
        emit_fatal "Must run as root: sanitisation and the capability reads it depends on are privileged operations"
    fi

    if ! install_packages; then
        emit_fatal "jq is unavailable -- cannot assemble the evidence register, and an erase with no evidence is worse than no erase"
    fi
    [[ -n "$TOOLS_MISSING" ]] && add_note "Some tools could not be installed" \
        "Missing:$TOOLS_MISSING. Media that only those tools can reach cannot be inventoried or sanitised here."

    collect_environment
    collect_media_inventory
    plan_methods

    if sanitize_gate; then
        sanitize_media
    else
        audit "AUDIT MODE: no device was modified"
        if [[ "$ALLOW_RAID_RECONFIG" == "true" && "$PIN_MISMATCH" != "true" ]]; then
            raid_probe_jbod
        elif [[ "$PIN_MISMATCH" == "true" ]]; then
            log "=== Pinned to another chassis: not probing the controller either ==="
        fi
    fi

    build_remediation

    local ok=true
    assemble_report || ok=false

    rm -rf "$WORK_DIR"

    log "=========================================="
    log "Complete. Total time: $(( $(date +%s) - SCRIPT_START ))s"
    log "=========================================="

    [[ "$ok" == "false" ]] && exit 1
    [[ "$OVERALL" == "FAIL" ]] && exit 1
    exit 0
}

main "$@"
