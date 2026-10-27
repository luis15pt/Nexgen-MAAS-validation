#!/bin/bash
# --- Start MAAS Metadata ---
# name: 90-nexgen-gpu-install-595-13
# title: NexGen GPU Driver 595 + CUDA 13 + DCGM 4.x Installation
# description: Installs nvidia-driver-595-server-open, cuda-toolkit-12-8,
#   DCGM 4.x (datacenter-gpu-manager-4-cuda13), and support tools.
#   Before loading the driver, reads Confidential Computing mode on every GPU
#   with NVIDIA gpu-admin-tools and switches it off where a previous tenant
#   left it on (the mode persists in the GPU across reboots and reimages, and
#   the host driver refuses a CC-mode GPU). A switch voids this run: the
#   later scripts halt and commissioning must be repeated.
#   Enables persistence mode, loads kernel modules, starts DCGM service.
#   Must run before 92-inventory, 98-stress-test and 99-burn-in.
#   Override at runtime: NVIDIA_DRIVER=... CUDA_TOOLKIT=... DCGM_CUDA_MAJOR=...
#   GPU_ADMIN_TOOLS_REF=<commit> GPU_ADMIN_TOOLS_DIR=<checkout>
# script_type: commissioning
# hardware_type: gpu
# timeout: 00:20:00
# destructive: false
# may_reboot: false
# --- End MAAS Metadata ---

set -o pipefail
trap 'warn "Command failed at line $LINENO (exit code $?)"' ERR

###############################################################################
# CONFIG
###############################################################################
NVIDIA_DRIVER="${NVIDIA_DRIVER:-nvidia-driver-595-server-open}"
CUDA_TOOLKIT="${CUDA_TOOLKIT:-cuda-toolkit-12-8}"
DCGM_CUDA_MAJOR="${DCGM_CUDA_MAJOR:-13}"
# Seconds to wait for the NVSwitch fabric to reach "Completed" after Fabric
# Manager starts. Blackwell/NVL fabric training can take a while; a restart +
# IMEX nudge is attempted at the halfway mark.
FABRIC_READY_TIMEOUT="${FABRIC_READY_TIMEOUT:-300}"
# The first nvidia-smi after modprobe is what boots GSP on every GPU, so it can
# block for a minute or more on 8 Blackwell cards. NVSMI_CALL_TIMEOUT bounds a
# single call; NVSMI_READY_TIMEOUT bounds the retry window for fast failures.
NVSMI_CALL_TIMEOUT="${NVSMI_CALL_TIMEOUT:-240}"
NVSMI_READY_TIMEOUT="${NVSMI_READY_TIMEOUT:-180}"
# NVIDIA gpu-admin-tools, used to read and clear Confidential Computing mode
# before the driver loads (see reset_confidential_compute). Pinned to one of
# NVIDIA's dated release tags so every node runs the same tool and the pin is
# readable; a branch name or commit hash works in the same slot. The fetch
# failing is a WARN with the reason, never a hard failure of the install.
# GPU_ADMIN_TOOLS_DIR points at an existing checkout and skips the fetch.
GPU_ADMIN_TOOLS_REF="${GPU_ADMIN_TOOLS_REF:-v2026.09.29}"
GPU_ADMIN_TOOLS_URL="${GPU_ADMIN_TOOLS_URL:-https://github.com/NVIDIA/gpu-admin-tools/archive/${GPU_ADMIN_TOOLS_REF}.tar.gz}"
GPU_ADMIN_TOOLS_DIR="${GPU_ADMIN_TOOLS_DIR:-}"
CC_TOOL_TIMEOUT="${CC_TOOL_TIMEOUT:-300}"
# Marker the later GPU scripts consult (same path as script 91 writes). Set
# when this run cannot produce usable evidence and must be repeated.
NEXGEN_HALT_FILE="${NEXGEN_HALT_FILE:-/run/nexgen-commissioning-halt}"
WORK_DIR="/tmp/gpu-install-$$"
SCRIPT_VERSION="2.2.0"

###############################################################################
# LOGGING
###############################################################################
log()  { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [INFO]  $*" >&2; }
warn() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [WARN]  $*" >&2; }
err()  { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [ERROR] $*" >&2; }

mkdir -p "$WORK_DIR"

###############################################################################
# HELPER: Parse driver + CUDA from nvidia-smi header (always works)
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
# HELPER: Get DCGM version (4.x uses dcgmi --version or dcgmi -v)
###############################################################################
get_dcgm_version() {
    DCGM_VER=$(dcgmi --version 2>/dev/null | grep -oP '[0-9]+\.[0-9]+\.[0-9]+' | head -1 || true)
    [[ -z "$DCGM_VER" ]] && DCGM_VER=$(dcgmi -v 2>/dev/null | grep -oP '[0-9]+\.[0-9]+\.[0-9]+' | head -1 || true)
    [[ -z "$DCGM_VER" ]] && DCGM_VER=$(dcgmi version 2>/dev/null | grep -oP '[0-9]+\.[0-9]+\.[0-9]+' | head -1 || true)
    [[ -z "$DCGM_VER" ]] && DCGM_VER="unknown"
}

###############################################################################
# HELPER: Detect NVSwitch fabric (does this node need Fabric Manager?)
###############################################################################
# B300/NVL systems expose NO NVSwitch PCI or /dev nodes, and the
# --query-gpu=fabric.state CSV field is unreliable on this driver -- but the
# GPUs DO report a "Fabric" section in `nvidia-smi -q`. That State line is the
# reliable signal. _fabric_states echoes one State per GPU (e.g. "In Progress"
# / "Completed" / "N/A"); empty output => GPUs are not fabric-attached.
_fabric_states() {
    nvidia-smi -q 2>/dev/null | awk '
        /^[[:space:]]*Fabric[[:space:]]*$/ {f=1; next}
        f && /State[[:space:]]*:/ { sub(/^[^:]*:[[:space:]]*/, ""); print; f=0 }'
}

detect_nvswitch() {
    local dev_count fab
    dev_count=$(ls -1 /dev/nvidia-nvswitch[0-9]* 2>/dev/null | wc -l)
    NVSWITCH_COUNT=$(( dev_count + 0 ))
    fab=$(_fabric_states)
    # Fabric node if a /dev nvswitch node exists OR any GPU reports a real
    # (non-N/A) fabric State. Plain PCIe cards report neither, so they skip
    # the fabric gate (FM is still installed -- harmless there).
    if [[ "$NVSWITCH_COUNT" -gt 0 ]] \
       || echo "$fab" | grep -viqE '^[[:space:]]*(n/?a)?[[:space:]]*$'; then
        FABRIC_REQUIRED=true
    else
        FABRIC_REQUIRED=false
    fi
}

###############################################################################
# HELPER: is the NVSwitch fabric fully trained? (all GPUs "Completed")
###############################################################################
# Sets FABRIC_STATE to the first reported state; returns 0 iff every GPU is
# "Completed".
fabric_is_ready() {
    local states total completed
    states=$(_fabric_states)
    [[ -z "$states" ]] && { FABRIC_STATE="unknown"; return 1; }
    total=$(echo "$states" | grep -c .)
    completed=$(echo "$states" | grep -ci 'completed')
    FABRIC_STATE=$(echo "$states" | head -1 | tr -d '[:space:]')
    [[ "$total" -gt 0 && "$completed" -eq "$total" ]]
}

###############################################################################
# HELPER: wait for the fabric to finish training (with a restart nudge)
###############################################################################
# Polls fabric_is_ready up to FABRIC_READY_TIMEOUT seconds. At the halfway
# mark, if still not ready, restarts Fabric Manager (and starts IMEX if it's
# installed but stopped -- the all-zero Cluster UUID/Clique signature). Returns
# 0 when the fabric reaches Completed, 1 on timeout.
wait_for_fabric() {
    local timeout="${FABRIC_READY_TIMEOUT:-300}"
    local waited=0 restarted=false
    # Blackwell fabric (NVLink5) trains via NVLSM, which rides on ib_umad --
    # make sure it's loaded before we wait, or the fabric never completes.
    lsmod 2>/dev/null | grep -q '^ib_umad' || modprobe ib_umad 2>/dev/null || true
    log "Waiting up to ${timeout}s for NVSwitch fabric to reach Completed..."
    local fm_installed=false
    dpkg -l 'nvidia-fabricmanager*' 2>/dev/null | grep -q '^ii' && fm_installed=true
    while :; do
        if fabric_is_ready; then
            log "Fabric ready (state=$FABRIC_STATE) after ${waited}s"
            return 0
        fi
        # A hard-failed unit will never train the fabric, no matter how long
        # we poll -- restart it immediately (once); if it fails AGAIN, dump
        # the journal and bail early instead of burning the full timeout.
        if $fm_installed && systemctl is-failed --quiet nvidia-fabricmanager 2>/dev/null; then
            if ! $restarted; then
                warn "nvidia-fabricmanager unit is in 'failed' state after ${waited}s -- reloading ib_umad + restarting..."
                modprobe ib_umad 2>/dev/null || true
                systemctl reset-failed nvidia-fabricmanager >/dev/null 2>&1 || true
                systemctl restart nvidia-fabricmanager >/dev/null 2>&1 || true
                restarted=true
            elif [[ $waited -ge 15 ]]; then
                err "nvidia-fabricmanager failed again after restart -- aborting fabric wait early"
                journalctl -u nvidia-fabricmanager --no-pager 2>/dev/null | tail -20 >&2 || true
                break
            fi
        elif ! $restarted && [[ $waited -ge $(( timeout / 2 )) ]] && $fm_installed; then
            # Unit is up but the fabric is stuck training -- give FM one nudge.
            warn "Fabric still '${FABRIC_STATE}' after ${waited}s -- reloading ib_umad + restarting nvidia-fabricmanager..."
            modprobe ib_umad 2>/dev/null || true
            systemctl restart nvidia-fabricmanager >/dev/null 2>&1 || true
            restarted=true
        fi
        [[ $waited -ge $timeout ]] && break
        sleep 5
        waited=$(( waited + 5 ))
    done
    err "NVSwitch fabric did NOT reach Completed within ${timeout}s (state=${FABRIC_STATE})"
    return 1
}

###############################################################################
# FABRIC MANAGER (required on NVSwitch/NVL systems for CUDA to initialize)
###############################################################################
# We install Fabric Manager on every node (harmless on PCIe cards), start it,
# and -- only on nodes that actually have NVSwitches -- verify the fabric
# reaches "Completed".  On a PCIe card FM simply won't stay running and that is
# reported as OK, never a failure.
#
# CRITICAL: the FM package version must EXACTLY match the running driver
# (X.Y.Z), or nvidia-fabricmanager.service starts then immediately exits with
# "fabric manager version A doesn't match driver version B".  We derive the
# version from the RUNNING driver (nvidia-smi), not the pinned package name.
setup_fabric_manager() {
    log "=== Fabric Manager setup ==="

    export DEBIAN_FRONTEND=noninteractive
    export NEEDRESTART_MODE=l
    export NEEDRESTART_SUSPEND=1

    # Defaults for the JSON report (updated as we go)
    FM_INSTALLED=false
    FM_VERSION="not installed"
    FM_SERVICE_ACTIVE=false
    FABRIC_STATE="N/A"
    FABRIC_READY=false

    # Driver is loaded by now, so both NVSwitch signals are valid
    detect_nvswitch
    log "NVSwitch devices detected: ${NVSWITCH_COUNT} (fabric_required=${FABRIC_REQUIRED})"

    # -- Derive FM branch from the RUNNING driver (not the pinned name) --
    local fm_branch=""
    if [[ -n "$SMI_DRIVER" && "$SMI_DRIVER" != "unknown" ]]; then
        fm_branch="${SMI_DRIVER%%.*}"
    else
        local pkgver
        pkgver=$(dpkg-query -W -f='${Version}' 'nvidia-driver-*-server-open' 2>/dev/null \
            | grep -oP '^[0-9]+' | head -1)
        fm_branch="${pkgver:-$(echo "$NVIDIA_DRIVER" | grep -oP 'nvidia-driver-\K[0-9]+' || true)}"
        warn "SMI_DRIVER unknown -- deriving Fabric Manager branch as '${fm_branch:-?}' from fallback"
    fi

    if [[ -z "$fm_branch" ]]; then
        warn "Could not determine driver branch -- skipping Fabric Manager install"
        return 0
    fi

    # -- Resolve the exact repo version matching the driver X.Y.Z --
    local fm_pkg="nvidia-fabricmanager-${fm_branch}"
    local fm_exact=""
    if [[ -n "$SMI_DRIVER" && "$SMI_DRIVER" != "unknown" ]]; then
        fm_exact=$(apt-cache madison "$fm_pkg" 2>/dev/null | awk '{print $3}' \
            | grep -F "$SMI_DRIVER" | head -1 || true)
    fi

    # -- Install: exact-version pinned first, then unpinned candidate --
    if dpkg --list "$fm_pkg" 2>/dev/null | grep -q '^ii'; then
        log "$fm_pkg already installed"
    elif [[ -n "$fm_exact" ]]; then
        log "Installing ${fm_pkg}=${fm_exact} (matched to driver ${SMI_DRIVER})..."
        apt-get install -y -qq "${fm_pkg}=${fm_exact}" 2>&1 | tail -5 >&2 \
            || warn "Exact-version FM install failed -- will retry unpinned"
    fi
    if ! dpkg --list "$fm_pkg" 2>/dev/null | grep -q '^ii'; then
        log "Installing ${fm_pkg} (candidate version)..."
        apt-get install -y -qq "$fm_pkg" 2>&1 | tail -5 >&2 \
            || warn "Fabric Manager install failed (${fm_pkg}) -- fabric may not initialize"
    fi

    # -- Verify install + assert version match --
    local installed_ver
    installed_ver=$(dpkg-query -W -f='${Version}' "$fm_pkg" 2>/dev/null \
        | grep -oP '[0-9]+\.[0-9]+\.[0-9]+' | head -1 || true)
    if [[ -n "$installed_ver" ]]; then
        FM_INSTALLED=true
        FM_VERSION="$installed_ver"
        log "Fabric Manager installed: $FM_VERSION"
        if [[ -n "$SMI_DRIVER" && "$SMI_DRIVER" != "unknown" && "$installed_ver" != "$SMI_DRIVER" ]]; then
            warn "Fabric Manager $installed_ver != driver $SMI_DRIVER -- service may refuse to start (version mismatch)"
        fi
    else
        warn "Fabric Manager package not installed"
        [[ "$FABRIC_REQUIRED" == "true" ]] && \
            warn "  This is an NVSwitch node -- CUDA/DCGM WILL fail without Fabric Manager"
        return 0
    fi

    # -- Blackwell (NVLink5 / 4th-gen NVSwitch) fabric prerequisites --
    # B200/B300 train the fabric through NVLSM (NVLink Subnet Manager), which FM
    # launches under its own systemd unit. NVLSM needs the nvlsm/libnvsdm
    # packages and the ib_umad kernel module; without them the fabric stays
    # "training in progress" forever (all-zero Cluster UUID, CUDA 802). These
    # must be in place BEFORE FM starts. Only relevant on fabric nodes.
    if [[ "$FABRIC_REQUIRED" == "true" ]]; then
        if ! dpkg -l nvlsm 2>/dev/null | grep -q '^ii'; then
            log "Installing NVLSM stack (nvlsm libnvsdm)..."
            apt-get install -y -qq nvlsm libnvsdm 2>&1 | tail -3 >&2 \
                || warn "nvlsm/libnvsdm not installable -- Blackwell fabric needs the DOCA-OFED repo"
        fi
        # nvidia-fabricmanager-start.sh probes the CX bridge ports with
        # `ibstat` (infiniband-diags) to pick the NVLSM management port.
        # Without it the ExecStart wrapper exits 1 EVERY time ("ibstat
        # command not found"), FM never launches, and the fabric sits at
        # "In Progress" until the wait times out.  Hard prerequisite.
        if ! command -v ibstat &>/dev/null; then
            log "Installing infiniband-diags (ibstat -- required by FM start wrapper)..."
            apt-get install -y -qq infiniband-diags 2>&1 | tail -3 >&2 || true
        fi
        if command -v ibstat &>/dev/null; then
            log "ibstat available: $(command -v ibstat)"
        else
            warn "ibstat STILL missing -- nvidia-fabricmanager-start.sh WILL exit 1 and the fabric will never train"
            warn "  Install the infiniband-diags package (or full DOCA-OFED) in the base image"
        fi
        if modprobe ib_umad 2>/dev/null; then
            log "ib_umad kernel module loaded (NVLSM management path)"
            echo "ib_umad" > /etc/modules-load.d/nvidia-nvlsm.conf 2>/dev/null || true
        else
            warn "Could NOT load ib_umad -- NVLSM cannot train the NVLink fabric"
            warn "  Install DOCA-OFED (provides ib_umad) BEFORE the NVIDIA driver, or modprobe ib_umad manually"
        fi
        local fm_cfg="/usr/share/nvidia/nvswitch/fabricmanager.cfg"
        if [[ -f "$fm_cfg" ]]; then
            local fmode
            fmode=$(grep -oP '^\s*FABRIC_MODE\s*=\s*\K[0-9]+' "$fm_cfg" 2>/dev/null | head -1)
            if [[ -n "$fmode" && "$fmode" != "0" ]]; then
                warn "FABRIC_MODE=$fmode in $fm_cfg (expected 0 for bare metal) -- fabric may wait for a hypervisor handshake that never comes"
            fi
        fi
    fi

    # -- Start the service (restart so it re-inits with ib_umad/NVLSM present) --
    log "Starting nvidia-fabricmanager service..."
    systemctl enable nvidia-fabricmanager >/dev/null 2>&1 || true
    systemctl restart nvidia-fabricmanager >/dev/null 2>&1 \
        || warn "systemctl restart nvidia-fabricmanager failed"
    if systemctl is-active --quiet nvidia-fabricmanager 2>/dev/null; then
        FM_SERVICE_ACTIVE=true
        log "nvidia-fabricmanager service active"
    else
        warn "nvidia-fabricmanager service not active"
    fi

    # -- PCIe node: FM not required; inactivity is expected, never a fail --
    if [[ "$FABRIC_REQUIRED" != "true" ]]; then
        log "PCIe topology -- Fabric Manager not required; not gating on fabric state"
        FABRIC_STATE="N/A"
        FABRIC_READY=true    # not-applicable == not a problem for the verdict
        return 0
    fi

    # -- NVSwitch node: wait for the fabric to finish training --
    if wait_for_fabric; then
        FABRIC_READY=true
    else
        FABRIC_READY=false
        # Dump the "why" to stderr -- this shows up in MAAS output when the node
        # is not SSH-reachable. "training in progress" + all-zero Cluster UUID
        # usually means IMEX/clique config; a link error means hardware.
        warn "--- Fabric Manager diagnostics ---"
        echo "ib_umad loaded: $(lsmod 2>/dev/null | grep -c '^ib_umad')" >&2
        dpkg -l 2>/dev/null | grep -iE 'nvlsm|libnvsdm|doca-ofed|fabricmanager' >&2 || true
        systemctl status nvidia-fabricmanager --no-pager >&2 2>&1 || true
        journalctl -u nvidia-fabricmanager --no-pager 2>/dev/null | tail -50 >&2 || true
        tail -50 /var/log/fabricmanager.log >&2 2>&1 || true
        nvidia-smi -q 2>/dev/null | grep -A6 -i 'Fabric' >&2 || true
        warn "--- end Fabric Manager diagnostics ---"
    fi
    return 0
}

###############################################################################
# DETECT NVIDIA HARDWARE
###############################################################################
detect_gpus() {
    log "=== Detecting NVIDIA GPUs ==="

    # No `grep -q` here: it closes the pipe on the first match, lspci dies of
    # SIGPIPE on a long device list, and pipefail then fails the whole test
    # (this is why big HPE boxes logged "detected via sysfs").
    if [[ -n "$(lspci 2>/dev/null | grep -i "nvidia")" ]]; then
        log "NVIDIA GPU(s) detected via lspci"
        return 0
    elif [[ -n "$(lspci -n 2>/dev/null | grep -i "10de:")" ]]; then
        log "NVIDIA GPU(s) detected via vendor ID 10de"
        update-pciids 2>/dev/null >&2 || true
        return 0
    elif ls /sys/bus/pci/devices/*/vendor 2>/dev/null | xargs grep -l "0x10de" &>/dev/null; then
        log "NVIDIA GPU(s) detected via sysfs"
        update-pciids 2>/dev/null >&2 || true
        return 0
    fi

    err "No NVIDIA GPUs detected"
    return 1
}

###############################################################################
# INSTALL PACKAGES
###############################################################################
###############################################################################
# HELPER: Choose the driver package to install. Prints it on stdout.
#
# Keeps the pin unless the pinned package is genuinely absent from the index.
# The old check piped `apt-cache policy` into `grep -q` under pipefail, so a
# non-zero apt-cache exit failed it even with the Candidate line printed; with
# stderr discarded and the dump cut to `head -5`, nothing showed why. Any newer
# -server-open branch then won, so every 595 run silently installed 615.
###############################################################################
resolve_driver_pkg() {
    local pinned="$1" policy rc candidate newest
    # Inside `if` so a non-zero exit is captured, not reported by the ERR trap.
    if policy=$(apt-cache policy "$pinned" 2>&1); then rc=0; else rc=$?; fi
    candidate=$(awk '$1 == "Candidate:" {print $2; exit}' <<<"$policy")

    if [[ -n "$candidate" && "$candidate" != "(none)" ]]; then
        if [[ $rc -ne 0 ]]; then
            warn "apt-cache policy exited $rc but lists candidate $candidate -- keeping the pin. Full output:"
            printf '%s\n' "$policy" >&2
        fi
        log "Driver candidate: $pinned $candidate"
        echo "$pinned"
        return 0
    fi

    warn "$pinned has no installable candidate (apt-cache policy exit $rc). Full output:"
    printf '%s\n' "$policy" >&2

    # Listed in the index under its own name: a candidate problem, not a
    # missing branch. Keep the pin and let apt-get install say what is wrong,
    # rather than swap the driver version under the acceptance run.
    if [[ -n "$(apt-cache search --names-only "^${pinned}\$" 2>/dev/null)" ]]; then
        warn "apt-cache search lists $pinned -- keeping the pin"
        echo "$pinned"
        return 0
    fi

    # Genuinely absent (e.g. the branch is not yet published): degrade to the
    # newest -server-open branch. awk reads all input, so no early pipe close.
    newest=$(apt-cache search --names-only '^nvidia-driver-[0-9]+-server-open$' 2>/dev/null \
        | grep -oP 'nvidia-driver-\K[0-9]+' | sort -rn | awk 'NR == 1')
    if [[ -n "$newest" ]]; then
        warn "Falling back to nvidia-driver-${newest}-server-open -- $pinned is not in the package index"
        echo "nvidia-driver-${newest}-server-open"
    else
        warn "No nvidia-driver-*-server-open in the package index -- attempting $pinned anyway"
        echo "$pinned"
    fi
}

install_packages() {
    log "=== Installing packages ==="
    export DEBIAN_FRONTEND=noninteractive
    export NEEDRESTART_MODE=l          # list only -- never auto-restart services
    export NEEDRESTART_SUSPEND=1       # fully suspend needrestart hooks

    # Base tools
    log "Installing base tools..."
    apt-get update -qq
    apt-get install -y -qq jq pciutils dmidecode ethtool wget 2>&1 | tail -3 >&2

    # Kernel headers
    log "Installing kernel headers for $(uname -r)..."
    apt-get install -y -qq "linux-headers-$(uname -r)" dkms 2>&1 | tail -3 >&2 || {
        warn "Exact headers unavailable -- trying generic"
        apt-get install -y -qq linux-headers-generic dkms 2>&1 | tail -3 >&2 || true
    }

    # CUDA repo
    log "Adding NVIDIA CUDA repository..."
    local DISTRO="ubuntu$(lsb_release -rs | tr -d '.')"
    local ARCH="x86_64"
    wget -q "https://developer.download.nvidia.com/compute/cuda/repos/${DISTRO}/${ARCH}/cuda-keyring_1.1-1_all.deb" \
        -O "$WORK_DIR/cuda-keyring.deb" || {
        DISTRO="ubuntu2204"
        wget -q "https://developer.download.nvidia.com/compute/cuda/repos/${DISTRO}/${ARCH}/cuda-keyring_1.1-1_all.deb" \
            -O "$WORK_DIR/cuda-keyring.deb"
    }
    dpkg -i "$WORK_DIR/cuda-keyring.deb" 2>&1 | tail -2 >&2
    apt-get update -qq

    # Pinned driver + CUDA
    local driver_pkg="$NVIDIA_DRIVER"
    local cuda_pkg="$CUDA_TOOLKIT"
    log "Pinned driver: $driver_pkg"
    log "Pinned CUDA:   $cuda_pkg"

    driver_pkg=$(resolve_driver_pkg "$driver_pkg")

    log "Installing $driver_pkg and $cuda_pkg..."
    apt-get install -y -qq "$driver_pkg" "$cuda_pkg" 2>&1 | tail -10 >&2 || {
        warn "CUDA toolkit install failed -- trying driver only..."
        apt-get install -y -qq "$driver_pkg" 2>&1 | tail -10 >&2
    }

    # DCGM 4.x (required for driver 595+, DCGM 3.x is NOT compatible)
    # Purge old 3.x first
    log "Purging old DCGM 3.x if present..."
    dpkg --list datacenter-gpu-manager &>/dev/null && \
        apt-get purge -y -qq datacenter-gpu-manager 2>&1 | tail -3 >&2 || true
    dpkg --list datacenter-gpu-manager-config &>/dev/null && \
        apt-get purge -y -qq datacenter-gpu-manager-config 2>&1 | tail -3 >&2 || true

    log "Installing DCGM 4.x (datacenter-gpu-manager-4-cuda${DCGM_CUDA_MAJOR})..."
    apt-get install -y -qq --install-recommends \
        "datacenter-gpu-manager-4-cuda${DCGM_CUDA_MAJOR}" 2>&1 | tail -5 >&2 || {
        warn "DCGM cuda${DCGM_CUDA_MAJOR} failed -- trying without cuda suffix..."
        apt-get install -y -qq datacenter-gpu-manager-4 2>&1 | tail -5 >&2 || {
            warn "DCGM install failed -- stress test (98) will be unavailable"
        }
    }

    echo "$driver_pkg"  > "$WORK_DIR/installed_driver.txt"
    echo "$cuda_pkg"    > "$WORK_DIR/installed_cuda.txt"
}

###############################################################################
# HELPER: Log the kernel's view of every GPU memory BAR
###############################################################################
# The nvidia probe needs BAR0 (registers) AND BAR1 (framebuffer aperture). If
# either is unassigned it rejects the device at modprobe time ("This PCI I/O
# region assigned to your NVIDIA device is invalid"), modprobe still returns
# 0, and nvidia-smi then fails instantly with "No devices were found". BAR1 is
# the one the BIOS runs out of room for: 128 GB per card on a 96 GB RTX PRO
# 6000, 128 GB on H100/H200. The setpci BAR0 check reads only the low 32 bits,
# which on a 64-bit BAR legitimately shows just the flag bits (0x0000000c), so
# the sysfs resource file is the view to trust.
#
# The resource file has one line per BAR *register* (six of them), not per
# region: a 64-bit BAR occupies two registers and its second line is all
# zeros. On the RTX PRO 6000 BAR0 itself is 64-bit, so reading line 2 as
# "BAR1" found the upper half of BAR0 and reported a false "unassigned BAR1"
# on every card while lspci showed all regions assigned. Each line's flags
# column says what the register is: 0 means not implemented, bit 8 (0x100)
# I/O, bit 9 (0x200) memory, bit 20 (0x100000) a 64-bit BAR whose next
# register is its upper half. Memory regions are named BAR0/BAR1/BAR3 in
# NVIDIA's order (registers, framebuffer, RAMIN) with the register index in
# brackets. One line per GPU; returns 1 if any memory region is unassigned.
log_gpu_bars() {
    local root="${SYSFS_PCI_ROOT:-/sys/bus/pci/devices}"
    local pci_addr rc=0
    for pci_addr in $(lspci -Dn | awk '/10de:/{print $1}'); do
        local res="${root}/${pci_addr}/resource"
        [[ -r "$res" ]] || continue
        local line="" missing="" reg=0 nth=0 skip=false
        local start end flags
        while [[ $reg -lt 6 ]] && read -r start end flags; do
            local r=$reg
            reg=$((reg + 1))
            if $skip; then skip=false; continue; fi
            [[ -z "$flags" ]] && continue
            local f=$((flags))
            (( f == 0 )) && continue
            (( f & 0x100 )) && continue
            (( f & 0x100000 )) && skip=true
            local name size
            case $nth in 0) name="BAR0" ;; 1) name="BAR1" ;; *) name="BAR3" ;; esac
            nth=$((nth + 1))
            if [[ $((start)) -eq 0 ]]; then
                size="unassigned"
                missing+=" ${name}[reg${r}]"
            else
                size=$(numfmt --to=iec $(( end - start + 1 )) 2>/dev/null || echo "$(( (end - start + 1) / 1048576 ))M")
            fi
            line+=" ${name}[reg${r}]=${start}(${size})"
        done < "$res"
        if [[ -n "$missing" ]]; then
            warn "  ${pci_addr}:${line} -- unassigned:${missing}"
            rc=1
        else
            log "  ${pci_addr}:${line}"
        fi
    done
    return $rc
}

###############################################################################
# HELPER: dump everything needed to diagnose a driver that will not come up
###############################################################################
# Called when modprobe nvidia fails AND when modprobe succeeds but nvidia-smi
# does not (the RTX PRO 6000 case that used to end in a bare "nvidia-smi
# failed after module load" with nothing else to go on). Everything goes to
# stderr so it lands in the MAAS commissioning output for script 90, which is
# usually the only place it can be read: the node is in an ephemeral image.
dump_gpu_load_diagnostics() {
    local reason="$1"
    err "--- GPU driver diagnostics: ${reason} ---"
    log "--- kernel module vs userspace ---"
    cat /proc/driver/nvidia/version >&2 2>/dev/null || warn "  /proc/driver/nvidia/version absent -- nvidia module not loaded"
    log "  modinfo version: $(modinfo -F version nvidia 2>/dev/null || echo '?')"
    log "  nvidia-utils:    $(dpkg-query -W -f='${Package} ${Version}\n' 'nvidia-utils-*' 2>/dev/null | tr '\n' ';' || echo '?')"
    lsmod | grep -E '^(nvidia|nouveau)' >&2 || warn "  no nvidia/nouveau modules loaded"
    log "--- device nodes ---"
    ls -la /dev/nvidia* >&2 2>&1 || warn "  no /dev/nvidia* nodes"
    log "--- per-GPU: bound driver, PCI id, class ---"
    local pci_addr
    for pci_addr in $(lspci -Dn | awk '/10de:/{print $1}'); do
        local drv
        drv=$(basename "$(readlink "/sys/bus/pci/devices/${pci_addr}/driver" 2>/dev/null)" 2>/dev/null)
        log "  ${pci_addr}: driver=${drv:-none} id=$(cat "/sys/bus/pci/devices/${pci_addr}/device" 2>/dev/null) class=$(cat "/sys/bus/pci/devices/${pci_addr}/class" 2>/dev/null)"
    done
    log "--- per-GPU: kernel BARs ---"
    log_gpu_bars || warn "  at least one GPU has an unassigned BAR0/BAR1 -- BIOS MMIO (Above 4G Decoding / MMIO High Base+Size) problem, no software fix"
    log "--- /proc/driver/nvidia/gpus ---"
    cat /proc/driver/nvidia/gpus/*/information >&2 2>/dev/null || log "  (no GPUs registered with the driver)"
    log "--- dmesg: NVRM / nvidia / Xid / PCI resource lines (last 60) ---"
    dmesg | grep -iE "NVRM|nvidia|nouveau|Xid|gsp|firmware|BAR [0-9]|can't assign|no space for|fallen off|confidential" | tail -60 >&2 || true
    if [[ -s "$WORK_DIR/cc_tool.log" ]]; then
        log "--- gpu-admin-tools output (Confidential Computing check, last 40) ---"
        tail -40 "$WORK_DIR/cc_tool.log" >&2 || true
    fi
    log "--- GSP firmware on disk ---"
    find /lib/firmware/nvidia/ -name "gsp_*" -ls 2>/dev/null >&2 || warn "  no GSP firmware found"
    log "--- DKMS status ---"
    dkms status >&2 2>&1 || true
    log "--- first GPU: lspci -vv (regions, link state, capabilities) ---"
    lspci -vvs "$(lspci -Dn | awk '/10de:/{print $1; exit}')" >&2 2>&1 || true
    log "--- Secure Boot ---"
    mokutil --sb-state >&2 2>&1 || true
    err "--- end GPU driver diagnostics ---"
}

###############################################################################
# CONFIDENTIAL COMPUTING MODE -- read and switch off BEFORE the driver loads
###############################################################################
# A GPU a previous tenant left in NVIDIA Confidential Computing (CC) mode
# keeps that mode in its own non-volatile state. It survives reboots, power
# cycles and the reimage, so nothing in the wipe clears it, and it is GPU
# state, not platform state: 8 of 28 eu1-hpe1-rtx6000pro-se nodes had it with
# BIOS settings identical to the 20 that did not. The host driver then refuses
# every such GPU with
#   NVRM: GPU0 confComputeConstructEngine_IMPL: CPU does not support confidential compute.
#   NVRM: RmInitAdapter failed! (0x22:0x38:894)
# nvidia.ko loads, /dev/nvidia0-7 appear, and nvidia-smi exits 6 "No devices
# were found". Even where the driver did come up, a CC-mode GPU runs with bus
# encryption on and performance counters off, which the next customer would
# inherit.
#
# nvidia-smi conf-compute can only report the mode. Switching it needs
# NVIDIA's gpu-admin-tools, which talks to the GPU's FSP through BAR0, so it
# must run with no nvidia driver bound and no kernel lockdown (Secure Boot
# off). Hence this runs after nouveau is gone and before modprobe nvidia, one
# invocation per GPU by BDF so a card the tool cannot initialise is reported
# on its own line instead of aborting the whole query. --ignore-nvidia-driver
# is deliberately not used.
#
# NVIDIA's deployment guide wants a full host reboot after a CC switch, and
# nvtrust issue 150 shows stale state on an RTX PRO 6000 until one. A reboot
# here would lose the ephemeral environment and the driver this script just
# installed, so the pipeline's existing mechanism stands in for it: a run
# that had to switch a GPU is marked for revalidation, the later scripts skip
# via the halt marker, and the second commissioning run is the clean boot.
# Query first, switch only what is not off, and record before/after per BDF.
cc_gpu_bdfs() {
    # Display-class NVIDIA functions only: not the audio function, bridges or
    # NVSwitches, none of which carry a CC mode.
    lspci -Dn 2>/dev/null | awk '$2 ~ /^030[02]:/ && $3 ~ /^10de:/ {print $1}'
}

secure_boot_enabled() {
    # Kernel lockdown is what actually blocks the tool's BAR0 mapping; Secure
    # Boot is what turns lockdown on. Either answer is enough.
    local ld="${LOCKDOWN_FILE:-/sys/kernel/security/lockdown}"
    if [[ -r "$ld" ]] && ! grep -q '\[none\]' "$ld" 2>/dev/null; then
        return 0
    fi
    if command -v mokutil &>/dev/null && mokutil --sb-state 2>/dev/null | grep -qi 'enabled'; then
        return 0
    fi
    return 1
}

# Sets CC_TOOL to nvidia_gpu_tools.py, or CC_SKIP_REASON and returns 1.
cc_fetch_tool() {
    if [[ -n "$GPU_ADMIN_TOOLS_DIR" ]]; then
        if [[ -f "$GPU_ADMIN_TOOLS_DIR/nvidia_gpu_tools.py" ]]; then
            CC_TOOL="$GPU_ADMIN_TOOLS_DIR/nvidia_gpu_tools.py"
            return 0
        fi
        CC_SKIP_REASON="GPU_ADMIN_TOOLS_DIR=$GPU_ADMIN_TOOLS_DIR has no nvidia_gpu_tools.py"
        return 1
    fi
    if ! command -v python3 &>/dev/null; then
        CC_SKIP_REASON="python3 is not available"
        return 1
    fi
    local dest="$WORK_DIR/gpu-admin-tools"
    mkdir -p "$dest"
    log "Fetching gpu-admin-tools ${GPU_ADMIN_TOOLS_REF:0:12} from $GPU_ADMIN_TOOLS_URL"
    if ! wget -q -O "$WORK_DIR/gpu-admin-tools.tar.gz" "$GPU_ADMIN_TOOLS_URL" \
       || ! tar -xzf "$WORK_DIR/gpu-admin-tools.tar.gz" -C "$dest" --strip-components=1 \
       || [[ ! -f "$dest/nvidia_gpu_tools.py" ]]; then
        CC_SKIP_REASON="gpu-admin-tools could not be fetched from $GPU_ADMIN_TOOLS_URL"
        return 1
    fi
    CC_TOOL="$dest/nvidia_gpu_tools.py"
}

# Runs the tool for one BDF. Everything it prints is appended to cc_tool.log
# (it logs to stderr); the parsed mode (on/off/devtools) goes to stdout, or
# "unknown" with CC_LAST_ERROR set and a non-zero return.
cc_tool_run() {
    local bdf="$1"; shift
    local out rc=0
    out=$(timeout "$CC_TOOL_TIMEOUT" python3 "$CC_TOOL" --devices "$bdf" "$@" 2>&1) || rc=$?
    printf -- '--- %s %s (exit %s)\n%s\n' "$bdf" "$*" "$rc" "$out" >> "$WORK_DIR/cc_tool.log"
    local mode
    mode=$(printf '%s\n' "$out" | grep -oP 'CC mode is \K(on|off|devtools)' | tail -1)
    if [[ $rc -ne 0 || -z "$mode" ]]; then
        CC_LAST_ERROR=$(printf '%s\n' "$out" | grep -E 'ERROR|Error|error' | tail -1 | cut -c1-200)
        [[ $rc -eq 124 ]] && CC_LAST_ERROR="timed out after ${CC_TOOL_TIMEOUT}s"
        [[ -z "$CC_LAST_ERROR" ]] && CC_LAST_ERROR="exit $rc, no CC mode in output"
        echo "unknown"
        return 1
    fi
    echo "$mode"
}

cc_query()   { cc_tool_run "$1" --query-cc-mode; }
cc_set_off() { cc_tool_run "$1" --set-cc-mode=off --reset-after-cc-mode-switch --query-cc-mode; }

cc_write_json() {
    jq -n \
        --argjson checked "${CC_CHECKED:-false}" \
        --arg reason "${CC_SKIP_REASON:-}" \
        --arg tool "gpu-admin-tools@${GPU_ADMIN_TOOLS_REF:0:12}" \
        --argjson gpus "${CC_GPUS:-[]}" \
        --argjson switched "${CC_SWITCHED:-0}" \
        --argjson not_off "${CC_NOT_OFF:-0}" \
        --argjson unknown "${CC_UNKNOWN:-0}" \
        '{
            checked: $checked,
            skipped_reason: (if $reason == "" then null else $reason end),
            tool: $tool,
            gpus: $gpus,
            switched_off_count: $switched,
            not_off_count: $not_off,
            unknown_count: $unknown
        }' > "$WORK_DIR/cc_mode.json"
}

reset_confidential_compute() {
    log "=== Confidential Computing mode (must be off; checked before the driver loads) ==="
    CC_CHECKED=false; CC_SKIP_REASON=""; CC_GPUS="[]"
    CC_SWITCHED=0; CC_NOT_OFF=0; CC_UNKNOWN=0
    cc_write_json

    local bdfs
    bdfs=$(cc_gpu_bdfs)
    if [[ -z "$bdfs" ]]; then
        CC_SKIP_REASON="no display-class NVIDIA function in lspci"
        warn "CC check skipped: $CC_SKIP_REASON"
        cc_write_json
        return 0
    fi

    if lsmod 2>/dev/null | grep -qE '^nvidia '; then
        warn "nvidia module is already loaded before the CC check -- unloading so gpu-admin-tools can reach the GPUs"
        modprobe -r nvidia_uvm nvidia_drm nvidia_modeset nvidia 2>/dev/null || true
        if lsmod 2>/dev/null | grep -qE '^nvidia '; then
            CC_SKIP_REASON="nvidia module already loaded and could not be unloaded"
            warn "CC check skipped: $CC_SKIP_REASON"
            cc_write_json
            return 0
        fi
    fi

    if secure_boot_enabled; then
        CC_SKIP_REASON="Secure Boot / kernel lockdown is enabled, so gpu-admin-tools cannot map BAR0"
        warn "CC check skipped: $CC_SKIP_REASON"
        cc_write_json
        return 0
    fi

    if ! cc_fetch_tool; then
        warn "CC check skipped: $CC_SKIP_REASON"
        cc_write_json
        return 0
    fi

    CC_CHECKED=true
    local bdf
    for bdf in $bdfs; do
        local before after switched=false error=""
        if before=$(cc_query "$bdf"); then :; else error="$CC_LAST_ERROR"; fi
        case "$before" in
            off)
                log "  ${bdf}: CC mode off"
                after="off"
                ;;
            unknown)
                warn "  ${bdf}: CC mode could not be read (${error})"
                after="unknown"
                ;;
            *)
                warn "  ${bdf}: CC mode is '${before}' (left by a previous tenant) -- switching off"
                if after=$(cc_set_off "$bdf"); then :; else error="$CC_LAST_ERROR"; fi
                if [[ "$after" == "off" ]]; then
                    switched=true
                    CC_SWITCHED=$((CC_SWITCHED + 1))
                    log "  ${bdf}: CC mode now off (GPU reset by the tool)"
                else
                    # The switch call failed or did not take; re-read so the
                    # record is the GPU's answer, not the failure's.
                    if [[ "$after" == "unknown" ]]; then
                        after=$(cc_query "$bdf") || true
                    fi
                    err "  ${bdf}: CC mode still '${after}' after switch + reset (${error:-no error text})"
                fi
                ;;
        esac
        case "$after" in
            off)     ;;
            unknown) CC_UNKNOWN=$((CC_UNKNOWN + 1)) ;;
            *)       CC_NOT_OFF=$((CC_NOT_OFF + 1)) ;;
        esac
        CC_GPUS=$(printf '%s' "$CC_GPUS" | jq \
            --arg bdf "$bdf" --arg b "$before" --arg a "$after" \
            --argjson sw "$switched" --arg e "$error" \
            '. + [{bdf: $bdf, mode_before: $b, mode_after: $a, switched_off: $sw,
                   error: (if $e == "" then null else $e end)}]')
    done
    cc_write_json
    log "CC mode: $(printf '%s' "$CC_GPUS" | jq length) GPU(s) checked, ${CC_SWITCHED} switched off, ${CC_NOT_OFF} still on, ${CC_UNKNOWN} unreadable"
    return 0
}

###############################################################################
# LOAD MODULES + VERIFY + START DCGM
###############################################################################
load_and_verify() {
    log "=== Loading kernel modules ==="

    # ── Blacklist nouveau ──────────────────────────────────────────────
    # Nouveau claims older GPUs (A100, V100, etc.) and prevents the
    # nvidia driver from binding.  Best practice: always write the
    # blacklist, then unload if loaded.
    echo "blacklist nouveau"        > /etc/modprobe.d/blacklist-nouveau.conf
    echo "options nouveau modeset=0" >> /etc/modprobe.d/blacklist-nouveau.conf

    # ── Check BAR0 status (kernel vs hardware view) ─────────────────────
    # The nvidia driver reads BAR0 from the kernel's resource struct, NOT
    # from PCI config space.  If the kernel marks BAR0 as <ignored> (e.g.
    # due to BIOS BAR collisions or insufficient 32-bit MMIO), nvidia sees
    # BAR0=0x0 even though setpci shows a non-zero value in hardware.
    #
    # We check BOTH views:
    #   - setpci: raw PCI config register (hardware)
    #   - sysfs resource file: kernel's view (what nvidia actually uses)
    # BAR0 is the first line of the resource file and config offset 0x10.
    declare -A _saved_bar0
    local _bar0_needs_fix=false
    local pci_addr
    # Use -D to always include PCI domain (needed for multi-domain systems)
    for pci_addr in $(lspci -Dn | awk '/10de:/{print $1}'); do
        local bar0_hw bar0_kern
        bar0_hw=$(setpci -s "$pci_addr" BASE_ADDRESS_0 2>/dev/null) || continue
        # Kernel's view: first line of resource file, first field is start address
        bar0_kern=$(awk 'NR==1{print $1}' "/sys/bus/pci/devices/${pci_addr}/resource" 2>/dev/null)
        _saved_bar0["$pci_addr"]="$bar0_hw"

        if [[ -z "$bar0_kern" ]] || [[ "$bar0_kern" == "0x0000000000000000" ]] || [[ "$bar0_kern" == "0x00000000" ]]; then
            if [[ "$bar0_hw" != "00000000" ]]; then
                warn "  ${pci_addr}: BAR0 hw=0x${bar0_hw} but kernel=<ignored> (BIOS collision or conflict)"
                _bar0_needs_fix=true
            else
                warn "  ${pci_addr}: BAR0 is 0x0 in both hardware and kernel"
                _bar0_needs_fix=true
            fi
        else
            log "  BAR0 ${pci_addr}: hw=0x${bar0_hw} kern=${bar0_kern} OK"
        fi
    done

    # BAR0 alone is not enough: the probe also needs BAR1 (the framebuffer
    # aperture, 128 GB on RTX PRO 6000 / H100), which is the BAR a too-small
    # BIOS MMIO window leaves unassigned. Nothing here is fixable in software,
    # so this only reports -- the remove+rescan fixup below stays BAR0-only.
    log "Kernel view of GPU BARs (BAR0=registers, BAR1=framebuffer, BAR3=RAMIN):"
    log_gpu_bars || warn "At least one GPU has an unassigned BAR0/BAR1 -- the nvidia probe will reject it. Check Above 4G Decoding and MMIO High Base/Size in BIOS"

    local _nouveau_was_loaded=false
    if lsmod | grep -q nouveau; then
        _nouveau_was_loaded=true
        log "nouveau is loaded -- unloading..."

        # Step 1: Unbind the framebuffer console from nouveau.
        # Nouveau's nouveaufb holds a reference through the VT console;
        # without this unbind, rmmod silently fails to fully release devices.
        # Ref: https://nouveau.freedesktop.org/KernelModeSetting.html
        local vtcon
        for vtcon in /sys/class/vtconsole/vtcon*/; do
            if [[ -e "${vtcon}name" ]] && grep -q "frame buffer" "${vtcon}name" 2>/dev/null; then
                log "  Unbinding framebuffer vtconsole ${vtcon##*/sys/class/vtconsole/}..."
                echo 0 > "${vtcon}bind" 2>/dev/null || true
            fi
        done
        sleep 1

        # Step 2: Unbind nouveau from all GPU PCI devices via sysfs.
        local pci_addr
        for pci_addr in /sys/bus/pci/drivers/nouveau/[0-9]*; do
            if [[ -e "$pci_addr" ]]; then
                echo "${pci_addr##*/}" > /sys/bus/pci/drivers/nouveau/unbind 2>/dev/null || true
                log "  Unbound ${pci_addr##*/} from nouveau"
            fi
        done

        # Step 3: Remove nouveau and its full dependency chain.
        local mod
        for mod in nouveau drm_kms_helper drm ttm; do
            if lsmod | grep -q "^${mod} "; then
                rmmod "$mod" 2>/dev/null || true
            fi
        done
        modprobe -r nouveau 2>/dev/null || true

        # Step 4: If nouveau is STILL loaded, last-ditch rmmod.
        if lsmod | grep -q "^nouveau "; then
            sleep 1
            rmmod -f nouveau 2>/dev/null || warn "Could not unload nouveau (may need reboot)"
        fi

        # Verify
        if lsmod | grep -q "^nouveau "; then
            warn "nouveau is STILL loaded -- nvidia may fail to bind"
        else
            log "nouveau successfully unloaded"
        fi

        # NOTE: We intentionally do NOT perform PCI Function Level Reset (FLR)
        # here.  On A100s, FLR resets the PCI BAR registers to zero.  The
        # kernel's PCI allocator often cannot reassign the 32-bit BAR0 region
        # for 8 GPUs (128 MB of scarce 32-bit MMIO), leaving BAR0 = 0x0 and
        # causing nvidia probe to fail with "PCI I/O region is invalid".

        sleep 2
    fi

    # ── BAR0 fixup decision ──────────────────────────────────────────
    # The remove+rescan fixup is only safe when nouveau previously held
    # the devices and zeroed their BARs during unload.  On systems where
    # BIOS never assigned BARs (e.g., some HPE Blackwell configs), the
    # remove+rescan causes GPUs to "fall off the bus" -- a fatal PCIe
    # link loss that no software fix can recover.  In that case the BIOS
    # must be configured correctly (Above 4G Decoding, MMIO allocation).
    if $_bar0_needs_fix && $_nouveau_was_loaded; then
        # Nouveau previously held these devices and may have zeroed their
        # BARs during unload (known A100 issue).  Safe to do remove+rescan
        # because the PCIe links are still alive -- nouveau just cleared
        # the BAR registers.
        log "BAR0 issues detected after nouveau unload -- attempting remove+rescan fixup..."

        local gpu_bdfs=()
        for pci_addr in $(lspci -Dn | awk '/10de:/{print $1}'); do
            gpu_bdfs+=("$pci_addr")
        done

        local bar0_size=$((16 * 1024 * 1024))  # 16 MB
        local alloc_base=0
        local needed=$(( ${#gpu_bdfs[@]} * bar0_size ))
        log "  Need ${#gpu_bdfs[@]} x 16MB = $((needed / 1024 / 1024))MB of free 32-bit MMIO"

        # Find a free 32-bit MMIO base by scanning /proc/iomem
        local best_start=0 best_size=0
        local prev_end=$((0x80000000))
        while IFS='-' read -r range_start range_rest; do
            range_start="0x${range_start// /}"
            local range_end="0x${range_rest%% *}"
            local rs=$((range_start)) re=$((range_end))
            [[ $rs -ge $((0x100000000)) ]] && continue
            [[ $rs -lt $prev_end ]] && { prev_end=$(( re > prev_end ? re : prev_end )); continue; }
            local gap_size=$(( rs - prev_end ))
            if [[ $gap_size -gt $best_size ]]; then
                best_start=$prev_end
                best_size=$gap_size
            fi
            prev_end=$(( re + 1 ))
        done < <(grep -v '^ ' /proc/iomem | sort)
        local trailing=$(( 0x100000000 - prev_end ))
        if [[ $trailing -gt $best_size ]]; then
            best_start=$prev_end
            best_size=$trailing
        fi
        if [[ $best_size -ge $needed ]] && [[ $best_start -gt 0 ]]; then
            alloc_base=$(( (best_start + bar0_size - 1) & ~(bar0_size - 1) ))
            log "  Found $((best_size/1024/1024))MB free at 0x$(printf '%x' $best_start), allocating from 0x$(printf '%x' $alloc_base)"
        else
            warn "  Not enough contiguous 32-bit MMIO (need $((needed/1024/1024))MB, have $((best_size/1024/1024))MB)"
            alloc_base=0
        fi

        log "  Writing BAR0 via sysfs config..."
        local idx=0
        declare -A _new_bar0
        for pci_addr in "${gpu_bdfs[@]}"; do
            local new_bar0
            if [[ $alloc_base -gt 0 ]]; then
                new_bar0=$(printf '%08x' $(( alloc_base + idx * bar0_size )))
            else
                new_bar0="${_saved_bar0[$pci_addr]:-00000000}"
                local other
                for other in "${gpu_bdfs[@]}"; do
                    [[ "$other" == "$pci_addr" ]] && break
                    if [[ "${_new_bar0[$other]}" == "$new_bar0" ]]; then
                        new_bar0=$(printf '%08x' $(( 0x$new_bar0 + bar0_size )))
                        warn "  ${pci_addr}: collision, shifted to 0x${new_bar0}"
                        break
                    fi
                done
            fi
            _new_bar0["$pci_addr"]="$new_bar0"
            setpci -s "$pci_addr" BASE_ADDRESS_0="$new_bar0" 2>/dev/null
            local readback
            readback=$(setpci -s "$pci_addr" BASE_ADDRESS_0 2>/dev/null)
            if [[ "$readback" == "$new_bar0" ]]; then
                log "  ${pci_addr}: BAR0=0x${new_bar0} (verified)"
            else
                warn "  ${pci_addr}: write 0x${new_bar0} readback 0x${readback:-failed}"
            fi
            idx=$((idx + 1))
        done

        log "  Removing GPU PCI devices..."
        for pci_addr in "${gpu_bdfs[@]}"; do
            if [[ -e "/sys/bus/pci/devices/${pci_addr}/remove" ]]; then
                echo 1 > "/sys/bus/pci/devices/${pci_addr}/remove" 2>/dev/null || true
            fi
        done
        sleep 2

        for pci_addr in "${gpu_bdfs[@]}"; do
            setpci -s "$pci_addr" BASE_ADDRESS_0="${_new_bar0[$pci_addr]}" 2>/dev/null || true
        done

        log "  Rescanning PCI bus..."
        echo 1 > /sys/bus/pci/rescan 2>/dev/null || true
        sleep 5

        local fixed=0 broken=0
        for pci_addr in "${gpu_bdfs[@]}"; do
            local bar0_kern bar0_hw
            bar0_kern=$(awk 'NR==1{print $1}' "/sys/bus/pci/devices/${pci_addr}/resource" 2>/dev/null)
            bar0_hw=$(setpci -s "$pci_addr" BASE_ADDRESS_0 2>/dev/null)
            if [[ "$bar0_kern" != "0x0000000000000000" ]] && [[ "$bar0_kern" != "0x00000000" ]] && [[ -n "$bar0_kern" ]]; then
                log "  ${pci_addr}: kernel BAR0=${bar0_kern} hw=0x${bar0_hw} OK"
                fixed=$((fixed + 1))
            else
                warn "  ${pci_addr}: kernel BAR0 still unassigned (hw=0x${bar0_hw})"
                broken=$((broken + 1))
            fi
        done
        log "BAR0 fixup: ${fixed} fixed, ${broken} still broken"
        sleep 1
    elif $_bar0_needs_fix; then
        # No nouveau -- BIOS did not assign BARs.  Do NOT attempt
        # remove+rescan: on Blackwell GPUs this causes "fallen off the
        # bus" (fatal PCIe link loss).  Try modprobe directly -- the
        # kernel may have valid 64-bit mappings that setpci can't show.
        warn "BAR0 issues detected but nouveau was NOT loaded -- likely a BIOS configuration problem"
        warn "  Recommended BIOS settings: Above 4G Decoding=Enabled, MMIO High Base, 64-bit PCIe resource allocation"
        warn "  Skipping remove+rescan fixup (unsafe without nouveau) -- trying modprobe directly"
    fi

    # ── DKMS build (if nvidia module not found) ───────────────────────
    if ! modinfo nvidia &>/dev/null; then
        log "nvidia module not found -- attempting DKMS build..."
        local nvidia_ver
        nvidia_ver=$(dkms status 2>/dev/null | grep -i nvidia | head -1 | awk -F'[,/]' '{print $2}' | xargs)
        if [[ -n "$nvidia_ver" ]]; then
            dkms build  -m nvidia -v "$nvidia_ver" -k "$(uname -r)" >&2 2>&1 || true
            dkms install -m nvidia -v "$nvidia_ver" -k "$(uname -r)" >&2 2>&1 || true
        fi
    fi

    # ── Verify GSP firmware (required for nvidia-open modules) ────────
    # The open kernel modules REQUIRE the GPU System Processor firmware.
    # Without it modprobe nvidia fails silently with "No such device".
    local nvidia_mod_ver
    nvidia_mod_ver=$(modinfo nvidia 2>/dev/null | awk '/^version:/{print $2}')
    if [[ -n "$nvidia_mod_ver" ]]; then
        local fw_path="/lib/firmware/nvidia/${nvidia_mod_ver}/gsp_ga10x.bin"
        if [[ -f "$fw_path" ]]; then
            log "GSP firmware found: ${fw_path}"
        else
            warn "GSP firmware NOT found at ${fw_path}"
            local found_fw
            found_fw=$(find /lib/firmware/nvidia/ -name "gsp_ga10x.bin" 2>/dev/null | head -1)
            if [[ -n "$found_fw" ]]; then
                log "  Found alternative: ${found_fw}"
            else
                warn "  No GSP firmware found anywhere -- nvidia-open WILL fail"
                warn "  Hint: ensure nvidia-firmware package is installed"
            fi
        fi
    fi

    # ── Secure Boot check ─────────────────────────────────────────────
    if command -v mokutil &>/dev/null; then
        if mokutil --sb-state 2>/dev/null | grep -qi "enabled"; then
            warn "Secure Boot is ENABLED -- unsigned DKMS modules may fail to load"
        fi
    fi

    # ── Confidential Computing mode: must be off, must be done now ───
    # Needs the GPUs unbound from nvidia, so it cannot move later.
    reset_confidential_compute

    # ── Load nvidia module (with retries) ─────────────────────────────
    local nvidia_loaded=false
    local attempt modprobe_err
    for attempt in 1 2 3; do
        modprobe_err=$(modprobe nvidia 2>&1) && { nvidia_loaded=true; break; }
        warn "modprobe nvidia attempt $attempt failed: $modprobe_err"
        sleep "$attempt"
    done

    # ── Last resort: check dmesg for BAR0 or fallen-off-bus failure ──
    if ! $nvidia_loaded; then
        if dmesg | grep -q "fallen off the bus"; then
            err "GPUs have fallen off the PCIe bus -- this is a hardware or BIOS issue"
            err "  The remove+rescan fixup or a PCIe link failure caused the GPUs to become unreachable"
            err "  Action: check BIOS settings, reseat GPUs, or update BIOS firmware"
        elif dmesg | grep -q "BAR0 is 0M" && $_nouveau_was_loaded; then
            # Only safe to retry remove+rescan if nouveau was the cause
            warn "dmesg confirms BAR0 invalid after nouveau -- re-running BAR0 fixup..."
            rmmod nvidia 2>/dev/null || true

            local gpu_bdfs=()
            for pci_addr in $(lspci -Dn | awk '/10de:/{print $1}'); do
                gpu_bdfs+=("$pci_addr")
            done
            local bar0_size=$((16 * 1024 * 1024))

            for pci_addr in "${gpu_bdfs[@]}"; do
                if [[ -e "/sys/bus/pci/devices/${pci_addr}/remove" ]]; then
                    echo 1 > "/sys/bus/pci/devices/${pci_addr}/remove" 2>/dev/null || true
                fi
            done
            sleep 2

            local fallback_base=$((0xB0000000))
            local idx=0
            for pci_addr in "${gpu_bdfs[@]}"; do
                local new_bar0
                new_bar0=$(printf '%08x' $(( fallback_base + idx * bar0_size )))
                setpci -s "$pci_addr" BASE_ADDRESS_0="$new_bar0" 2>/dev/null || true
                log "  ${pci_addr}: wrote BAR0=0x${new_bar0}"
                idx=$((idx + 1))
            done

            echo 1 > /sys/bus/pci/rescan 2>/dev/null || true
            sleep 4

            log "BAR0 re-fixup complete -- final modprobe nvidia attempt..."
            modprobe_err=$(modprobe nvidia 2>&1) && nvidia_loaded=true
            if ! $nvidia_loaded; then
                warn "modprobe after BAR0 re-fixup failed: $modprobe_err"
            fi
        elif dmesg | grep -q "BAR0 is 0M"; then
            err "BAR0 is invalid but nouveau was not involved -- BIOS did not assign GPU PCI resources"
            err "  Action: enable 'Above 4G Decoding' in BIOS, check MMIO allocation settings"
            err "  Compare BIOS version and settings with a working identical system"
        fi
    fi

    # ── Failure diagnostics ───────────────────────────────────────────
    if ! $nvidia_loaded; then
        err "Failed to load nvidia kernel module: ${modprobe_err}"
        dump_gpu_load_diagnostics "modprobe nvidia failed"
        return 1
    fi
    modprobe nvidia-uvm >&2 2>&1 || warn "nvidia-uvm failed to load"

    # ── Wait for nvidia-smi ───────────────────────────────────────────
    # modprobe returning 0 only means the module is resident; the probe may
    # have rejected every GPU, and GSP is not booted until the first open.
    # So: give each call room to boot 8 GPUs, retry an instant failure a few
    # times (udev may still be creating /dev nodes), and on failure keep the
    # output and dump the kernel log rather than discard both. An instant
    # non-zero exit is never a timing problem; its text names the cause:
    #   "No devices were found"            probe rejected the GPUs (dmesg NVRM)
    #   "couldn't communicate with ..."    no /dev nodes or module not loaded
    #   "Driver/library version mismatch"  kernel module != nvidia-utils
    #   "Unable to determine the device handle ... Unknown Error"
    #                                      GSP boot / RmInitAdapter failed (dmesg)
    local smi_out="" smi_rc=1 smi_attempt=1
    local smi_deadline=$(( $(date +%s) + NVSMI_READY_TIMEOUT ))
    while :; do
        if [[ ! -e /dev/nvidiactl ]] && command -v nvidia-modprobe &>/dev/null; then
            log "/dev/nvidiactl missing -- creating device nodes with nvidia-modprobe"
            nvidia-modprobe -c 0 -u >&2 2>&1 || true
        fi
        if smi_out=$(timeout "$NVSMI_CALL_TIMEOUT" nvidia-smi 2>&1); then
            smi_rc=0
            break
        else
            smi_rc=$?
        fi
        if [[ $smi_rc -eq 124 ]]; then
            warn "nvidia-smi attempt ${smi_attempt} timed out after ${NVSMI_CALL_TIMEOUT}s (GSP boot still in progress?)"
        else
            warn "nvidia-smi attempt ${smi_attempt} exit ${smi_rc}: $(printf '%s' "$smi_out" | head -3 | tr '\n' ' ')"
        fi
        [[ $(date +%s) -ge $smi_deadline ]] && break
        smi_attempt=$((smi_attempt + 1))
        sleep 5
    done

    if [[ $smi_rc -ne 0 ]]; then
        err "nvidia-smi failed after module load (exit ${smi_rc} after ${smi_attempt} attempt(s)). Output:"
        printf '%s\n' "$smi_out" >&2
        # The CC-mode signature is unambiguous and names the fix, so say it
        # here rather than leave it in the middle of a 60-line dmesg dump.
        local cc_rejects
        cc_rejects=$(dmesg 2>/dev/null | grep -c 'does not support confidential compute' || true)
        cc_rejects=$((${cc_rejects:-0} + 0))
        echo "$cc_rejects" > "$WORK_DIR/cc_driver_rejected.txt"
        if [[ $cc_rejects -gt 0 ]]; then
            err "dmesg: the driver rejected ${cc_rejects} GPU init(s) with 'CPU does not support confidential compute'"
            err "  The GPU is in Confidential Computing mode (GPU-side state from a previous tenant)."
            if [[ "${CC_SWITCHED:-0}" -gt 0 ]]; then
                err "  This script switched it off on ${CC_SWITCHED} GPU(s) this run; the GPU reset was not enough. Re-run commissioning after a full reboot."
            elif [[ "${CC_CHECKED:-false}" != "true" ]]; then
                err "  The CC reset was skipped (${CC_SKIP_REASON:-unknown reason}), so nothing cleared it."
            fi
        fi
        dump_gpu_load_diagnostics "nvidia module loaded but nvidia-smi exit ${smi_rc}"
        return 1
    fi

    get_smi_header_info
    log "nvidia-smi OK -- $SMI_GPU_COUNT GPU(s), driver $SMI_DRIVER, CUDA $SMI_CUDA"

    # Surface the latent pin discrepancy (e.g. pinned 595 but running 580).
    # Fabric Manager matches the RUNNING driver, so this is informational only.
    local pinned_branch actual_branch
    pinned_branch=$(echo "$NVIDIA_DRIVER" | grep -oP 'nvidia-driver-\K[0-9]+' || true)
    actual_branch="${SMI_DRIVER%%.*}"
    if [[ -n "$pinned_branch" && -n "$actual_branch" && "$actual_branch" != "unknown" && "$pinned_branch" != "$actual_branch" ]]; then
        warn "Pinned driver branch ($pinned_branch) != running driver branch ($actual_branch) -- Fabric Manager will match the running driver"
    fi

    # Enable persistence mode (critical for DCGM GPU discovery)
    log "Enabling persistence mode..."
    nvidia-smi -pm 1 >&2 2>&1 || warn "nvidia-smi -pm 1 failed (may need root)"
    local pm_status
    pm_status=$(nvidia-smi --query-gpu=persistence_mode --format=csv,noheader 2>/dev/null | head -1 || echo "unknown")
    log "Persistence mode: $pm_status"

    # Install + start Fabric Manager and, on NVSwitch nodes, verify the fabric.
    # Must run after nvidia-smi works (FM version is matched to the running
    # driver) and before CUDA-dependent steps. Harmless/no-op on PCIe cards.
    setup_fabric_manager

    # Start DCGM and verify GPU discovery
    local dcgm_available="false"
    local dcgm_gpus=0
    DCGM_VER="not installed"

    if command -v dcgmi &>/dev/null; then
        # Start DCGM service (4.x: systemd nvidia-dcgm, 3.x: nv-hostengine)
        if systemctl is-active --quiet nvidia-dcgm 2>/dev/null; then
            log "nvidia-dcgm service already running"
        elif systemctl start nvidia-dcgm >/dev/null 2>&1; then
            log "Started nvidia-dcgm systemd service"
        elif ! pgrep -x nv-hostengine &>/dev/null; then
            log "Starting nv-hostengine..."
            nv-hostengine >&2 2>&1 || {
                rm -f /var/run/nvidia-hostengine/socket 2>/dev/null
                nv-hostengine >&2 2>&1 || warn "nv-hostengine failed to start"
            }
        fi

        # Retry GPU discovery -- DCGM needs time to enumerate GPUs
        get_dcgm_version
        log "DCGM version: $DCGM_VER"

        local attempt
        for attempt in 1 2 3 4 5; do
            sleep 3
            dcgm_gpus=$(dcgmi discovery -l 2>/dev/null | grep -oP '^\d+ GPUs? found' | grep -oP '^\d+' || echo "0")
            dcgm_gpus=$((dcgm_gpus + 0))
            log "DCGM discovery attempt $attempt: $dcgm_gpus GPU(s)"
            if [[ "$dcgm_gpus" -gt 0 ]]; then
                dcgm_available="true"
                break
            fi
            # If stuck at 0, try restarting the service
            if [[ "$attempt" -eq 3 ]]; then
                warn "DCGM still sees 0 GPUs after 3 attempts -- restarting service..."
                systemctl restart nvidia-dcgm &>/dev/null 2>&1 || {
                    pkill -x nv-hostengine 2>/dev/null || true
                    sleep 1
                    nv-hostengine >&2 2>&1 || true
                }
            fi
        done

        if [[ "$dcgm_gpus" -eq 0 ]]; then
            warn "DCGM $DCGM_VER sees 0 GPUs after 5 attempts"
            # Dump diagnostics
            warn "--- DCGM diagnostics ---"
            dcgmi discovery -l >&2 2>&1 || true
            log "nvidia-smi persistence mode:"
            nvidia-smi --query-gpu=persistence_mode --format=csv >&2 2>&1 || true
            log "Device nodes:"
            ls -la /dev/nvidia* >&2 2>&1 || warn "No /dev/nvidia* device nodes found"
            log "Loaded nvidia modules:"
            lsmod | grep nvidia >&2 2>&1 || true
            warn "--- end DCGM diagnostics ---"
        else
            dcgm_available="true"
            log "DCGM $DCGM_VER -- $dcgm_gpus GPU(s) visible"
        fi
    else
        warn "dcgmi not found -- DCGM not installed"
    fi

    # ── Check diagnostic tool availability ──────────────────────────
    local nvidia_bug_report_available=false
    local fieldiag_available=false

    if command -v nvidia-bug-report.sh &>/dev/null; then
        nvidia_bug_report_available=true
        log "nvidia-bug-report.sh found: $(command -v nvidia-bug-report.sh)"
    else
        warn "nvidia-bug-report.sh not found -- diagnostics on failure will be limited"
    fi

    if command -v fieldiag &>/dev/null; then
        fieldiag_available=true
        log "fieldiag found: $(command -v fieldiag)"
    elif [[ -x /usr/bin/fieldiag ]] || [[ -x /opt/nvidia/fieldiag/fieldiag ]]; then
        fieldiag_available=true
        log "fieldiag found at hardcoded path"
    else
        log "fieldiag not found (optional -- available from NVIDIA during product registration)"
    fi

    jq -n \
        --arg driver_ver "$SMI_DRIVER" \
        --arg cuda_ver "$SMI_CUDA" \
        --argjson gpu_count "$SMI_GPU_COUNT" \
        --arg driver_pkg "$(cat "$WORK_DIR/installed_driver.txt" 2>/dev/null || echo 'unknown')" \
        --arg cuda_pkg "$(cat "$WORK_DIR/installed_cuda.txt" 2>/dev/null || echo 'unknown')" \
        --argjson dcgm_available "$dcgm_available" \
        --arg dcgm_ver "$DCGM_VER" \
        --argjson dcgm_gpus "$dcgm_gpus" \
        --argjson bug_report "$nvidia_bug_report_available" \
        --argjson fieldiag "$fieldiag_available" \
        --argjson fabric_required "${FABRIC_REQUIRED:-false}" \
        --argjson fm_installed "${FM_INSTALLED:-false}" \
        --arg fm_version "${FM_VERSION:-not installed}" \
        --argjson fm_active "${FM_SERVICE_ACTIVE:-false}" \
        --arg fabric_state "${FABRIC_STATE:-N/A}" \
        --argjson fabric_ready "${FABRIC_READY:-false}" \
        '{
            nvidia_driver_version: $driver_ver,
            cuda_version: $cuda_ver,
            gpu_count: $gpu_count,
            driver_package: $driver_pkg,
            cuda_package: $cuda_pkg,
            dcgm_available: $dcgm_available,
            dcgm_version: $dcgm_ver,
            dcgm_gpu_count: $dcgm_gpus,
            nvidia_bug_report_available: $bug_report,
            fieldiag_available: $fieldiag,
            fabric_required: $fabric_required,
            fabric_manager_installed: $fm_installed,
            fabric_manager_version: $fm_version,
            fabric_manager_service_active: $fm_active,
            fabric_state: $fabric_state,
            fabric_ready: $fabric_ready
        }' > "$WORK_DIR/install_result.json"

    # Fabric node with an uninitialized fabric == broken node: fail the
    # install (report is already written above so the fabric_* fields survive).
    if [[ "${FABRIC_REQUIRED:-false}" == "true" && "${FABRIC_READY:-false}" != "true" ]]; then
        err "NVSwitch fabric not ready (Fabric Manager missing/failed) -- failing install"
        return 1
    fi
}

###############################################################################
# REPORT
###############################################################################
output_report() {
    local test_end
    test_end=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
    local duration=$(( $(date +%s) - SCRIPT_START ))

    if [[ ! -f "$WORK_DIR/install_result.json" ]]; then
        warn "install_result.json missing -- module load or nvidia-smi failed; reporting what did get installed"
        # The packages went in even though the driver never came up, so say
        # so: "dcgm_version: not installed" next to a DCGM 4.7 apt log misled
        # the first RTX PRO 6000 read-through. The version comes from the
        # module on disk, not a running driver.
        local dcgm_pkg_ver="not installed"
        if command -v dcgmi &>/dev/null; then
            get_dcgm_version
            dcgm_pkg_ver="$DCGM_VER"
        fi
        jq -n \
            --arg driver_ver "$(modinfo -F version nvidia 2>/dev/null || echo unknown)" \
            --arg driver_pkg "$(cat "$WORK_DIR/installed_driver.txt" 2>/dev/null || echo unknown)" \
            --arg cuda_pkg "$(cat "$WORK_DIR/installed_cuda.txt" 2>/dev/null || echo unknown)" \
            --arg dcgm_ver "$dcgm_pkg_ver" \
            '{
                nvidia_driver_version: $driver_ver, cuda_version: "unknown",
                gpu_count: 0, driver_package: $driver_pkg, cuda_package: $cuda_pkg,
                dcgm_available: false, dcgm_version: $dcgm_ver, dcgm_gpu_count: 0,
                driver_functional: false
            }' > "$WORK_DIR/install_result.json"
    fi

    local overall="PASS"
    local issues="[]"

    if ! timeout "$NVSMI_CALL_TIMEOUT" nvidia-smi &>/dev/null; then
        overall="FAIL"
        issues=$(echo "$issues" | jq '. + [{"issue":"nvidia-smi not functional after install","severity":"critical"}]')
    fi

    local expected_gpus
    expected_gpus=$(lspci -n | grep "10de:" | grep -E "030[02]:" | wc -l)
    expected_gpus=$((${expected_gpus:-0} + 0))
    local actual_gpus
    actual_gpus=$(jq -r '.gpu_count' "$WORK_DIR/install_result.json" 2>/dev/null || echo "0")

    if [[ "$actual_gpus" -lt "$expected_gpus" ]]; then
        [[ "$overall" != "FAIL" ]] && overall="WARN"
        issues=$(echo "$issues" | jq --arg m "Expected $expected_gpus GPUs but driver sees $actual_gpus" '. + [{"issue":$m}]')
    fi

    # DCGM GPU count mismatch
    local dcgm_gpus
    dcgm_gpus=$(jq -r '.dcgm_gpu_count' "$WORK_DIR/install_result.json" 2>/dev/null || echo "0")
    local dcgm_ver
    dcgm_ver=$(jq -r '.dcgm_version' "$WORK_DIR/install_result.json" 2>/dev/null || echo "unknown")
    if [[ "$dcgm_gpus" -lt "$actual_gpus" && "$dcgm_gpus" -ge 0 ]]; then
        local dcgm_avail
        dcgm_avail=$(jq -r '.dcgm_available' "$WORK_DIR/install_result.json" 2>/dev/null || echo "false")
        if [[ "$dcgm_avail" == "true" || "$dcgm_ver" != "not installed" ]]; then
            [[ "$overall" != "FAIL" ]] && overall="WARN"
            issues=$(echo "$issues" | jq \
                --arg m "DCGM $dcgm_ver sees $dcgm_gpus GPUs while nvidia-smi sees $actual_gpus -- stress test (98) may not work" \
                '. + [{"issue":$m,"severity":"warning"}]')
        fi
    fi

    # NVSwitch fabric health -- critical on fabric nodes, ignored on PCIe
    local fabric_required fabric_ready fabric_state
    fabric_required=$(jq -r '.fabric_required // false' "$WORK_DIR/install_result.json" 2>/dev/null || echo false)
    fabric_ready=$(jq -r '.fabric_ready // false' "$WORK_DIR/install_result.json" 2>/dev/null || echo false)
    fabric_state=$(jq -r '.fabric_state // "N/A"' "$WORK_DIR/install_result.json" 2>/dev/null || echo "N/A")
    if [[ "$fabric_required" == "true" && "$fabric_ready" != "true" ]]; then
        overall="FAIL"
        issues=$(echo "$issues" | jq --arg s "$fabric_state" \
            '. + [{"issue":"NVSwitch fabric not initialized (Fabric Manager missing/failed, state=\($s)) -- CUDA/DCGM will fail","severity":"critical"}]')
    fi

    # Confidential Computing mode. Written by reset_confidential_compute
    # before the driver loaded, so it is present whether or not the load
    # worked; the dmesg count is added if the load failed.
    local cc_json cc_rejected
    cc_json=$(cat "$WORK_DIR/cc_mode.json" 2>/dev/null) \
        || cc_json='{"checked":false,"skipped_reason":"check did not run","tool":null,"gpus":[],"switched_off_count":0,"not_off_count":0,"unknown_count":0}'
    cc_rejected=$(cat "$WORK_DIR/cc_driver_rejected.txt" 2>/dev/null || echo 0)
    cc_rejected=$((${cc_rejected:-0} + 0))
    cc_json=$(printf '%s' "$cc_json" | jq --argjson n "$cc_rejected" '. + {driver_rejected_count: $n}')

    local cc_checked cc_reason cc_switched cc_not_off cc_unknown
    cc_checked=$(printf '%s' "$cc_json" | jq -r '.checked')
    cc_reason=$(printf '%s' "$cc_json" | jq -r '.skipped_reason // ""')
    cc_switched=$(printf '%s' "$cc_json" | jq -r '.switched_off_count // 0')
    cc_not_off=$(printf '%s' "$cc_json" | jq -r '.not_off_count // 0')
    cc_unknown=$(printf '%s' "$cc_json" | jq -r '.unknown_count // 0')

    local revalidate="false"
    if [[ "$cc_not_off" -gt 0 ]]; then
        overall="FAIL"
        local still
        still=$(printf '%s' "$cc_json" | jq -r '[.gpus[] | select(.mode_after != "off" and .mode_after != "unknown") | "\(.bdf)=\(.mode_after)"] | join(", ")')
        issues=$(echo "$issues" | jq --arg s "$still" --argjson n "$cc_not_off" \
            '. + [{"issue":"\($n) GPU(s) are in Confidential Computing mode and could not be switched off (\($s)) -- the host driver refuses to initialise a CC-mode GPU and the next tenant would inherit bus encryption and disabled performance counters","severity":"critical"}]')
    fi
    if [[ "$cc_rejected" -gt 0 ]]; then
        overall="FAIL"
        local why
        if [[ "$cc_checked" != "true" ]]; then
            why="the CC reset was skipped (${cc_reason:-unknown reason})"
        elif [[ "$cc_switched" -gt 0 ]]; then
            why="it was switched off on ${cc_switched} GPU(s) this run and the GPU reset was not enough -- a full reboot is required"
        else
            why="gpu-admin-tools read it as off"
        fi
        issues=$(echo "$issues" | jq --arg w "$why" --argjson n "$cc_rejected" \
            '. + [{"issue":"Driver rejected \($n) GPU init(s): NVRM \"CPU does not support confidential compute\" -- the GPU is in Confidential Computing mode; \($w)","severity":"critical"}]')
    fi
    if [[ "$cc_switched" -gt 0 ]]; then
        # Same reasoning as ECC in script 91: the fix persists, the evidence
        # from this run does not count. NVIDIA wants a full reboot after a CC
        # switch; the next commissioning run is that reboot.
        revalidate="true"
        overall="FAIL"
        local which
        which=$(printf '%s' "$cc_json" | jq -r '[.gpus[] | select(.switched_off) | "\(.bdf) was \(.mode_before)"] | join(", ")')
        issues=$(echo "$issues" | jq --arg s "$which" --argjson n "$cc_switched" \
            '. + [{"issue":"Confidential Computing mode was ON on \($n) GPU(s), left by a previous tenant, and has been switched off by this script (\($s)). NVIDIA requires a full reboot after a CC mode switch before the GPU state can be trusted, so no evidence from this run can characterise these cards. Re-run commissioning.","severity":"critical"}]')
        err "=============================================================="
        err "  ACTION REQUIRED -- RE-RUN COMMISSIONING"
        err "=============================================================="
        err "  Confidential Computing mode was ON on ${cc_switched} GPU(s) and"
        err "  has now been switched off (${which})."
        err ""
        err "  The mode is GPU-side state from a previous tenant. It is now"
        err "  off and persists across reboots. NVIDIA requires a full host"
        err "  reboot after a CC mode switch, which a commissioning run"
        err "  cannot do without losing the driver it installed, so the"
        err "  later scripts are halted and this machine must simply be"
        err "  commissioned a second time; that run will produce valid"
        err "  evidence on cleanly booted GPUs."
        err "=============================================================="
        printf 'Confidential Computing mode was ON on %s GPU(s) and has been switched off by script 90 (%s). NVIDIA requires a full reboot after a CC mode switch, so no evidence from this run is usable. Re-run commissioning.\n' \
            "$cc_switched" "$which" > "$NEXGEN_HALT_FILE" 2>/dev/null \
            || warn "could not write halt marker $NEXGEN_HALT_FILE -- later scripts will still run"
    elif [[ "$cc_checked" != "true" && "$cc_rejected" -eq 0 ]]; then
        [[ "$overall" != "FAIL" ]] && overall="WARN"
        issues=$(echo "$issues" | jq --arg r "$cc_reason" \
            '. + [{"issue":"Confidential Computing mode was not verified: \($r). A CC-mode GPU left by a previous tenant would be inherited by the next one","severity":"warning"}]')
    elif [[ "$cc_unknown" -gt 0 ]]; then
        [[ "$overall" != "FAIL" ]] && overall="WARN"
        issues=$(echo "$issues" | jq --argjson n "$cc_unknown" \
            '. + [{"issue":"Confidential Computing mode could not be read on \($n) GPU(s) (see gpu-admin-tools output in the script log)","severity":"warning"}]')
    fi

    jq -n \
        --arg ver "$SCRIPT_VERSION" --arg name "gpu-install" \
        --arg ts "$test_end" --argjson dur "$duration" \
        --arg verdict "$overall" --argjson issues "$issues" \
        --argjson expected "$expected_gpus" --argjson actual "$actual_gpus" \
        --argjson cc "$cc_json" --argjson revalidate "$revalidate" \
        --slurpfile install "$WORK_DIR/install_result.json" \
        '{
            report_metadata: {
                script_version: $ver, script_name: $name,
                generated_at: $ts, duration_seconds: $dur,
                gpu_count_expected: $expected, gpu_count_visible: $actual
            },
            verdict: {overall: $verdict, issues: $issues},
            install: ($install[0] + {confidential_compute: $cc}),
            revalidation_required: $revalidate
        }'

    log "=== INSTALL COMPLETE -- Verdict: $overall ==="
}

###############################################################################
# MAIN
###############################################################################
main() {
    SCRIPT_START=$(date +%s)

    log "=========================================="
    log "NexGen GPU Install v${SCRIPT_VERSION}"
    log "=========================================="
    log "Host: $(hostname)"
    log "Date: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
    log "=========================================="

    detect_gpus || {
        jq -n --arg v "$SCRIPT_VERSION" '{
            report_metadata: {script_version:$v, script_name:"gpu-install"},
            verdict: {overall:"FAIL", issues:[{"issue":"No NVIDIA GPUs detected"}]}
        }'
        exit 1
    }

    install_packages
    local load_ok=true
    if ! load_and_verify; then
        load_ok=false
        warn "load_and_verify failed -- generating FAIL report"
    fi
    output_report

    if [[ "$load_ok" == "false" ]]; then
        exit 1
    fi

    # A CC-mode switch this run voids the evidence of everything after it;
    # the verdict above already says so and the halt marker is written.
    if [[ "${CC_SWITCHED:-0}" -gt 0 ]]; then
        rm -rf "$WORK_DIR"
        exit 1
    fi

    rm -rf "$WORK_DIR"

    log "=========================================="
    log "Install complete. Total time: $(( $(date +%s) - SCRIPT_START ))s"
    log "=========================================="
}

# Guarded so the test suite can source the functions without running an
# install; MAAS executes the file, where $0 is this script.
if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    main "$@"
fi
