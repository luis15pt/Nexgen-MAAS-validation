#!/bin/bash
# --- Start MAAS Metadata ---
# name: 90-nexgen-gpu-install-595-13
# title: NexGen GPU Driver 595 + CUDA 13 + DCGM 4.x Installation
# description: Installs nvidia-driver-595-server-open, cuda-toolkit-12-8,
#   DCGM 4.x (datacenter-gpu-manager-4-cuda13), and support tools.
#   Enables persistence mode, loads kernel modules, starts DCGM service.
#   Must run before 92-inventory, 98-stress-test and 99-burn-in.
#   Override at runtime: NVIDIA_DRIVER=... CUDA_TOOLKIT=... DCGM_CUDA_MAJOR=...
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
WORK_DIR="/tmp/gpu-install-$$"
SCRIPT_VERSION="2.1.6"

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

    if lspci | grep -qi "nvidia"; then
        log "NVIDIA GPU(s) detected via lspci"
        return 0
    elif lspci -n | grep -qi "10de:"; then
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

    # Fallback if the pinned driver branch has no installable candidate
    # (e.g. 595 not yet published -- degrade to the newest -server-open branch).
    if ! apt-cache policy "$driver_pkg" 2>/dev/null | grep -qE 'Candidate:\s*[0-9]'; then
        warn "$driver_pkg has no installable candidate -- searching for newest nvidia-driver-*-server-open..."
        # Dump the raw policy output: a false negative here (e.g. cache not
        # yet refreshed, epoch-prefixed versions) otherwise masks itself.
        apt-cache policy "$driver_pkg" 2>&1 | head -5 >&2 || true
        local newest
        newest=$(apt-cache search --names-only '^nvidia-driver-[0-9]+-server-open$' 2>/dev/null \
            | grep -oP 'nvidia-driver-\K[0-9]+' | sort -rn | head -1)
        if [[ -n "$newest" && "nvidia-driver-${newest}-server-open" != "$driver_pkg" ]]; then
            driver_pkg="nvidia-driver-${newest}-server-open"
            warn "Falling back to $driver_pkg"
        elif [[ -n "$newest" ]]; then
            # Search found the pinned package itself => the policy check was
            # a false negative, not a missing package. Proceed with the pin.
            warn "apt-cache search DOES list $driver_pkg -- candidate check false negative; keeping the pin"
        else
            warn "No nvidia-driver-*-server-open candidate found -- attempting $driver_pkg anyway"
        fi
    fi

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
        err "Failed to load nvidia kernel module"
        log "--- dmesg GPU diagnostics ---"
        dmesg | grep -iE "nvidia|nouveau|pci|firmware|gsp|drm|NVRM" | tail -50 >&2
        log "--- BAR0 status (hardware vs kernel) ---"
        for pci_addr in $(lspci -Dn | awk '/10de:/{print $1}'); do
            local b0_hw b0_kern
            b0_hw=$(setpci -s "$pci_addr" BASE_ADDRESS_0 2>/dev/null)
            b0_kern=$(awk 'NR==1{print $1}' "/sys/bus/pci/devices/${pci_addr}/resource" 2>/dev/null)
            log "  ${pci_addr} hw=0x${b0_hw:-?} kern=${b0_kern:-?}" >&2
        done
        log "--- GSP firmware check ---"
        find /lib/firmware/nvidia/ -name "gsp_ga*" -ls 2>/dev/null >&2 || warn "No GSP firmware found"
        log "--- module info ---"
        modinfo nvidia 2>&1 | head -20 >&2 || warn "modinfo nvidia failed"
        log "--- DKMS status ---"
        dkms status >&2 2>&1 || true
        log "--- PCIe diagnostics ---"
        lspci -Dnn | grep -i "10de" >&2 2>&1 || warn "No NVIDIA devices in lspci"
        lspci -vvs "$(lspci -Dn | grep '10de:' | head -1 | awk '{print $1}')" >&2 2>&1 || true
        log "Secure Boot status:"
        mokutil --sb-state >&2 2>&1 || true
        log "--- end diagnostics ---"
        return 1
    fi
    modprobe nvidia-uvm >&2 2>&1 || warn "nvidia-uvm failed to load"
    sleep 3

    if ! nvidia-smi &>/dev/null; then
        err "nvidia-smi failed after module load"
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
            dcgm_gpus=$(dcgmi discovery -l 2>/dev/null | grep -oP '^\d+ GPUs found' | grep -oP '^\d+' || echo "0")
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
        warn "install_result.json missing -- module load likely failed"
        jq -n '{
            nvidia_driver_version:"unknown",cuda_version:"unknown",
            gpu_count:0,driver_package:"unknown",cuda_package:"unknown",
            dcgm_available:false,dcgm_version:"not installed",dcgm_gpu_count:0
        }' > "$WORK_DIR/install_result.json"
    fi

    local overall="PASS"
    local issues="[]"

    if ! nvidia-smi &>/dev/null; then
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

    jq -n \
        --arg ver "$SCRIPT_VERSION" --arg name "gpu-install" \
        --arg ts "$test_end" --argjson dur "$duration" \
        --arg verdict "$overall" --argjson issues "$issues" \
        --argjson expected "$expected_gpus" --argjson actual "$actual_gpus" \
        --slurpfile install "$WORK_DIR/install_result.json" \
        '{
            report_metadata: {
                script_version: $ver, script_name: $name,
                generated_at: $ts, duration_seconds: $dur,
                gpu_count_expected: $expected, gpu_count_visible: $actual
            },
            verdict: {overall: $verdict, issues: $issues},
            install: $install[0]
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

    rm -rf "$WORK_DIR"

    log "=========================================="
    log "Install complete. Total time: $(( $(date +%s) - SCRIPT_START ))s"
    log "=========================================="
}

main "$@"
