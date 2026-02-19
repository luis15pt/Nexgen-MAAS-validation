#!/bin/bash
# --- Start MAAS Metadata ---
# name: 90-nexgen-gpu-install-580-12.8
# title: NexGen GPU Driver 580 + CUDA 12.8 + DCGM 4.x Installation
# description: Installs nvidia-driver-580-server-open, cuda-toolkit-12-8,
#   DCGM 4.x (datacenter-gpu-manager-4-cuda13), and support tools.
#   Enables persistence mode, loads kernel modules, starts DCGM service.
#   Must run before 98-inventory and 99-stress-test.
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
NVIDIA_DRIVER="${NVIDIA_DRIVER:-nvidia-driver-580-server-open}"
CUDA_TOOLKIT="${CUDA_TOOLKIT:-cuda-toolkit-12-8}"
DCGM_CUDA_MAJOR="${DCGM_CUDA_MAJOR:-13}"
WORK_DIR="/tmp/gpu-install-$$"
SCRIPT_VERSION="2.1.2"

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
    SMI_GPU_COUNT=$(nvidia-smi --query-gpu=count --format=csv,noheader,nounits 2>/dev/null | head -1 || echo "0")
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

    log "Installing $driver_pkg and $cuda_pkg..."
    apt-get install -y -qq "$driver_pkg" "$cuda_pkg" 2>&1 | tail -10 >&2 || {
        warn "CUDA toolkit install failed -- trying driver only..."
        apt-get install -y -qq "$driver_pkg" 2>&1 | tail -10 >&2
    }

    # DCGM 4.x (required for driver 580+, DCGM 3.x is NOT compatible)
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
            warn "DCGM install failed -- stress test (99) will be unavailable"
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
    local pci_short
    for pci_short in $(lspci -n | awk '/10de:/{print $1}'); do
        local bar0_hw bar0_kern
        bar0_hw=$(setpci -s "$pci_short" BASE_ADDRESS_0 2>/dev/null) || continue
        # Kernel's view: first line of resource file, first field is start address
        bar0_kern=$(awk 'NR==1{print $1}' "/sys/bus/pci/devices/0000:${pci_short}/resource" 2>/dev/null)
        _saved_bar0["$pci_short"]="$bar0_hw"

        if [[ "$bar0_kern" == "0x0000000000000000" ]] || [[ "$bar0_kern" == "0x00000000" ]]; then
            if [[ "$bar0_hw" != "00000000" ]]; then
                warn "  ${pci_short}: BAR0 hw=0x${bar0_hw} but kernel=<ignored> (BIOS collision or conflict)"
                _bar0_needs_fix=true
            else
                warn "  ${pci_short}: BAR0 is 0x0 in both hardware and kernel"
                _bar0_needs_fix=true
            fi
        else
            log "  BAR0 ${pci_short}: hw=0x${bar0_hw} kern=${bar0_kern} OK"
        fi
    done

    if lsmod | grep -q nouveau; then
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
        for pci_addr in /sys/bus/pci/drivers/nouveau/0000:*; do
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

    # ── Fix BAR0 if kernel has it as <ignored> ───────────────────────
    # When the kernel marks BAR0 as <ignored>, writing via setpci alone
    # does not help -- the kernel's resource struct stays at 0.  We must
    # remove the device, write a valid unique BAR0 address, then rescan
    # so the kernel re-reads config space and claims the resource.
    if $_bar0_needs_fix; then
        log "BAR0 needs fixing -- assigning unique addresses via PCI remove/rescan..."

        # Collect GPU BDF addresses
        local gpu_bdfs=()
        for pci_short in $(lspci -n | awk '/10de:/{print $1}'); do
            gpu_bdfs+=("$pci_short")
        done

        # Find a free 32-bit MMIO base by scanning /proc/iomem.
        # We need 16 MB per GPU (BAR0 size) aligned to 16 MB.
        # Look for the largest gap below 4 GB that is not in use.
        local bar0_size=$((16 * 1024 * 1024))  # 16 MB
        local alloc_base=0

        # Parse used 32-bit regions and find a gap big enough for all GPUs
        local needed=$(( ${#gpu_bdfs[@]} * bar0_size ))
        log "  Need ${#gpu_bdfs[@]} × 16MB = $((needed / 1024 / 1024))MB of free 32-bit MMIO"

        # Look for free space in the typical PCI MMIO window (above RAM, below 4GB)
        # Strategy: start from 0xA0000000 and check for a contiguous gap
        local candidate=0
        local best_start=0 best_size=0
        local prev_end=$((0x80000000))  # Start scanning from 2GB

        while IFS='-' read -r range_start range_rest; do
            range_start="0x${range_start// /}"
            local range_end="0x${range_rest%% *}"
            local rs=$((range_start)) re=$((range_end))

            # Only care about 32-bit range
            [[ $rs -ge $((0x100000000)) ]] && continue
            [[ $rs -lt $prev_end ]] && { prev_end=$(( re > prev_end ? re : prev_end )); continue; }

            local gap_size=$(( rs - prev_end ))
            if [[ $gap_size -gt $best_size ]]; then
                best_start=$prev_end
                best_size=$gap_size
            fi
            prev_end=$(( re + 1 ))
        done < <(grep -v '^ ' /proc/iomem | sort)

        # Check trailing space up to 4GB
        local trailing=$(( 0x100000000 - prev_end ))
        if [[ $trailing -gt $best_size ]]; then
            best_start=$prev_end
            best_size=$trailing
        fi

        if [[ $best_size -ge $needed ]] && [[ $best_start -gt 0 ]]; then
            # Align to 16 MB
            alloc_base=$(( (best_start + bar0_size - 1) & ~(bar0_size - 1) ))
            log "  Found ${best_size} bytes free at 0x$(printf '%x' $best_start), allocating from 0x$(printf '%x' $alloc_base)"
        else
            # Fallback: use the saved BIOS values but de-duplicate collisions
            warn "  Could not find large enough free 32-bit MMIO gap (need $((needed/1024/1024))MB, best ${best_size} bytes)"
            warn "  Falling back to BIOS BAR0 values with collision fixup"
            alloc_base=0
        fi

        # Remove all GPU PCI devices
        for pci_short in "${gpu_bdfs[@]}"; do
            if [[ -e "/sys/bus/pci/devices/0000:${pci_short}/remove" ]]; then
                echo 1 > "/sys/bus/pci/devices/0000:${pci_short}/remove" 2>/dev/null || true
            fi
        done
        sleep 2

        # Write unique BAR0 addresses to PCI config space while devices are
        # removed from the kernel but still physically on the bus.
        # The bridge keeps config space accessible.
        local idx=0
        for pci_short in "${gpu_bdfs[@]}"; do
            local new_bar0
            if [[ $alloc_base -gt 0 ]]; then
                new_bar0=$(printf '%08x' $(( alloc_base + idx * bar0_size )))
            else
                # De-duplicate: use saved value but shift collisions
                new_bar0="${_saved_bar0[$pci_short]:-00000000}"
                # Check for collision with already-assigned addresses
                local collision=false
                local other
                for other in "${gpu_bdfs[@]}"; do
                    [[ "$other" == "$pci_short" ]] && break
                    if [[ "${_saved_bar0[$other]}" == "$new_bar0" ]]; then
                        collision=true
                        break
                    fi
                done
                if $collision; then
                    # Shift by 16MB to resolve collision
                    local val=$((0x$new_bar0 + bar0_size))
                    new_bar0=$(printf '%08x' $val)
                    warn "  ${pci_short}: collision detected, shifted to 0x${new_bar0}"
                fi
            fi
            setpci -s "$pci_short" BASE_ADDRESS_0="$new_bar0" 2>/dev/null || \
                warn "  Failed to write BAR0 on ${pci_short}"
            log "  ${pci_short}: wrote BAR0=0x${new_bar0}"
            idx=$((idx + 1))
        done

        # Rescan so the kernel discovers devices with corrected BARs
        echo 1 > /sys/bus/pci/rescan 2>/dev/null || true
        sleep 4

        # Verify kernel now sees BAR0
        local fixed=0 broken=0
        for pci_short in "${gpu_bdfs[@]}"; do
            local bar0_kern
            bar0_kern=$(awk 'NR==1{print $1}' "/sys/bus/pci/devices/0000:${pci_short}/resource" 2>/dev/null)
            if [[ "$bar0_kern" != "0x0000000000000000" ]] && [[ "$bar0_kern" != "0x00000000" ]] && [[ -n "$bar0_kern" ]]; then
                log "  ${pci_short}: kernel BAR0=${bar0_kern} OK"
                fixed=$((fixed + 1))
            else
                warn "  ${pci_short}: kernel still shows BAR0 as unassigned"
                broken=$((broken + 1))
            fi
        done
        log "BAR0 fixup: ${fixed} fixed, ${broken} still broken"
        sleep 1
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

    # ── Last resort: check dmesg for BAR0 failure and retry ─────────
    if ! $nvidia_loaded; then
        # Check if it's a BAR0 issue that we didn't catch earlier
        if dmesg | grep -q "BAR0 is 0M"; then
            warn "dmesg confirms BAR0 invalid -- re-running BAR0 fixup..."
            rmmod nvidia 2>/dev/null || true
            _bar0_needs_fix=true

            # Same fixup logic: remove devices, write BARs, rescan
            local gpu_bdfs=()
            for pci_short in $(lspci -n | awk '/10de:/{print $1}'); do
                gpu_bdfs+=("$pci_short")
            done
            local bar0_size=$((16 * 1024 * 1024))

            for pci_short in "${gpu_bdfs[@]}"; do
                if [[ -e "/sys/bus/pci/devices/0000:${pci_short}/remove" ]]; then
                    echo 1 > "/sys/bus/pci/devices/0000:${pci_short}/remove" 2>/dev/null || true
                fi
            done
            sleep 2

            # Assign sequential BAR0 addresses starting from a safe base
            # Use 0xB0000000 as a fallback base (typically available)
            local fallback_base=$((0xB0000000))
            local idx=0
            for pci_short in "${gpu_bdfs[@]}"; do
                local new_bar0
                new_bar0=$(printf '%08x' $(( fallback_base + idx * bar0_size )))
                setpci -s "$pci_short" BASE_ADDRESS_0="$new_bar0" 2>/dev/null || true
                log "  ${pci_short}: wrote BAR0=0x${new_bar0}"
                idx=$((idx + 1))
            done

            echo 1 > /sys/bus/pci/rescan 2>/dev/null || true
            sleep 4

            log "BAR0 re-fixup complete -- final modprobe nvidia attempt..."
            modprobe_err=$(modprobe nvidia 2>&1) && nvidia_loaded=true
            if ! $nvidia_loaded; then
                warn "modprobe after BAR0 re-fixup failed: $modprobe_err"
            fi
        fi
    fi

    # ── Failure diagnostics ───────────────────────────────────────────
    if ! $nvidia_loaded; then
        err "Failed to load nvidia kernel module"
        log "--- dmesg GPU diagnostics ---"
        dmesg | grep -iE "nvidia|nouveau|pci|firmware|gsp|drm|NVRM" | tail -50 >&2
        log "--- BAR0 status (hardware vs kernel) ---"
        for pci_short in $(lspci -n | awk '/10de:/{print $1}'); do
            local b0_hw b0_kern
            b0_hw=$(setpci -s "$pci_short" BASE_ADDRESS_0 2>/dev/null)
            b0_kern=$(awk 'NR==1{print $1}' "/sys/bus/pci/devices/0000:${pci_short}/resource" 2>/dev/null)
            log "  ${pci_short} hw=0x${b0_hw:-?} kern=${b0_kern:-?}" >&2
        done
        log "--- GSP firmware check ---"
        find /lib/firmware/nvidia/ -name "gsp_ga*" -ls 2>/dev/null >&2 || warn "No GSP firmware found"
        log "--- module info ---"
        modinfo nvidia 2>&1 | head -20 >&2 || warn "modinfo nvidia failed"
        log "--- DKMS status ---"
        dkms status >&2 2>&1 || true
        log "--- PCIe diagnostics ---"
        lspci -nn | grep -i "10de" >&2 2>&1 || warn "No NVIDIA devices in lspci"
        lspci -vvs "$(lspci -n | grep '10de:' | head -1 | awk '{print $1}')" >&2 2>&1 || true
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

    # Enable persistence mode (critical for DCGM GPU discovery)
    log "Enabling persistence mode..."
    nvidia-smi -pm 1 >&2 2>&1 || warn "nvidia-smi -pm 1 failed (may need root)"
    local pm_status
    pm_status=$(nvidia-smi --query-gpu=persistence_mode --format=csv,noheader 2>/dev/null | head -1 || echo "unknown")
    log "Persistence mode: $pm_status"

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
            nv-hostengine 2>&1 >&2 || {
                rm -f /var/run/nvidia-hostengine/socket 2>/dev/null
                nv-hostengine 2>&1 >&2 || warn "nv-hostengine failed to start"
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
                    nv-hostengine 2>&1 >&2 || true
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

    jq -n \
        --arg driver_ver "$SMI_DRIVER" \
        --arg cuda_ver "$SMI_CUDA" \
        --argjson gpu_count "$SMI_GPU_COUNT" \
        --arg driver_pkg "$(cat "$WORK_DIR/installed_driver.txt" 2>/dev/null || echo 'unknown')" \
        --arg cuda_pkg "$(cat "$WORK_DIR/installed_cuda.txt" 2>/dev/null || echo 'unknown')" \
        --argjson dcgm_available "$dcgm_available" \
        --arg dcgm_ver "$DCGM_VER" \
        --argjson dcgm_gpus "$dcgm_gpus" \
        '{
            nvidia_driver_version: $driver_ver,
            cuda_version: $cuda_ver,
            gpu_count: $gpu_count,
            driver_package: $driver_pkg,
            cuda_package: $cuda_pkg,
            dcgm_available: $dcgm_available,
            dcgm_version: $dcgm_ver,
            dcgm_gpu_count: $dcgm_gpus
        }' > "$WORK_DIR/install_result.json"
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
                --arg m "DCGM $dcgm_ver sees $dcgm_gpus GPUs while nvidia-smi sees $actual_gpus -- stress test (99) may not work" \
                '. + [{"issue":$m,"severity":"warning"}]')
        fi
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
