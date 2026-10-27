# 90 - GPU Driver Install (`v2.2.0`)

[← Back to README](../../README.md)

Installs the full NVIDIA GPU software stack:

- **Driver**: `nvidia-driver-595-server-open` (falls back to the newest
  available `nvidia-driver-*-server-open` branch if 595 has no candidate)
- **CUDA Toolkit**: `cuda-toolkit-12-8`
- **DCGM**: `datacenter-gpu-manager-4` (CUDA 13)
- **Fabric Manager**: `nvidia-fabricmanager-<branch>`, version-matched to the
  *running* driver

Enables persistence mode, loads kernel modules, and starts the DCGM service.

On NVSwitch/NVL nodes it also installs the NVLSM stack (`nvlsm`, `libnvsdm`,
`infiniband-diags`), loads `ib_umad`, starts Fabric Manager, and waits for the
NVLink fabric to reach `Completed`. A fabric-attached node whose fabric never
trains is a **FAIL** — CUDA and DCGM cannot initialize without it. Plain PCIe
cards skip the fabric gate entirely.

## Confidential Computing mode is switched off before the driver loads

A GPU a previous tenant left in NVIDIA Confidential Computing (CC) mode keeps
that mode in its own non-volatile state. It survives reboots, power cycles and
the reimage, so the wipe does not clear it, and it is GPU state rather than
platform state: 8 of 28 `eu1-hpe1-rtx6000pro-se` nodes had it with BIOS settings
identical to the 20 that did not. The host driver refuses every such GPU:

```
NVRM: GPU0 confComputeConstructEngine_IMPL: CPU does not support confidential compute.
NVRM: RmInitAdapter failed! (0x22:0x38:894)
```

`nvidia.ko` loads, `/dev/nvidia0-7` appear, and `nvidia-smi` exits 6 with
`No devices were found`. Where the driver did happen to come up, the GPU ran
with bus encryption on and performance counters off, which the next customer
inherited.

`nvidia-smi conf-compute` can only report the mode. Switching it needs NVIDIA's
[gpu-admin-tools](https://github.com/NVIDIA/gpu-admin-tools), which talks to the
GPU's FSP through BAR0 and therefore must run with no nvidia driver bound and no
kernel lockdown (Secure Boot off). So the script, after unloading nouveau and
before `modprobe nvidia`:

1. Fetches gpu-admin-tools at a pinned commit (`GPU_ADMIN_TOOLS_REF`), or uses
   `GPU_ADMIN_TOOLS_DIR` if a checkout is already present.
2. Refuses to proceed if Secure Boot or kernel lockdown is on, or if an nvidia
   module is already loaded and will not unload. `--ignore-nvidia-driver` is
   never used. The check is recorded as skipped with the reason and the run is
   a **WARN**.
3. Queries CC mode on each display-class NVIDIA function by BDF, one invocation
   per GPU so a card the tool cannot read is reported on its own line.
4. Where the mode is anything but `off`, sets it off with
   `--set-cc-mode=off --reset-after-cc-mode-switch` and reads it back.

Everything is recorded in `install.confidential_compute`: `checked`,
`skipped_reason`, the tool commit, and per GPU `bdf`, `mode_before`,
`mode_after`, `switched_off` and `error`. If the driver still fails afterwards,
the `CPU does not support confidential compute` count from `dmesg` is added as
`driver_rejected_count` and named in the verdict.

**A switch voids the run.** NVIDIA's deployment guide requires a full host
reboot after a CC mode switch, and
[nvtrust issue 150](https://github.com/NVIDIA/nvtrust/issues/150) shows an
RTX PRO 6000 holding stale state through FLR and further toggles until one. A
reboot inside a commissioning run would lose the ephemeral environment and the
driver this script just installed, so the pipeline's existing mechanism stands
in for it, exactly as script `91` does for an ECC enable: the verdict is
**FAIL** with `revalidation_required: true`, the halt marker is written so
`91`–`99` skip in under a second, and the report carries the *action required*
banner. The mode is now off and persists, so the second commissioning run is
the clean boot and produces valid evidence.

A GPU that is still on after the switch, or that the driver rejected as
CC-mode, is a **FAIL** that names the BDF and the mode. A GPU the tool could not
read is a **WARN**.

## BAR check

The sysfs `resource` file has one line per BAR *register*, six of them, not one
per region: a 64-bit BAR takes two registers and its second line is all zeros.
On the RTX PRO 6000 BAR0 itself is 64-bit, so reading line 2 as "BAR1" found the
upper half of BAR0 and warned of an unassigned BAR1 on every card while `lspci`
showed every region assigned. The check now reads each line's flags column
(0 = register not implemented, `0x100` = I/O, `0x200` = memory, `0x100000` =
64-bit, so skip the next register) and names memory regions BAR0/BAR1/BAR3 in
NVIDIA's order with the register index in brackets:

```
0000:1b:00.0: BAR0[reg0]=0x00000000b8000000(64M) BAR1[reg2]=0x0000200000000000(128G) BAR3[reg4]=0x0000202000000000(32M)
```

| Env Override | Default | Description |
|---|---|---|
| `NVIDIA_DRIVER` | `nvidia-driver-595-server-open` | Driver package name |
| `CUDA_TOOLKIT` | `cuda-toolkit-12-8` | CUDA toolkit package |
| `DCGM_CUDA_MAJOR` | `13` | DCGM CUDA major version |
| `FABRIC_READY_TIMEOUT` | `300` | Seconds to wait for the NVLink fabric to train |
| `GPU_ADMIN_TOOLS_REF` | `v2026.09.29` | gpu-admin-tools release tag (a branch or commit works too) fetched as a tarball from GitHub |
| `GPU_ADMIN_TOOLS_URL` | GitHub archive of that ref | Where the tarball comes from; point it at an internal mirror to drop the GitHub dependency |
| `GPU_ADMIN_TOOLS_DIR` | unset | Existing checkout to use instead of fetching |
| `CC_TOOL_TIMEOUT` | `300` | Seconds allowed per gpu-admin-tools invocation |
| `NEXGEN_HALT_FILE` | `/run/nexgen-commissioning-halt` | Halt marker path, shared with scripts 91–99 |

**Timeout**: 20 minutes
