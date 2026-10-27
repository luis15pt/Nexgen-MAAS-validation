# Troubleshooting

[← Back to README](../README.md)

## `nvidia-fabricmanager` starts, then exits immediately

Version mismatch. The log reads *"fabric manager version A doesn't match
driver version B"*. FM must match the running driver on all three components
(`X.Y.Z`), not just the branch. Check:

```bash
nvidia-smi --query-gpu=driver_version --format=csv,noheader | head -1
dpkg-query -W -f='${Version}\n' 'nvidia-fabricmanager-*'
```

## Fabric stuck at `In Progress` and never reaches `Completed`

On Blackwell (B200/B300) the fabric is trained by NVLSM, which has
prerequisites that fail silently:

```bash
command -v ibstat          # from infiniband-diags -- FM's start wrapper
                           # shells out to this and exits 1 without it
lsmod | grep ib_umad       # NVLSM's management transport
dpkg -l nvlsm libnvsdm     # the NVLSM stack itself
grep FABRIC_MODE /usr/share/nvidia/nvswitch/fabricmanager.cfg   # want 0
```

A missing `ibstat` is the easiest one to miss: `nvidia-fabricmanager-start.sh`
exits 1 every single time without it, so FM never launches at all and the only
symptom is a fabric that stays `In Progress` until the timeout. `FABRIC_MODE`
other than `0` makes FM wait for a hypervisor handshake that never arrives on
bare metal. An all-zero Cluster UUID points at IMEX/clique configuration.

Script `90` installs and checks all of these, and dumps
`journalctl -u nvidia-fabricmanager` plus `/var/log/fabricmanager.log` to
stderr on failure — so on a node with no SSH access, read the MAAS
commissioning output for script `90` first.

## CUDA fails with error 802 (`system not yet initialized`)

Fabric not trained. See above — this is the CUDA-side symptom, not a separate
problem.

## `modprobe nvidia` succeeds but `nvidia-smi` fails straight away

Seen first on 8× RTX PRO 6000 Blackwell (HPE, two PCI domains). A clean
modprobe only proves the module is resident: the probe can still reject every
GPU, and GSP is not booted until the first `nvidia-smi` opens the device. The
*speed* of the failure is the first clue. An instant non-zero exit is never a
timing problem; its text names the cause:

| `nvidia-smi` says | Meaning | Where to look |
|---|---|---|
| `No devices were found` | probe rejected the GPUs at modprobe | `dmesg` `NVRM:` lines: invalid BAR, unsupported device, another driver bound |
| `couldn't communicate with the NVIDIA driver` | no `/dev/nvidia*` nodes, or module not loaded | `ls /dev/nvidia*`, `lsmod` |
| `Driver/library version mismatch` | kernel module and `nvidia-utils` differ | `/proc/driver/nvidia/version` vs `dpkg -l nvidia-utils-*` |
| `Unable to determine the device handle ... Unknown Error` | GSP boot / `RmInitAdapter` failed | `dmesg` for `GSP`, `Xid`, `RmInitAdapter` |

Blackwell adds one BAR-shaped cause the old BAR0-only check could not see:
BAR1 (the framebuffer aperture) is **128 GB per card** on a 96 GB RTX PRO
6000, and it is the region a too-small BIOS MMIO-above-4G window leaves
unassigned. The probe then rejects the device while BAR0 looks fine. On a
64-bit BAR0, `setpci` showing `0x0000000c` is *not* a problem: that is just the
flag bits of the low dword; the sysfs `resource` file carries the address.

From v2.1.7 script `90` prints, on this failure, the `nvidia-smi` output
itself, the kernel view of BAR0/BAR1/BAR3 per GPU, which driver each GPU is
bound to, `/proc/driver/nvidia/gpus`, and the last 60 `NVRM`/`Xid`/PCI
resource lines from `dmesg`, and it waits for a slow GSP boot rather than
giving up after one call. Read the MAAS commissioning output for `90` before
anything else. To get the same evidence by hand on a box you can reach:

```bash
nvidia-smi; echo "exit=$?"
dmesg | grep -iE 'NVRM|Xid|gsp|BAR [0-9]|no space for'
for d in /sys/bus/pci/devices/*; do grep -q 0x10de "$d/vendor" && { echo "$d"; sed -n '1,2p;4p' "$d/resource"; }; done
ls -la /dev/nvidia*; cat /proc/driver/nvidia/version
```

## `NVRM: ... CPU does not support confidential compute` and `nvidia-smi` finds no devices

Seen on 8 of 28 `eu1-hpe1-rtx6000pro-se` nodes (DL380a Gen12, 8× RTX PRO 6000
Blackwell Server Edition). `modprobe nvidia` succeeds, `/dev/nvidia0-7` appear,
`nvidia-smi` exits 6 with `No devices were found`, and `dmesg` has, for every
GPU:

```
NVRM: GPU0 confComputeConstructEngine_IMPL: CPU does not support confidential compute.
NVRM: RmInitAdapter failed! (0x22:0x38:894)
```

The previous tenant enabled NVIDIA Confidential Computing mode on the GPUs. It
is a GPU-side setting, separate from CPU TDX, stored in the GPU, and it survives
reboots, power cycles and reimaging. BIOS was identical across all 28 nodes
(checked over Redfish), so it is purely GPU state. The host driver refuses a
CC-mode GPU when the kernel presents no CPU TEE. The old "unassigned BAR1"
warning that accompanied this was a false positive (see the BAR section of the
[script 90 docs](scripts/90-gpu-driver-install.md)).

From v2.2.0 script `90` queries the mode on every GPU before loading the driver
and switches it off with NVIDIA gpu-admin-tools; the run is then voided and must
be repeated, because NVIDIA requires a full reboot after the switch. By hand, on
a box with **no nvidia module loaded** and Secure Boot off:

```bash
git clone https://github.com/NVIDIA/gpu-admin-tools && cd gpu-admin-tools
sudo python3 ./nvidia_gpu_tools.py --devices gpus --query-cc-mode
for bdf in $(lspci -Dn | awk '$2 ~ /^030[02]:/ && $3 ~ /^10de:/ {print $1}'); do
  sudo python3 ./nvidia_gpu_tools.py --devices "$bdf" --set-cc-mode=off --reset-after-cc-mode-switch
done
sudo reboot
```

`nvidia-smi conf-compute -f` can confirm the mode once the driver is up, but
cannot change it. Do not use `--ignore-nvidia-driver`: unload the driver instead.

## `NVRM: BAR0 is 0M @ 0x0` and the driver refuses to load

Two distinct causes with *opposite* fixes, which is why script `90` checks both
the hardware view (`setpci`) and the kernel view (the sysfs `resource` file):

- **nouveau zeroed the BARs during unload** (classic on 8× A100). PCIe links
  are still alive, so a `remove` + `rescan` with rewritten BAR0 values
  recovers it. Never use FLR here — a function level reset zeroes the BAR
  registers on A100s and makes things worse.
- **The BIOS never assigned the BARs.** No software fix exists. The script
  deliberately does *not* attempt remove+rescan in this case, because on
  Blackwell it makes the GPUs fall off the bus permanently. Fix it in BIOS:
  enable *Above 4G Decoding*, and check the MMIO high base and 64-bit PCIe
  resource allocation settings.

## DCGM diag exits 124 at the script ceiling with `killed by a signal`

The DCGM scripts (`93`–`96`, one per level) wrap `dcgmi diag` in
`timeout "$DCGM_DIAG_TIMEOUT"` (per-level default; 9600 s for level 3, it was
6000 s before `v3.0.0`). When that fires the log reads:

```
DCGM diag exited 124 in 6072s
"runtime_error" : "Error: Unable to complete diagnostic for entities *,cpu:*. Return: (-53) : The DCGM diagnostic was killed by a signal"
```

That is the script's own SIGTERM being reported back by DCGM, not a GPU fault.
The JSON carries no test results because `nvvs` never got to write any, so the
report says `no test was executed` as well. Neither line says which test was
running, and that is the question.

**Seen on 2026-10-26** on `eu1-hpe1-rtx6000pro-se-008` and `-010` (HPE DL380a
Gen12, 8× RTX PRO 6000 Blackwell Server Edition, driver 595.91.07, DCGM 4.7.0,
level 3). Both started within a minute of each other and both were killed at
6000 s, so this is the platform, not one card. The evidence that survived:

| Observation | Reading |
|---|---|
| Chassis `Power Meter` 940–1740 W, mean 1506 W over 180 samples | Idle is ~940 W; eight cards under concurrent stress would be >5 kW. The concurrent plugins (targeted stress/power, SM diagnostic) never ran. A sustained ~560 W above idle is one card near its 600 W cap at a time, or all eight at light load |
| GPU 0 `SW Power Capping` counter 182 ms before, 81.25 s after | GPU 0 did reach its cap, for 81 s cumulative, so some per-GPU load phase ran |
| No Xid, zero ECC / remap / replay deltas, final `nvidia-smi -q` clean | The cards were not harmed and did not fault; the run was slow or stuck, not broken |
| Inventory (`92`) 18 s earlier: all eight PASS, ECC on, DRAM encryption on, 96 GB GDDR7, BAR1 128 GB | Nothing pre-existing explains it |
| The burn-in (`99`) PASSED on the same hosts afterwards | An hour of sustained full load on all eight cards completed cleanly. If `99`'s log says `Load tool: dcgmi diag targeted_stress`, that is DCGM's own level-3 load plugin running on all eight GPUs at once without trouble, which pins the stall on the serialised level-2 plugins (`pcie`, `memory`). If it says `gpu-burn`, it shows the cards are healthy under load but says nothing about DCGM's plugins |

NVIDIA's figure for level 3 is under 35 min on an 8-GPU Hopper host; these
runs were past 100 min inside the serialised early phase. Candidates are the
per-GPU `memory` and `pcie` plugins against 96 GB cards, DCGM 4.7.0's handling
of this SKU, and the `cpu:*` entities the run was also asked to cover (the
Xeon 6710E CPU EUD). The run as it stood cannot distinguish them.

**Since `v3.0.0` the levels are separate scripts**, so the next run on this
platform says in minutes whether level 1 and level 2 complete, and how long
each takes, before level 3 is attempted at all.

**What `v2.7.0` records so the next run can:**

- a `diag progress t+NNNs:` line every 60 s with per-GPU utilisation, power
  and SM clock, the nvvs processes alive, and the last plugin line in nvvs'
  log -- also echoed to `/dev/console`, so the BMC remote console shows the
  run live (MAAS itself only shows output once the script ends);
- `nvvs` log tail and plugin markers (`--debugLogFile`, and
  `/var/log/nvidia-dcgm/nvvs.log`);
- the GPU load timeline and the sentence `GPUs were at >=50% utilisation in N
  of M samples, first at .., last at .. -- Ns before the diag ended`;
- DCGM's own `-t` timeout 120 s under the ceiling, so DCGM can stop itself and
  still write JSON.

**To reproduce by hand** on a deployed host with the driver and DCGM installed:

```
dcgmi diag -r 2 -v --debugLogFile /tmp/nvvs-r2.log ; echo "exit=$?"
grep -iE 'plugin|running|test' /tmp/nvvs-r2.log | tail -40
```

Level 2 is the serialised part on its own. If it completes, its wall time is
the number to budget before level 3 is retried; if it does not, the log names
the plugin, and that plugin can be excluded or tuned with `-p` on the
`dcgmi diag` command line.

