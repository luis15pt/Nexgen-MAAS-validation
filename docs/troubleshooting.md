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
