# 98 - GPU Stress Test (`v2.6.0`)

[← Back to README](../../README.md)

The DCGM diagnostic. This is the *diagnostic* half of the acceptance
specification's load requirement; the sustained burn-in in script `99` is the
other half, and neither replaces the other.


Runs DCGM diagnostics at configurable severity levels:

| Level | Duration | Scope |
|---|---|---|
| 1 | ~1 min | Quick health check |
| 2 | ~5 min | Medium validation |
| 3 (default) | ~15 min | Standard stress test |
| 4 | ~90 min | Full burn-in validation |

| Env Override | Default | Description |
|---|---|---|
| `DCGM_DIAG_LEVEL` | `3` | Diagnostic level, or a DCGM 4 named level |
| `DCGM_DIAG_TIMEOUT` | `6000` | Hard ceiling on the diag run, in seconds |

## Why `nvbandwidth` skips on H100 PCIe, and what that costs

DCGM reports `nvbandwidth` as **Skip** on every H100 PCIe with an empty `info`
field and no warning, which is easy to misread. Established from DCGM 4.6.1's
source and the SKU table compiled into its `nvvs` binary:

- the plugin has exactly one Skip path, `is_allowed` false;
- that parameter **defaults to false** and is enabled per-SKU;
- the table's entry for PCI device **`0x2331` (H100 PCIe)** has no `nvbandwidth`
  key at all, while `0x2330` (H100 SXM5) does.

So it skips on this SKU **whether or not NVLink bridges are fitted** — NVIDIA
has not qualified that plugin for the PCIe part. The empty `info` is because the
skip message goes through a log-only call that never reaches the JSON. A
*missing binary* would be FAIL (`DCGM_FR_INTERNAL`), not Skip — and the binary
is not missing: the DCGM packages ship it at
`/usr/libexec/datacenter-gpu-manager-4/plugins/cuda13/nvbandwidth`.

An earlier version of this rule keyed off NVLink presence instead. It gave the
right answer on a bridgeless host for the wrong reason and the **wrong** answer
on a bridged one, putting all eight GPUs of a machine with four working bridges
into review over a skip that was by design.

**What it costs:** on a machine sold *with* bridges, the NVLink fabric goes
unmeasured. The report says the skip is expected, which is an explanation and
not evidence. Peer bandwidth needs the standalone tool run directly —
`nvbandwidth -t device_to_device_memcpy_write_ce --format json` yields a
per-pair GB/s matrix in which the bridged pairs separate from the PCIe-only
ones. Forcing the DCGM plugin with `-p nvbandwidth.is_allowed=true` is *not*
the answer: it runs a path NVIDIA has not qualified for this SKU.

Fabric Manager is **not** required for bridges — it configures NVSwitch memory
fabrics, and there is no NVSwitch on a PCIe card. The bridges train with the
driver alone, which the `NV12` entries in the peer matrix confirm.

Any status that is not a recognised pass counts as a non-pass. `CONFIG` and
`RETEST` mean the rig was misconfigured and the hardware was never
characterised, so they fail rather than passing silently; `SKIP` means the test
did not run and evidences nothing. A run that produced no results, or results
of which none passed, fails. So does unparseable output — enumeration is not a
test.

`dcgmi diag` runs under `timeout`. Without it a hang is killed by MAAS at the
script timeout with no JSON emitted at all, so the run yields no evidence
either way.

**Timeout**: 2 hours
