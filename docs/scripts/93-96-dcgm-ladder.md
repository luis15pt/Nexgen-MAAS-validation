# 93–96 - DCGM Diagnostic Ladder (`v3.0.0`)

[← Back to README](../../README.md)

The DCGM diagnostic. This is the *diagnostic* half of the acceptance
specification's load requirement; the sustained burn-in in script `99` is the
other half, and neither replaces the other.

## One script per level, each gated on the one below

| Script | Level | Default ceiling (`DCGM_DIAG_TIMEOUT`) | Metadata timeout |
|---|---|---|---|
| `93-nexgen-dcgm-level1.sh` | 1 | 1200 s | 00:30:00 |
| `94-nexgen-dcgm-level2.sh` | 2 | 4800 s | 01:30:00 |
| `95-nexgen-dcgm-level3.sh` | 3 | 9600 s | 03:00:00 |
| `96-nexgen-dcgm-level4.sh` | 4 | 27600 s | 08:00:00 |

The four files share one body; each reads its level from its own file name.
Edit any one, then run `tools/dcgm-ladder-sync.sh sync <that file>` to copy
the body onto the other three (`tests/run-all.sh` fails if they drift).

**Gating.** Every rung appends its verdict to `/run/nexgen-dcgm-ladder.jsonl`.
Before running, a rung reads the records of the levels below it: if any is
`FAIL` it does not run, emits a `skipped: true` report naming the level that
failed, and exits 1. `WARN` does not gate. A rung whose script was not
uploaded leaves no record and is not consulted, so **you choose which rungs
run by choosing which scripts to upload**, and MAAS's per-script selection
when commissioning does the same. `DCGM_LADDER_IGNORE_PREVIOUS=1` forces a
rung to run regardless.

**Why rungs rather than one run.** DCGM levels are cumulative, so running
them in order costs the lower levels twice, which is seconds for level 1 and
minutes for level 2. In return every level reports on its own, with its own
time and its own verdict, so a host that cannot get through level 2 says so
in minutes instead of being killed at the level 3 ceiling having proved
nothing.

**Level 4 is hours.** The MAAS metadata timeout is documentation only in this
deployment (see `tests/validate-maas-metadata.py`), so the in-script ceiling
is the real limit, and the MAAS node timeout must be raised past it before
`96` is uploaded, exactly as for the burn-in.

**Certificate.** The report generator reads all four rungs and adjudicates
the highest one that actually ran; a rung skipped because a lower one failed
is not a result. The pre-split `98-nexgen-gpu-stress-test` is still
recognised for machines commissioned before `v3.0.0`.


Runs DCGM diagnostics at configurable severity levels:

| Level | NVIDIA's figure (8-GPU Hopper host) | Scope |
|---|---|---|
| 1 | seconds | Software / deployment checks |
| 2 | minutes | + memory, PCIe |
| 3 (default) | under 35 min | + targeted stress, targeted power, SM diagnostic, memory bandwidth |
| 4 | under 2.25 h | + memtest, pulse, EUD |

The durations are the ones in NVIDIA's DCGM 4 documentation, measured on
Hopper. They are not a promise for other silicon: on 2026-10-26 two HPE
DL380a Gen12 hosts with eight RTX PRO 6000 Blackwell each (`...rtx6000pro-se-008`
and `-010`, DCGM 4.7.0, driver 595.91.07) were still inside level 3 when the
6000 s ceiling killed them, with the chassis never drawing more than 1.74 kW --
so the concurrent stress plugins had not even started. See
[troubleshooting](../troubleshooting.md#dcgm-diag-exits-124-at-the-script-ceiling-with-killed-by-a-signal).

| Env Override | Default | Description |
|---|---|---|
| `DCGM_DIAG_LEVEL` | from the file name | Overrides the level the file name implies; also accepts a DCGM 4 named level or a comma list of test names such as `pcie,memory` |
| `DCGM_DIAG_PARAMS` | empty | Passed to `dcgmi diag -p`, e.g. `pcie.is_allowed=false` or `targeted_power.test_duration=120` |
| `DCGM_DIAG_TIMEOUT` | per level, table above | Hard ceiling on the diag run, in seconds. When `dcgmi diag` offers `-t`, DCGM is also told to stop itself 120 s earlier so it can still write JSON |
| `NEXGEN_DCGM_LADDER_FILE` | `/run/nexgen-dcgm-ladder.jsonl` | Where rungs record their verdicts for the gate |
| `DCGM_LADDER_IGNORE_PREVIOUS` | `0` | `1` runs the rung even if a lower one failed |
| `CHASSIS_SAMPLE_INTERVAL` | `30` | BMC fan / inlet / power sampling cadence during the diag, seconds; `0` disables |
| `GPU_SAMPLE_INTERVAL` | `30` | Per-GPU utilisation / power / clock sampling cadence during the diag, seconds; `0` disables |
| `PROGRESS_INTERVAL` | `60` | Cadence of the `diag progress t+NNNs:` lines (per-GPU util/W/SM clock, nvvs processes, last nvvs plugin line), seconds; also echoed to `/dev/console`; `0` disables |

## What a run that never finishes leaves behind

`dcgmi diag -j` only ever says `killed by a signal` when it is stopped, and
the test it was in is not in that JSON. Since `v2.7.0` the script therefore
keeps three other records and prints them on any FAIL:

- **nvvs' own log**, via `--debugLogFile` when the installed `dcgmi` has that
  option (probed from `--help`, not assumed) and `/var/log/nvidia-dcgm/nvvs.log`
  either way. Its plugin start/finish lines name the test that was running.
- **A GPU load timeline**: one line per sample with utilisation, board power
  and SM clock for every GPU plus which `nvvs`-family processes existed. The
  log also states when the cards were last at or above 50 % utilisation and
  how long before the kill that was, which separates a slow test from a hang
  after the load phase.
- **`dcgm_diagnostics.load_profile`** in the report JSON: sample count,
  samples under load, first/last load timestamps and peak summed board power.

**Watching it live.** MAAS only shows a script's output once the script has
finished, so the progress lines are there to read the run as a timeline
afterwards. To watch while it runs, open the BMC remote console (iLO / BMC
KVM or SOL): every progress line is also written to `/dev/console`.

The `nvidia-bug-report` is checked with `gzip -t` before it is printed.
Earlier versions warned `could not decompress` on every report over 2 MB:
`head -c` closed the pipe, `zcat` died of SIGPIPE, and `pipefail` turned that
into a failure *after* the report had been printed in full.

`chassis.fans.all_spinning` is `null`, not `true`, when the BMC exposes no RPM
sensors (HPE iLO reports fans as percentages, which the sampler drops).

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
