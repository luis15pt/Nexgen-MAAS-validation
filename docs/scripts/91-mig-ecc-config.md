# 91 - MIG Disable & ECC Enable (`v1.3.0`)

[← Back to README](../../README.md)

Disables MIG mode and enables ECC on every GPU. Both settings live in GPU
NVRAM, so the script writes them and then activates them with
`nvidia-smi --gpu-reset` rather than requiring a full reboot. If the reset
fails, the change is reported as *pending reboot* instead of being treated as
an error.

Requires script `90` to have run first.

## If ECC was disabled, this run is void

Finding ECC **disabled** fails the run, and the remaining GPU scripts skip
immediately rather than spending 30–90 minutes on evidence that cannot be used.

The reason is that ECC off does not merely mean errors went *unrecorded* — the
hardware was not detecting them at all, so none occurred to record. Enabling ECC
does not backfill: the aggregate counters restart from zero at that moment. Any
zero measured later in the same run therefore proves nothing about the card's
prior life, which is precisely the number a buyer inspects. (H100 ships with ECC
enabled, so finding it off means someone turned it off — often to free the ~6.25%
of VRAM that ECC reserves.)

Script 91 writes a halt marker to `/run/nexgen-commissioning-halt`; scripts `92`,
`92`, `98` and `99` check it and exit in well under a second, reporting
`skipped: true`. The report shows a prominent **action required** banner and
refuses to read as an acceptance certificate.

ECC persists across reboots, so the remedy is simply to commission the machine a
second time. The marker is on tmpfs and cannot survive into that run.

**Why a marker rather than relying on MAAS to stop.** MAAS does *not* abort the
remaining commissioning scripts when one fails — `run_serial_scripts()` in
`maas_run_remote_scripts.py` increments a failure count and continues to the next
script, with no break or early return. A commissioning failure marks the machine
`Failed commissioning` and blocks the *testing* phase, but every remaining
commissioning script still runs. Without the marker, a void run would still spend
its full 30–90 minutes.

The marker is race-free because commissioning scripts run serially in
name-sorted order: `Script.parallel` defaults to `SCRIPT_PARALLEL.DISABLED`, and
those are exactly the scripts `run_serial_scripts()` handles. All five GPU
scripts now declare `parallel: disabled` explicitly rather than relying on that
default — the pipeline already depends on ordering (`92` needs the driver `90`
installs), so the dependency is better stated than assumed.

Two things still survive an ECC-disabled history and remain trustworthy:
**remapped row counts** and the **bank remap availability histogram**, both
recorded in InfoROM as physical spare-row consumption. A non-zero value there is
real evidence of past damage; a zero is ambiguous.

| Env Override | Default | Description |
|---|---|---|
| `NEXGEN_HALT_FILE` | `/run/nexgen-commissioning-halt` | Halt marker path, shared with scripts 92/98/99 |

**Timeout**: 5 minutes
