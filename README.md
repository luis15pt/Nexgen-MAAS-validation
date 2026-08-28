# NexGen MAAS GPU Validation

Automated GPU commissioning, validation, and certification pipeline for bare-metal servers managed by [Canonical MAAS](https://maas.io/). These scripts run during MAAS commissioning to install GPU drivers, collect hardware inventory, execute stress tests, and generate HTML certification reports.

## Folder Structure

```
Nexgen-MAAS-validation/
├── README.md
├── .gitignore
├── .env.example                             # MAAS credentials template
├── commissioning-scripts/        # MAAS commissioning scripts (run in order)
│   ├── 90-nexgen-gpu-install-595-13.sh     # Step 1: Driver + CUDA + DCGM + Fabric Manager
│   ├── 91-nexgen-gpu-mig-ecc-config.sh     # Step 2: Disable MIG, enable ECC
│   ├── 92-nexgen-gpu-inventory.sh          # Step 3: GPU inventory & health check
│   ├── 98-nexgen-gpu-stress-test.sh        # Step 4: DCGM stress test
│   └── 99-nexgen-gpu-burn-in.sh            # Step 5: Sustained load (optional)
├── reporting/                    # Report generation tooling
│   └── device_certificate.py               # HTML certification report generator
├── tests/                        # Offline test suite (no GPU required)
│   ├── run-all.sh                          # Everything
│   ├── run-stress-verdicts.sh              # DCGM verdict matrix
│   ├── run-burnin-verdicts.sh              # Burn-in verdict matrix
│   ├── run-acceptance.py                   # Acceptance adjudication matrix
│   ├── validate-maas-metadata.py           # Metadata block is internally consistent
│   ├── check-python-compat.py              # Generator still runs on the oldest Python we deploy to
│   ├── stubs/                              # Fake nvidia-smi, dcgmi, dmesg, lspci, systemctl, gpu_burn, dcgmproftester13
│   └── fixtures/                           # Captured and synthetic evidence
├── reports/                      # Generated reports (git-ignored)
│   └── .gitkeep
└── examples/                     # Example outputs (sanitized)
    └── EXAMPLE-GPU-001-MAAS-validation.html # Sample GPU commissioning report
```

## Commissioning Scripts

All five scripts run as MAAS commissioning scripts in sequence, and output structured JSON on stdout for the report generator to consume. The numeric prefix sets the execution order: `99` is optional, and every script depends on `90` having run first. Cheap phases run first so a failure costs the least — inventory (`92`) before any load, then the DCGM diagnostic (`98`), then the 1-hour burn-in (`99`) last.

### 90 - GPU Driver Install (`v2.1.6`)

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

| Env Override | Default | Description |
|---|---|---|
| `NVIDIA_DRIVER` | `nvidia-driver-595-server-open` | Driver package name |
| `CUDA_TOOLKIT` | `cuda-toolkit-12-8` | CUDA toolkit package |
| `DCGM_CUDA_MAJOR` | `13` | DCGM CUDA major version |
| `FABRIC_READY_TIMEOUT` | `300` | Seconds to wait for the NVLink fabric to train |

**Timeout**: 20 minutes

### 91 - MIG Disable & ECC Enable (`v1.3.0`)

Disables MIG mode and enables ECC on every GPU. Both settings live in GPU
NVRAM, so the script writes them and then activates them with
`nvidia-smi --gpu-reset` rather than requiring a full reboot. If the reset
fails, the change is reported as *pending reboot* instead of being treated as
an error.

Requires script `90` to have run first.

#### If ECC was disabled, this run is void

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

### 92 - GPU Inventory (`v2.3.0`)

Runs before any load phase, so its counters are the **pre-load delivery
baseline**. Collects detailed GPU hardware inventory via a single bulk
`nvidia-smi` query:

- Serial numbers, UUIDs, and VBIOS versions
- VRAM capacity and free memory
- ECC mode plus corrected/uncorrected volatile and aggregate counters
- Remapped rows and memory bank availability
- Temperature, power draw, and power limit
- PCIe link generation and width, with **Device Max and Host Max kept separate** — `pcie.link.gen.max` is the negotiated ceiling, so a Gen4 host caps it at 4; only the `-q` `Link Info` section distinguishes "the card is Gen4" from "the slot is Gen4"
- NUMA topology mapping
- **NVLink presence**, measured from the topo matrix and `nvlink --status` — the report uses it to tell an expected `nvbandwidth` skip from an unexplained one
- **Physical CPU cores** (`Socket(s)` × `Core(s) per socket`), because MAAS reports logical CPUs and labels them cores
- Raw `nvidia-smi -q -d ROW_REMAPPER,ECC` per card, embedded in the JSON — the artifact the specification names, and what the report renders per serial

Extended ECC and retired-page fields are queried separately from the safe
field set, because they do not exist on every driver version. If the extended
query fails the script degrades to the safe set rather than failing the run.

Also dumps `nvidia-smi`, `nvidia-smi -q`, and `nvidia-smi topo -m` to stderr,
so full GPU state is recoverable from the MAAS commissioning log on nodes that
are not SSH-reachable.

Identity and link evidence is parsed from the full `nvidia-smi -q` document:
product name, board and GPU part numbers, PCI device ID, InfoROM object
versions and a corruption flag, SRAM error counts, and the PCIe ceilings.

The three PCIe generation values are reported separately and the distinction
matters. `pcie.link.gen.max` is the *negotiated* ceiling, which a Gen4 host caps
at 4 on a perfectly good Gen5 card — on its own it cannot tell a weak card from
a weak slot. **Device Max** proves the card, **Host Max** explains the observed
value, and the idle *current* value is recorded but never treated as a fault,
since links downtrain to Gen1 to save power.

The full `-q` output is retained per GPU as base64 alongside a
`-q -d ROW_REMAPPER,ECC` capture, and also written to stderr where MAAS keeps it.

Outputs structured JSON. Installs no packages — depends on script `90`.

**Timeout**: 5 minutes

### 98 - GPU Stress Test (`v2.3.0`)

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

### 99 - Sustained Burn-In (`v1.6.0`, optional)

Applies a sustained full-power load and records what the GPUs actually did
under it. A DCGM diagnostic characterises a card for minutes; reballed and
reflowed cards pass cold and fail hot, so only sustained load exposes them.

Runs **last**: it is by far the longest phase, so anything that can fail
cheaply has already failed by the time it starts.

Optional by construction — skip it by not uploading it to MAAS.

gpu-burn is built from source against the installed CUDA toolkit, for the
compute capability actually present: the upstream Makefile defaults to
`COMPUTE=75`, which would build the fault-detection compare kernel as Turing PTX
and JIT it onto a Hopper card. The script reads `compute_cap` and passes it
through.

The JSON report is the deliverable, so it is assembled to a file, validated, and
only then emitted; if it cannot be built the script reports FAIL with the reason
rather than an unevidenced PASS. Log artifacts reach `jq` by path, never as
arguments — a single argv entry is capped at 131072 bytes, and the base64 of an
hour of gpu-burn output is far past it.

Per GPU it records min/max/mean power, GPU and memory temperature, SM clock,
and per-reason throttle sample counts. ECC, remapped-row and PCIe replay
counters are snapshotted either side of the window and reported as **deltas**,
because an absolute replay count can include boot-time link training that is
not a defect. `dmesg` is diffed across the window and Xid events are split into
the disqualifying set (48/63/64/79/92/94/95) and the rest — 13/31/43 are
typically application faults raised by the load itself.

| Env Override | Default | Description |
|---|---|---|
| `BURN_DURATION` | `3600` | Seconds of sustained load. 1 hour, double the 1800 s the spec asks for, because 5 min is not thermal steady state. **Sets total commissioning time (~80 min) — raise the MAAS node timeout first** |
| `BURN_MODE` | `characterize` | `characterize` measures and gates on nothing; `enforce` applies the floors below |
| `BURN_TOOL` | `auto` | `auto` prefers `gpu-burn`; then `dcgmi-diag`; then `dcgmproftester` |
| `BURN_GPU_BURN_ARGS` | (empty) | Extra gpu-burn flags — `-tc` for tensor cores, `-d` for double precision |
| `BURN_MIN_SM_CLOCK_MHZ` | (unset) | Sustained SM clock floor, `enforce` only |
| `BURN_MIN_POWER_W` | (unset) | Minimum peak power, `enforce` only |
| `BURN_SAMPLE_INTERVAL` | `10` | Seconds between telemetry samples |
| `BURN_LOADED_POWER_FRAC` | `0.5` | Fraction of a GPU's own power limit that counts as "under load" |
| `BURN_COVERAGE_FRAC` | `0.9` | Fraction of `BURN_DURATION` each GPU must actually spend under load |
| `ARTIFACT_MAX_BYTES` | `131072` | Bytes of each raw log retained (base64) in the JSON report, kept from the **tail** |

**Load coverage is measured per GPU, not per run.** A real run had
`dcgmproftester` cover four of eight cards for the full window and the other four
for roughly 290 seconds, while the run's own duration looked correct — so the
report would have claimed a sustained load on cards that never got one. Each
GPU's `loaded_seconds` is now counted from its own power draw, and a card that
falls short fails in either mode: a card that was not loaded was not tested. The
tool is also given an explicit GPU id list rather than being left to choose.

**Run `characterize` first.** The throttle and clock thresholds should come from
measurement, not assumption: run it across known-good, properly-cooled cards to
find out whether they ever reach thermal slowdown and what their sustained clock
floor actually is, then set `BURN_MIN_SM_CLOCK_MHZ` from that data. `enforce`
without a floor warns rather than silently passing.

#### Load source

**`gpu-burn` is the default**, because it is what the acceptance specification
names for the sustained phase, it loads every GPU in one process by design, and
— uniquely among the options — it reports a **per-GPU `OK`/`FAULTY` verdict** plus
a computation error count. That is arithmetic verification, which no amount of
power and temperature telemetry can substitute for: a card can sit at 350W and
71°C and still return wrong results.

The report therefore treats it as the strongest single piece of burn-in
evidence. A `FAULTY` card, any non-zero error count, a GPU gpu-burn did not
report on, and a run that produced no summary at all are each a failure.

It is built from source on the node when not already installed, which needs
`git`, `make`, `g++`, `nvcc` (from the CUDA toolkit script `90` installs) and
outbound network to GitHub. Every failure path in the build is logged — a silent
build failure would otherwise downgrade the run to a weaker load source without
anyone noticing.

Two fallbacks exist, in order:

- **`dcgmi diag -r targeted_stress`** (`BURN_TOOL=dcgmi-diag`). NVIDIA's own
  sustained-load path: part of the level-3 diagnostic, every GPU in one
  invocation, duration via a documented parameter
  (`targeted_stress.test_duration`, default 30s). Its non-zero exit is a
  hardware verdict, so it is treated as one — but it gives no per-GPU
  arithmetic result.

- **`dcgmproftester`** (`BURN_TOOL=dcgmproftester`). Worth knowing what this
  actually is: NVIDIA documents it as a load generator for validating DCGM's
  *measurement* path, not as a stress or burn-in tool. `-t 1004` drives a
  half-precision matrix-multiply-accumulate on the tensor cores
  (`DCGM_FI_PROF_PIPE_TENSOR_ACTIVE`). It also **batches** GPUs when handed a
  multi-GPU id list — a real 8-GPU run drove four cards for 1780s, then
  re-initialised and started the other four, getting 280s into them before the
  ceiling. It is therefore launched as **one process per GPU, concurrently**.
If no load generator can be found the run **fails**: no load applied means
nothing was tested.

**Timeout**: 90 minutes

## Report Generator

`reporting/device_certificate.py` (v3.3.0) generates a consolidated HTML certification report from MAAS commissioning data.

### Prerequisites

```bash
pip install requests-oauthlib
```

Python **3.8 or newer**. The generator is often run from a different host than
it is developed on, and `from __future__ import annotations` hides most of that
difference — the `X | None` annotations are never evaluated, so the module
imports cleanly on 3.8 and an incompatibility surfaces mid-run instead. That is
what `tests/check-python-compat.py` exists to catch; it checks syntax *and*
runtime API use, because a syntax check cannot see an attribute call.

### Configuration

Copy the example env file and fill in your MAAS credentials:

```bash
cp .env.example .env
```

```ini
# .env
MAAS_URL=http://your-maas-server:5240/MAAS
MAAS_API_KEY=consumer:token:secret
```

The `.env` file is git-ignored and will never be committed. The script loads it automatically — no need to export variables manually.

You can also use env vars or CLI flags (`--maas-url`, `--api-key`) which take priority over `.env`.

### Usage

**From MAAS API (recommended):**

```bash
python3 reporting/device_certificate.py --host EXAMPLE-GPU-001 -o reports/EXAMPLE-GPU-001-MAAS-validation.html
```

**From local JSON files (offline/fallback):** each flag takes that script's
stdout. Any subset works — omitted stages render as `N/A`, which degrades the
verdict rather than being ignored.

```bash
python3 reporting/device_certificate.py \
  --install   90-output.json \
  --config    91-output.json \
  --inventory 92-output.json \
  --stress    98-output.json \
  --burnin    99-output.json \
  -o reports/report.html
```

In file mode there is no MAAS to query, so the full per-script logs are
unavailable and the report falls back to the excerpts the scripts embed in their
own JSON. MAAS-derived hardware detail (DIMMs, NICs, storage, CPU topology) is
also absent.

The generated report includes:
- **Hardware acceptance, per GPU** — up to 24 criteria per card across the 8
  reject conditions, as a criterion × GPU matrix, with a collapsible evidence
  block per serial holding that card's raw `nvidia-smi` output
- Machine hardware summary (CPU, RAM, storage, network)
- Per-GPU identity, driver, firmware, and configuration details
- ECC counters, remapped rows, and memory bank availability
- DCGM diagnostic results with pass/fail status
- Sustained-load telemetry and counter deltas, when script `99` ran
- Raw logs, embedded and collapsed (see below)
- DIMM inventory from lshw
- Overall validation verdict

### Raw evidence in the report

The acceptance specification asks that the evidence be retained, not just the
verdict, and the HTML file is the whole deliverable — whoever receives it is not
assumed to have access to the MAAS that produced it. **Nothing links out.** Every
report carries, collapsed by default:

- `nvidia-smi -q -d ROW_REMAPPER,ECC` for each card — the artifact Section 2 names — nested inside that card's row in the acceptance table
- the full `nvidia-smi -q` device record per card
- the complete commissioning log for every script, when MAAS is the data source
- otherwise the excerpts the scripts embed in their own JSON: the sustained-load log, the kernel-log window scanned for Xid, and the DCGM diagnostic output

The one transformation applied is collapsing carriage-return repaints. gpu-burn
rewrites its progress line in place roughly 70 times per 0.1% step it actually
reports, which is what turns a 1-hour run into ~22 MB. Collapsing them keeps one
line per reported step — complete at the tool's own granularity, carrying that
step's latest counters — and each block states how many were collapsed. Measured
on a log of the real shape: 433 KB → 6.3 KB, with every reported step and the
per-GPU verdict intact.

`ARTIFACT_MAX_BYTES` (script side) bounds what each script embeds in its own
JSON; the generator's own cap is a 4 MB backstop, and if it ever engages the
block says so in its summary. `@media print` omits the logs, so a printed
certificate stays readable.

### Hardware acceptance

One report per server — servers ship with their GPUs installed — but every
serial carries its own identity, counters, telemetry and per-criterion evidence
in a collapsible block, so the report is per-GPU rather than lot-level. A server
holding a rejectable card cannot show a `PASS` headline, and the issue names the
offending serials.

Two criteria deliberately depart from a literal reading of a strict acceptance
specification, because a literal reading fails healthy hardware:

- **Throttling.** A card at sustained full load sits against its power limiter
  and reports `sw_power_cap`. That is the limiter working, not a defect. Only
  `hw_thermal_slowdown`, `hw_power_brake_slowdown` and `sw_thermal_slowdown` are
  treated as faults.
- **PCIe.** Links downtrain to Gen1 at idle, so the idle value proves nothing,
  and comparing against a fixed Gen5 would fail every card in a Gen4 host. What
  is checked is that the link reached the best generation *both* ends support,
  plus full width. A Gen4 host capping a Gen5 card reads as "host is the limit,
  not the card".

Three requirements common to such specifications are **not machine-verifiable**
and remain manual attestations: matching a serial against the *physical label*
(software can only prove the two nvidia-smi sources agree), "chassis providing
spec airflow", and any test-date-relative-to-shipment window — the test date is
recorded, but the ship date is not knowable here.

The report also cross-references script `91`: if ECC had to be **enabled during
the run**, the aggregate counters cannot evidence the card's prior life and the
card is rejected with an instruction to re-commission. If script `91`'s result is
missing altogether the prior ECC state is unknown, which is treated as needing
review rather than as a pass.

One evidentiary limit is worth stating plainly: **aggregate ECC counters only
mean something if ECC was enabled during the card's prior life.** Aggregate
counters persist in InfoROM where volatile ones reset, which is why they are the
ones to ask for — but a used card that ran with ECC *disabled* has no aggregate
history, and the zero means nothing was ever counted, not that the card is
clean. Script `91` enables ECC, so any card it had to change has no meaningful
history by definition.

> **Known gap**: script `90` emits `fabric_required`, `fabric_manager_*`,
> `fabric_state`, and `fabric_ready` in its JSON, but the HTML report does not
> render them yet. On NVSwitch nodes, check the fabric verdict in the MAAS
> commissioning output for script `90` directly.

## Example Report

**[View live example report](https://luis15pt.github.io/Nexgen-MAAS-validation/examples/EXAMPLE-GPU-001-MAAS-validation.html)**

Or see the source at [`examples/EXAMPLE-GPU-001-MAAS-validation.html`](examples/EXAMPLE-GPU-001-MAAS-validation.html).

## Adding Scripts to MAAS

> **The metadata comment block is documentation only — MAAS does not parse it.**
> It uses `# --- Start MAAS Metadata ---`, and MAAS matches
> `# --- Start MAAS 1.0 script metadata ---`, so MAAS ignores the block and takes
> `name`, `script_type` and everything else from the upload command.
>
> That is deliberate, and was arrived at the hard way. Switching to the delimiter
> MAAS parses made MAAS start *validating* the block, and uploads stopped working
> against MAAS 3.1.4 in this environment — first with *"May not override values
> defined in embedded YAML"* when a supplied `name=` differed from the embedded
> one, then with *"This field is required"* when the embedded `name` was removed,
> and then still failing once both were consistent. Reverting to the ignored
> delimiter restored working uploads. Which key MAAS 3.1.4 objected to was never
> established.
>
> **Consequence worth knowing:** the declared `timeout` values are therefore *not*
> enforced by MAAS. `Script.timeout` defaults to 0, which means no timeout. The
> real protection is the in-script `timeout` wrappers around `dcgmi diag` and the
> load generator, which bound the two phases that can actually hang. Titles and
> descriptions likewise do not appear in the MAAS UI.
>
> `tests/validate-maas-metadata.py` checks the block is internally consistent and
> that `name` matches the filename — a stale name silently registers a second
> script — and fails if the live MAAS delimiter reappears.

Upload the commissioning scripts via the MAAS CLI. MAAS does not inject
per-script configuration, so anything a script needs is a default inside it:
`BURN_DURATION` in particular sets total commissioning time and has to be edited
before upload.

```bash
maas $PROFILE commissioning-scripts create \
  name=90-nexgen-gpu-install-595-13 \
  script_type=commissioning \
  hardware_type=gpu \
  content@=commissioning-scripts/90-nexgen-gpu-install-595-13.sh

maas $PROFILE commissioning-scripts create \
  name=91-nexgen-gpu-mig-ecc-config \
  script_type=commissioning \
  hardware_type=gpu \
  content@=commissioning-scripts/91-nexgen-gpu-mig-ecc-config.sh

maas $PROFILE commissioning-scripts create \
  name=92-nexgen-gpu-inventory \
  script_type=commissioning \
  hardware_type=gpu \
  content@=commissioning-scripts/92-nexgen-gpu-inventory.sh

maas $PROFILE commissioning-scripts create \
  name=98-nexgen-gpu-stress-test \
  script_type=commissioning \
  hardware_type=gpu \
  content@=commissioning-scripts/98-nexgen-gpu-stress-test.sh

# Optional -- omit this one to skip the sustained burn-in entirely
maas $PROFILE commissioning-scripts create \
  name=99-nexgen-gpu-burn-in \
  script_type=commissioning \
  hardware_type=gpu \
  content@=commissioning-scripts/99-nexgen-gpu-burn-in.sh
```

## Workflow

```
Commission Machine in MAAS
         │
         ▼
   90 - Install Drivers ──► nvidia-driver-595 + CUDA 12.8 + DCGM 4.x
         │                  + Fabric Manager (NVSwitch/NVL nodes)
         ▼
   91 - MIG / ECC Config ─► MIG off, ECC on (via --gpu-reset)
         │
         ▼
   92 - GPU Inventory ────► JSON: identity, ECC, remapped rows, PCIe, NUMA
         │                  (pre-load delivery baseline)
         ▼
   98 - DCGM Diagnostics ─► dcgmi diag (level 1-4)
         │
         ▼
   99 - Burn-In (opt.) ───► 1 h sustained load: power, temp, clocks,
         │                  throttle reasons, Xid, counter deltas
         │
         ▼
   device_certificate.py ─► reports/<hostname>-MAAS-validation.html
```

> **Raise the MAAS node timeout before running the burn-in.** Settings →
> Configuration, default **30 minutes**. With `BURN_DURATION=3600` the pipeline
> is roughly 75 minutes of scripts plus ~4 of PXE and ephemeral boot; measured on
> an 8× H100 node the scripts alone were 18.9 min at a 300 s burn and 73.9 min at
> 3600 s. Past the node timeout MAAS marks the machine **Failed** while the
> burn-in is still working, and the run is lost.

Observed stage timings on an 8× H100 PCIe node (1-hour burn):

| Stage | Duration |
|---|---|
| `90` driver install | 4 m 21 s |
| `91` MIG / ECC | 3 s |
| `92` inventory | 15 s |
| `98` DCGM level 3 | 8 m 12 s |
| `99` burn-in | 62 m 12 s |

## Testing

The whole suite runs offline against fixtures and stub tooling — no GPU, no MAAS:

```bash
./tests/run-all.sh          # everything: 113 assertions
```

`run-all.sh` is the entry point and invokes the rest, so its count is the total.
Each can also be run on its own when iterating on one area:

```bash
./tests/run-stress-verdicts.sh   # DCGM verdict matrix
./tests/run-burnin-verdicts.sh   # burn-in verdict matrix (~60 s, real load windows)
./tests/run-acceptance.py        # per-GPU acceptance adjudication
./tests/check-python-compat.py   # generator still runs on Python 3.8
./tests/validate-maas-metadata.py
```

`tests/stubs/` holds fixture-driven fakes for `nvidia-smi`, `dcgmi`,
`dcgmproftester13`, `gpu_burn`, `dmesg`, `systemctl` and `lspci`, so the
commissioning scripts run end to end on a machine with no GPU. `tests/fixtures/`
holds captured `nvidia-smi -q` documents, synthetic DCGM result sets, and
report-level JSON generated from real script output.

Beyond the verdict matrices, the suite pins the specific defects that reached
production, because each one produced a plausible-looking pass:

| Guard | The defect it pins |
|---|---|
| Report assembly under a large artifact | A single argv entry is capped at 131072 bytes, so `jq --arg <2 MB base64>` died with `E2BIG`. With pipefail but no `errexit` the script then printed `Verdict: PASS` and no JSON at all |
| Counter columns resolved by name | `SNAP_FIELDS` is discovered at runtime; when driver 595 dropped `pcie.replay_counter` every later column shifted, and the uncorrectable-remap delta became permanently null — silently disabling the FAIL gate that depends on it |
| Error count across a 250 KB log | `cmd \| head -1 \|\| echo 0` is racy under pipefail: once `sort` is still writing when `head` closes the pipe, the fallback *also* fires and the value becomes `"0\n0"` — invalid JSON, which took the whole report down |
| CPU cores are physical cores | MAAS NUMA `cores` lists hold *logical* cpu ids, so summing them reported a 2×64C/256T machine as "256 cores" |
| nvbandwidth skip adjudication | Excused only when NVLink is positively measured absent — never on an unknown, and never when another test also skipped |
| Raw logs embedded, not truncated | Oversized logs keep their tail, where the per-GPU verdict is printed; log text is escaped; unusable base64 degrades to empty |
| Python 3.8 compatibility | `str.removesuffix` imported fine and failed mid-run |

The acceptance matrix asserts every reject condition **and** the inverse cases
that guard against false failures — a power-capped card, a Gen5 card in a Gen4
host, an idle link at Gen1, and a fully healthy card must all be accepted. Those
inverses matter as much as the positives: a check that fails good hardware is
worse than no check.

## Error counters across the pipeline

Every phase after the inventory re-reads the ECC, remapped-row and PCIe replay
counters and compares them against a **pre-load baseline**, so a fault is
attributed to the phase that induced it.

```
92  inventory   writes the baseline   /run/nexgen-gpu-counter-baseline.json
98  stress      delta vs baseline  +  final nvidia-smi -q
99  burn-in     delta vs baseline  +  delta across its own load window
```

This exists because a window-only delta is not enough. The DCGM diagnostic in
script `98` is itself roughly 35 minutes of load on an 8-GPU host, and an error
it induces is invisible to both of the other measurements: script `92` ran
before it, so its absolutes are clean, and the burn-in subtracts a baseline
taken *after* `98`, so the error is already in it and the delta comes out zero.
Both `98` and `99` therefore compare against `92`'s baseline, which also means
the check still happens when the optional burn-in is not uploaded.

Each of these fails the run and rejects the card: **new uncorrectable ECC
errors**, **new uncorrectable remapped rows**, or a **remap pending or remap
failure flag that was not set at baseline**. Corrected-error and replay deltas
are reported rather than gated — they are the delivery baseline, not a fault.

Which phase induced a fault is therefore recoverable: `98`'s delta isolates the
diagnostic, `99`'s window delta isolates the burn-in, and `99`'s delta against
the baseline is the cumulative total the certificate reports. A fault raised
during `98` is consequently mentioned twice — once by `98`, once inside `99`'s
cumulative figure. That is one fault, reported from two vantage points.

A missing baseline is reported as such, never treated as clean. So is a counter
that went **backwards**: a value below the baseline was cleared mid-sequence
(`nvidia-smi -p` clears ECC counts, and `replays_since_reset` is per-reset by
definition), which leaves the comparison with no history to reason over. That
marks the card for review rather than passing it — the same reasoning that makes
aggregate counters preferable to volatile ones in the first place, applied one
level up.

Counters are read from `nvidia-smi -q` text rather than `--query-gpu`, because
several counter fields are absent on current drivers and one missing field takes
the whole query with it.

Both phases also dump a closing `nvidia-smi -q` to stderr, so the final state
of every card is retained in the MAAS commissioning log as the last word on it.

| Env Override | Default | Description |
|---|---|---|
| `NEXGEN_BASELINE_FILE` | `/run/nexgen-gpu-counter-baseline.json` | Baseline path, shared by scripts 92/98/99 |

## Troubleshooting

### `nvidia-fabricmanager` starts, then exits immediately

Version mismatch. The log reads *"fabric manager version A doesn't match
driver version B"*. FM must match the running driver on all three components
(`X.Y.Z`), not just the branch. Check:

```bash
nvidia-smi --query-gpu=driver_version --format=csv,noheader | head -1
dpkg-query -W -f='${Version}\n' 'nvidia-fabricmanager-*'
```

### Fabric stuck at `In Progress` and never reaches `Completed`

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

### CUDA fails with error 802 (`system not yet initialized`)

Fabric not trained. See above — this is the CUDA-side symptom, not a separate
problem.

### `NVRM: BAR0 is 0M @ 0x0` and the driver refuses to load

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

## Design Decisions

**Separate scripts, not one** -- Splitting install/config/inventory/stress into separate scripts means MAAS shows granular pass/fail per phase. If the driver install fails, you see that immediately without wading through inventory output.

**Pinned driver versions, with a fallback** -- After hitting `nvidia-smi` field incompatibilities with driver 590 (removed `cuda_version` query field, changed `memory.type` behavior), we pin an exact driver branch and encode it in the filename. The pin is not a hard failure though: if the pinned branch has no installable candidate (a new branch that is not published for the distro yet), the script degrades to the newest available `nvidia-driver-*-server-open` rather than aborting commissioning.

**Fabric Manager version is derived, not pinned** -- Fabric Manager refuses to start unless its version matches the running driver exactly, and the running driver is not necessarily the pinned one (see the fallback above). So the FM branch and exact version are read back from `nvidia-smi` at runtime instead of being assumed from the package name.

**Fabric detection reads `nvidia-smi -q`, not device nodes** -- The obvious signals are unreliable on current hardware: B300/NVL systems expose no `/dev/nvidia-nvswitch*` nodes at all, and the `--query-gpu=fabric.state` CSV field does not report correctly. The `Fabric:` section of `nvidia-smi -q` is the signal that actually works across A100 SXM, B200, and B300.

**A fabric that never trains is a hard FAIL** -- On a fabric-attached node, CUDA and DCGM cannot initialize without a trained NVLink fabric, so passing commissioning would certify a machine that cannot run a single job. The install JSON is written *before* the failure return, so the `fabric_*` diagnostic fields survive on a failed run.

**ASCII-only script output** -- MAAS terminal rendering mangles UTF-8 box-drawing characters and emoji. All script *output* uses plain ASCII formatting. Note that UTF-8 in script *comments* is also mangled when a script is pulled back out of MAAS (it comes back latin-1 decoded, so `─` returns as `â`); check comment blocks after any MAAS round-trip.

**Resilient nvidia-smi queries** -- Every field query has a fallback. If a field doesn't exist in the driver version (e.g., `retired_pages` on consumer GPUs), it degrades gracefully to "N/A" rather than crashing.

**DCGM optional** -- DCGM packages aren't always available for every driver version. The stress test detects DCGM availability and exits cleanly if absent, rather than failing the commissioning run.

**Adjudication lives in the report generator, not in the scripts** -- The scripts collect evidence and emit JSON; `device_certificate.py` decides what it means. This is not just tidiness: a rule in Python applies to commissioning data MAAS already holds, so a corrected reading reaches existing certificates without re-running anything, and the rule exists in exactly one place. Stage verdicts are *derived* rather than inherited -- a stage whose findings are all reclassified or filtered drops to PASS on its own -- which is the mechanism the nvbandwidth and PCIe-link filters both rely on. A rule that was briefly implemented in script 98 as well was removed for exactly this reason.

**Only adverse findings hold a stage below PASS** -- The findings table is for things needing attention. Informational notes -- "no NVLink fitted", "characterize mode" -- are commentary on evidence the per-GPU acceptance section already adjudicates, so they do not gate a stage and burn-in mode commentary is filtered out of the table entirely. The filter matches on info severity only, so a genuine critical finding can never be caught by it.

**Dual mode report generator** -- Supports both MAAS API mode (pulls data directly, recommended) and file-based mode (offline, self-contained) for environments without API access at report time.
