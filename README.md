# NexGen MAAS GPU Validation

Automated GPU commissioning, validation, and certification pipeline for bare-metal servers managed by [Canonical MAAS](https://maas.io/). These scripts run during MAAS commissioning to install GPU drivers, collect hardware inventory, execute stress tests, and generate HTML certification reports.

## Folder Structure

```
Nexgen-MAAS-validation/
├── README.md
├── .gitignore
├── .env.example                             # MAAS + NetBox credentials template
├── commissioning-scripts/        # MAAS commissioning scripts (run in order)
│   ├── 80-nexgen-network-cabling-verify.sh # Step 0: NetBox vs LLDP cabling check
│   ├── 90-nexgen-gpu-install-595-13.sh     # Step 1: Driver + CUDA + DCGM + Fabric Manager
│   ├── 91-nexgen-gpu-mig-ecc-config.sh     # Step 2: Disable MIG, enable ECC
│   ├── 92-nexgen-gpu-burn-in.sh            # Step 3: Sustained load (optional)
│   ├── 98-nexgen-gpu-inventory.sh          # Step 4: GPU inventory & health check
│   └── 99-nexgen-gpu-stress-test.sh        # Step 5: DCGM stress test
├── reporting/                    # Report generation tooling
│   └── device_certificate.py               # HTML certification report generator
├── tests/                        # Offline test suite (no GPU required)
│   ├── run-all.sh                          # Everything
│   ├── run-99-verdicts.sh                  # DCGM verdict matrix
│   ├── run-92-burnin.sh                    # Burn-in verdict matrix
│   ├── run-acceptance.py                   # Acceptance adjudication matrix
│   ├── stubs/                              # Fake nvidia-smi, dcgmi, dmesg, lspci
│   └── fixtures/                           # Captured and synthetic evidence
├── reports/                      # Generated reports (git-ignored)
│   └── .gitkeep
└── examples/                     # Example outputs (sanitized)
    └── EXAMPLE-GPU-001-MAAS-validation.html # Sample GPU commissioning report
```

## Commissioning Scripts

All six scripts are designed to run as MAAS commissioning scripts in sequence. They follow the MAAS metadata format and output structured JSON for downstream consumption. The numeric prefix sets the execution order — `80` is independent, `92` is optional, and every GPU script depends on `90` having run first.

### 80 - Network Cabling Verify (`v1.0.0`)

Verifies physical cabling before any GPU work runs. Identifies the host by DMI serial (fallback: hostname), pulls the planned per-interface cable destinations from NetBox 4.x, collects live LLDP neighbours on every physical NIC, and fails commissioning on any mismatch.

| Env Override | Default | Description |
|---|---|---|
| `NETBOX_URL` | `https://netbox.example.com` | NetBox base URL -- **edit before upload** |
| `NETBOX_TOKEN` | `CHANGEME` | NetBox API token -- **edit before upload** |
| `LLDP_WAIT_SECONDS` | `90` | Seconds to wait for LLDP neighbour discovery after starting `lldpd` |
| `MATCH_BY_ENDPOINT` | `true` | Match planned cables by `(switch, port)` rather than requiring the local NIC name to equal the NetBox interface name |
| `BMC_IFNAME_PATTERN` | `^(ipmi\|bmc\|mgmt\|management)$` | NetBox interfaces matching this regex are reported as `NOT_CHECKED` (info) -- the host OS can't observe BMC LLDP |
| `REQUIRE_ALL_PLANNED` | `true` | Fail if any NetBox-planned cable has no LLDP neighbour |
| `ALLOW_EXTRA_LINKS` | `false` | Strict mode: any live NIC without a NetBox cable fails the run |
| `NAME_MATCH_STRICT` | `false` | Require exact (post-normalisation) port-name match instead of suffix match |
| `IGNORE_IFNAMES` | (empty) | Comma-separated list of local NICs to exempt (e.g. management NIC) |

Installs `lldpd` via apt if missing. Writes a planned-vs-actual ASCII table to stderr and structured JSON (`planned`, `actual`, `comparison`, `verdict`) to stdout. Row results: `OK`, `NOT_CHECKED` (BMC/IPMI, info only), `WRONG_SWITCH`, `WRONG_PORT`, `MISSING_LINK`, `LOCAL_NIC_NOT_FOUND`, `UNPLANNED_LINK`, `NO_LLDP_ON_UP_NIC`, `UNPLANNED_IDLE`. A fleet-level `LLDP_SWITCH_OFFLINE` issue is raised when the host has ≥1 up NIC but zero neighbours, so the verdict reads as a switch-config problem rather than a flurry of per-row `MISSING_LINK` failures. On `MISSING_LINK` the reason string lists down NICs, up-with-no-LLDP NICs, and any neighbours observed elsewhere so cable/optic/switch-config failures can be told apart at a glance. When zero LLDP neighbours are observed on a host with Mellanox NICs, the script auto-installs `mft` and dumps firmware `LLDP_NB`/`DCBX` settings to help diagnose firmware-level LLDP intercept.

#### Example scenarios (defaults)

Baseline: NetBox plans `IPMI → mgmt-sw/Gi1/0/6`, `eth0 → leaf254a/Eth1/4`, `eth1 → leaf254b/Eth1/4`. Host has 2× mlx5 + 2× igb NICs.

| # | Physical reality | Per-row results | Extra NICs | Overall |
|---|---|---|---|---|
| 1 | Both mlx5 cabled exactly as planned. igb NICs down. | IPMI `NOT_CHECKED` · eth0 `OK` · eth1 `OK` | eno1/eno2 silenced (down) | **PASS** |
| 2 | `eth1` cable unplugged. `eth0` fine. | eth0 `OK` · eth1 `MISSING_LINK` (reason names down NICs) | — | **FAIL** |
| 3 | Both mlx5 cables swapped: mlx5_0→leaf254a, mlx5_1→leaf254b (opposite of plan). | eth0 `OK` · eth1 `OK` — endpoint mode accepts any host NIC that carries the planned `(switch, port)` | — | **PASS** |
| 4 | `eth0` moved to wrong leaf port: leaf254a/Eth1/**5** instead of Eth1/4. | eth0 `MISSING_LINK` (reason: "Observed elsewhere: …leaf254a/Ethernet1/5") · eth1 `OK` | — | **FAIL** |
| 5 | `eth0` moved to a different leaf: leaf253a/Eth1/4. | eth0 `MISSING_LINK` (reason names wrong leaf) · eth1 `OK` | — | **FAIL** |
| 6 | Planned links correct, but a 3rd cable plugged into eno1 with LLDP neighbour not in NetBox. | eth0 `OK` · eth1 `OK` | eno1 `UNPLANNED_LINK` critical | **FAIL** |
| 7 | Planned links correct, eno1 is up but its switch port has LLDP disabled. | eth0 `OK` · eth1 `OK` | eno1 `NO_LLDP_ON_UP_NIC` critical — "enable LLDP on the switch" | **FAIL** |
| 8 | LLDP disabled fleet-wide (nothing observed on any up NIC). | all planned rows `MISSING_LINK` | — | **FAIL** + top-level `LLDP_SWITCH_OFFLINE` alert + mlx firmware diagnostics block |
| 9 | NetBox entry `LOM` (not in BMC regex) has no host counterpart. | LOM `MISSING_LINK` | — | **FAIL** — add `LOM` to `BMC_IFNAME_PATTERN` to exempt |

**Timeout**: 5 minutes

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

### 91 - MIG Disable & ECC Enable (`v1.2.0`)

Disables MIG mode and enables ECC on every GPU. Both settings live in GPU
NVRAM, so the script writes them and then activates them with
`nvidia-smi --gpu-reset` rather than requiring a full reboot. If the reset
fails, the change is reported as *pending reboot* instead of being treated as
an error.

Requires script `90` to have run first.

**Timeout**: 5 minutes

### 92 - Sustained Burn-In (`v1.0.0`, optional)

Applies a sustained full-power load and records what the GPUs actually did
under it. A DCGM diagnostic characterises a card for minutes; reballed and
reflowed cards pass cold and fail hot, so only sustained load exposes them.

Optional by construction — skip it by not uploading it to MAAS.

Per GPU it records min/max/mean power, GPU and memory temperature, SM clock,
and per-reason throttle sample counts. ECC, remapped-row and PCIe replay
counters are snapshotted either side of the window and reported as **deltas**,
because an absolute replay count can include boot-time link training that is
not a defect. `dmesg` is diffed across the window and Xid events are split into
the disqualifying set (48/63/64/79/92/94/95) and the rest — 13/31/43 are
typically application faults raised by the load itself.

| Env Override | Default | Description |
|---|---|---|
| `BURN_DURATION` | `1800` | Seconds of sustained load |
| `BURN_MODE` | `characterize` | `characterize` measures and gates on nothing; `enforce` applies the floors below |
| `BURN_TOOL` | `auto` | `auto` prefers `dcgmproftester`; `gpu-burn` builds from source |
| `BURN_MIN_SM_CLOCK_MHZ` | (unset) | Sustained SM clock floor, `enforce` only |
| `BURN_MIN_POWER_W` | (unset) | Minimum peak power, `enforce` only |
| `BURN_SAMPLE_INTERVAL` | `10` | Seconds between telemetry samples |

**Run `characterize` first.** The throttle and clock thresholds should come from
measurement, not assumption: run it across known-good, properly-cooled cards to
find out whether they ever reach thermal slowdown and what their sustained clock
floor actually is, then set `BURN_MIN_SM_CLOCK_MHZ` from that data. `enforce`
without a floor warns rather than silently passing.

`dcgmproftester` is the default load source because it ships with DCGM, which
script `90` already installs — no compilation, no network. `gpu-burn` is
available as a genuinely independent load source but is built from source on the
node, which needs git, nvcc and outbound network. If no load generator can be
found the run **fails**: no load applied means nothing was tested.

**Timeout**: 90 minutes

### 98 - GPU Inventory (`v2.2.0`)

Collects detailed GPU hardware inventory via a single bulk `nvidia-smi` query:

- Serial numbers, UUIDs, and VBIOS versions
- VRAM capacity and free memory
- ECC mode plus corrected/uncorrected volatile and aggregate counters
- Remapped rows and memory bank availability
- Temperature, power draw, and power limit
- PCIe link generation and width (current vs. max)
- NUMA topology mapping

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

### 99 - GPU Stress Test (`v2.2.0`)

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

## Report Generator

`reporting/device_certificate.py` (v3.3.0) generates a consolidated HTML certification report from MAAS commissioning data.

### Prerequisites

```bash
pip install requests-oauthlib
```

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

**From local JSON files (offline/fallback):**

```bash
python3 reporting/device_certificate.py \
  --install 90-output.json \
  --inventory 98-output.json \
  --stress 99-output.json \
  -o reports/report.html
```

The generated report includes:
- **Hardware acceptance, per GPU** — 21 criteria per card as a criterion × GPU
  matrix, with a collapsible evidence block per serial
- Machine hardware summary (CPU, RAM, storage, network)
- Per-GPU identity, driver, firmware, and configuration details
- ECC counters, remapped rows, and memory bank availability
- DCGM diagnostic results with pass/fail status
- Sustained-load telemetry and counter deltas, when script `92` ran
- DIMM inventory from lshw
- Overall validation verdict

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

Upload the commissioning scripts via the MAAS CLI. **Before uploading the
80- script**, edit its `NETBOX_URL` / `NETBOX_TOKEN` defaults (top of the
file) -- MAAS does not inject per-script secrets:

```bash
maas $PROFILE commissioning-scripts create \
  name=80-nexgen-network-cabling-verify \
  script_type=commissioning \
  hardware_type=network \
  content@=commissioning-scripts/80-nexgen-network-cabling-verify.sh

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

# Optional -- omit this one to skip the sustained burn-in entirely
maas $PROFILE commissioning-scripts create \
  name=92-nexgen-gpu-burn-in \
  script_type=commissioning \
  hardware_type=gpu \
  content@=commissioning-scripts/92-nexgen-gpu-burn-in.sh

maas $PROFILE commissioning-scripts create \
  name=98-nexgen-gpu-inventory \
  script_type=commissioning \
  hardware_type=gpu \
  content@=commissioning-scripts/98-nexgen-gpu-inventory.sh

maas $PROFILE commissioning-scripts create \
  name=99-nexgen-gpu-stress-test \
  script_type=commissioning \
  hardware_type=gpu \
  content@=commissioning-scripts/99-nexgen-gpu-stress-test.sh
```

## Workflow

```
Commission Machine in MAAS
         │
         ▼
   80 - Cabling Verify ───► NetBox planned vs. LLDP actual (fails fast)
         │
         ▼
   90 - Install Drivers ──► nvidia-driver-595 + CUDA 12.8 + DCGM 4.x
         │                  + Fabric Manager (NVSwitch/NVL nodes)
         ▼
   91 - MIG / ECC Config ─► MIG off, ECC on (via --gpu-reset)
         │
         ▼
   92 - Burn-In (opt.) ───► sustained load: power, temp, clocks,
         │                  throttle reasons, Xid, counter deltas
         ▼
   98 - GPU Inventory ────► JSON: identity, ECC, remapped rows, PCIe, NUMA
         │
         ▼
   99 - Stress Test ──────► DCGM diagnostics (level 1-4)
         │
         ▼
   device_certificate.py ─► reports/<hostname>-MAAS-validation.html
```

## Testing

The whole suite runs offline against fixtures and stub tooling — no GPU, no MAAS:

```bash
./tests/run-all.sh
```

It covers shell and Python syntax; that every script's stdout is parseable JSON
with nothing leaked ahead of it; the script `99` verdict matrix; the script `92`
burn-in matrix; the acceptance adjudication matrix; end-to-end report rendering;
and an HTML well-formedness check.

`tests/stubs/` holds fixture-driven fakes for `nvidia-smi`, `dcgmi`, `dmesg` and
`lspci`, so the commissioning scripts can be run end to end on a machine with no
GPU. `tests/fixtures/` holds captured `nvidia-smi -q` documents, synthetic DCGM
result sets, and report-level JSON generated from real script output.

The acceptance matrix (`tests/run-acceptance.py`) asserts every reject condition
**and** the inverse cases that guard against false failures — a power-capped
card, a Gen5 card in a Gen4 host, an idle link at Gen1, and a fully healthy
card must all be accepted. Those inverses matter as much as the positives: a
check that fails good hardware is worse than no check.

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

**Dual mode report generator** -- Supports both MAAS API mode (pulls data directly, recommended) and file-based mode (offline, self-contained) for environments without API access at report time.
