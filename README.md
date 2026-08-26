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
│   ├── 98-nexgen-gpu-inventory.sh          # Step 2: GPU inventory & health check
│   └── 99-nexgen-gpu-stress-test.sh        # Step 3: DCGM stress test
├── reporting/                    # Report generation tooling
│   └── device_certificate.py               # HTML certification report generator
├── reports/                      # Generated reports (git-ignored)
│   └── .gitkeep
└── examples/                     # Example outputs (sanitized)
    └── EXAMPLE-GPU-001-MAAS-validation.html # Sample GPU commissioning report
```

## Commissioning Scripts

All four scripts are designed to run as MAAS commissioning scripts in sequence. They follow the MAAS metadata format and output structured JSON for downstream consumption.

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

### 98 - GPU Inventory (`v2.0.2`)

Collects detailed GPU hardware inventory via a single bulk `nvidia-smi` query:

- Serial numbers and UUIDs
- VRAM capacity and utilization
- ECC error counters
- PCIe link speed and width
- NUMA topology mapping

Outputs structured JSON. No packages installed -- depends on script 97.

**Timeout**: 5 minutes

### 99 - GPU Stress Test (`v2.1.2`)

Runs DCGM diagnostics at configurable severity levels:

| Level | Duration | Scope |
|---|---|---|
| 1 | ~1 min | Quick health check |
| 2 | ~5 min | Medium validation |
| 3 (default) | ~15 min | Standard stress test |
| 4 | ~90 min | Full burn-in validation |

Override with: `DCGM_DIAG_LEVEL=4`

**Timeout**: 2 hours

## Report Generator

`reporting/device_certificate.py` (v3.2.0) generates a consolidated HTML certification report from MAAS commissioning data.

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
  --install 97-output.json \
  --inventory 98-output.json \
  --stress 99-output.json \
  -o reports/report.html
```

The generated report includes:
- Machine hardware summary (CPU, RAM, storage, network)
- Per-GPU driver, firmware, and configuration details
- DCGM diagnostic results with pass/fail status
- DIMM inventory from lshw
- Overall validation verdict

## Example Report

**[View live example report](https://luis15pt.github.io/Nexgen-MAAS-validation/examples/EXAMPLE-GPU-001-MAAS-validation.html)**

Or see the source at [`examples/EXAMPLE-GPU-001-MAAS-validation.html`](examples/EXAMPLE-GPU-001-MAAS-validation.html).

## Adding Scripts to MAAS

Upload the commissioning scripts via the MAAS CLI. **Before uploading the
80- script**, edit its `NETBOX_URL` / `NETBOX_TOKEN` defaults (top of the
file) -- MAAS does not inject per-script secrets:

```bash
maas $PROFILE commissioning-scripts create \
<<<<<<< HEAD
  name=80-nexgen-network-cabling-verify \
  script_type=commissioning \
  hardware_type=network \
  content@=commissioning-scripts/80-nexgen-network-cabling-verify.sh

maas $PROFILE commissioning-scripts create \
  name=97-nexgen-gpu-install-580-12.8 \
=======
  name=90-nexgen-gpu-install-595-13 \
>>>>>>> ae5503c (Add Fabric Manager support and bump driver pin to 595)
  script_type=commissioning \
  hardware_type=gpu \
  content@=commissioning-scripts/90-nexgen-gpu-install-595-13.sh

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
   97 - Install Drivers ──► nvidia-driver-580 + CUDA 12.8 + DCGM 4.x
         │
         ▼
   98 - GPU Inventory ────► JSON: serials, VRAM, ECC, PCIe, NUMA
         │
         ▼
   99 - Stress Test ──────► DCGM diagnostics (level 1-4)
         │
         ▼
   device_certificate.py ─► reports/<hostname>-MAAS-validation.html
```

## Design Decisions

**Three scripts, not one** -- Splitting install/inventory/stress into separate scripts means MAAS shows granular pass/fail per phase. If the driver install fails, you see that immediately without wading through inventory output.

**Pinned driver versions** -- After hitting `nvidia-smi` field incompatibilities with driver 590 (removed `cuda_version` query field, changed `memory.type` behavior), we pinned to driver 580 + CUDA 12.8 and encoded versions in the filename.

**ASCII-only script output** -- MAAS terminal rendering mangles UTF-8 box-drawing characters and emoji. All script output uses plain ASCII formatting.

**Resilient nvidia-smi queries** -- Every field query has a fallback. If a field doesn't exist in the driver version (e.g., `retired_pages` on consumer GPUs), it degrades gracefully to "N/A" rather than crashing.

**DCGM optional** -- DCGM packages aren't always available for every driver version. The stress test detects DCGM availability and exits cleanly if absent, rather than failing the commissioning run.

**Dual mode report generator** -- Supports both MAAS API mode (pulls data directly, recommended) and file-based mode (offline, self-contained) for environments without API access at report time.
