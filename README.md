# NexGen MAAS GPU Validation

Automated GPU commissioning, validation, and certification pipeline for bare-metal servers managed by [Canonical MAAS](https://maas.io/). These scripts run during MAAS commissioning to install GPU drivers, collect hardware inventory, execute stress tests, and generate HTML certification reports.

## Folder Structure

```
Nexgen-MAAS-validation/
├── README.md
├── .gitignore
├── .env.example                             # MAAS credentials template
├── commissioning-scripts/        # MAAS commissioning scripts (run in order)
│   ├── 97-nexgen-gpu-install-580-12.8.sh   # Step 1: Driver + CUDA + DCGM install
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

All three scripts are designed to run as MAAS commissioning scripts in sequence. They follow the MAAS metadata format and output structured JSON for downstream consumption.

### 97 - GPU Driver Install (`v2.1.1`)

Installs the full NVIDIA GPU software stack:

- **Driver**: `nvidia-driver-580-server-open`
- **CUDA Toolkit**: `cuda-toolkit-12-8`
- **DCGM**: `datacenter-gpu-manager-4` (CUDA 13)

Enables persistence mode, loads kernel modules, and starts the DCGM service.

| Env Override | Default | Description |
|---|---|---|
| `NVIDIA_DRIVER` | `nvidia-driver-580-server-open` | Driver package name |
| `CUDA_TOOLKIT` | `cuda-toolkit-12-8` | CUDA toolkit package |
| `DCGM_CUDA_MAJOR` | `13` | DCGM CUDA major version |

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

Upload the commissioning scripts via the MAAS CLI:

```bash
maas $PROFILE commissioning-scripts create \
  name=97-nexgen-gpu-install-580-12.8 \
  script_type=commissioning \
  hardware_type=gpu \
  content@=commissioning-scripts/97-nexgen-gpu-install-580-12.8.sh

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
