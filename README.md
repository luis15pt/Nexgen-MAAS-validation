# NexGen MAAS GPU Validation

Automated GPU commissioning, validation, and certification pipeline for bare-metal servers managed by [Canonical MAAS](https://maas.io/). These scripts run during MAAS commissioning to install GPU drivers, collect hardware inventory, execute stress tests, and generate HTML certification reports.

## Documentation

| | |
|---|---|
| [Commissioning scripts](docs/scripts/) | One document per script — what it collects and why each call is adjudicated the way it is |
| [Report generator](docs/report-generator.md) | `device_certificate.py`, the Certificate of Sanitisation, and hardware acceptance |
| [Error counters across the pipeline](docs/error-counters.md) | How ECC, remapped-row and PCIe replay counters are baselined and attributed to a phase |
| [Adding scripts to MAAS](docs/maas-setup.md) | Upload commands and per-script settings |
| [Testing](docs/testing.md) | Offline suite — no GPU required |
| [Troubleshooting](docs/troubleshooting.md) | Fabric Manager, fabric training, CUDA 802, BAR0 |
| [Design decisions](docs/design-decisions.md) | Why the pipeline is shaped the way it is |

## Commissioning Scripts

All of these run as MAAS commissioning scripts and output structured JSON on
stdout for the report generator to consume. The numeric prefix sets the
execution order: `80` and `89` stand alone, `99` is optional, and every GPU
script depends on `90` having run first. Cheap phases run first so a failure
costs the least — cabling (`80`) and chassis inventory (`89`) before anything
is installed, GPU inventory (`92`) before any load, then the DCGM diagnostic
(`98`), then the 1-hour burn-in (`99`) last.

| | Script | Version | What it does |
|---|---|---|---|
| 80 | [`80-nexgen-network-cabling-verify.sh`](commissioning-scripts/80-nexgen-network-cabling-verify.sh) | `v1.1.0` | LLDP neighbours vs NetBox cabling — catches a cable in the neighbour port. [Docs](docs/scripts/80-network-cabling-verify.md) |
| 89 | [`89-nexgen-hw-inventory.sh`](commissioning-scripts/89-nexgen-hw-inventory.sh) | `v1.5.0` | Chassis inventory — BMC, PSUs, drives, card serials. No BMC credentials needed. [Docs](docs/scripts/89-hw-inventory.md) |
| 90 | [`90-nexgen-gpu-install-595-13.sh`](commissioning-scripts/90-nexgen-gpu-install-595-13.sh) | `v2.1.6` | Driver + CUDA 12.8 + DCGM 4.x + Fabric Manager. [Docs](docs/scripts/90-gpu-driver-install.md) |
| 91 | [`91-nexgen-gpu-mig-ecc-config.sh`](commissioning-scripts/91-nexgen-gpu-mig-ecc-config.sh) | `v1.3.0` | Disable MIG, enable ECC. [Docs](docs/scripts/91-mig-ecc-config.md) |
| 92 | [`92-nexgen-gpu-inventory.sh`](commissioning-scripts/92-nexgen-gpu-inventory.sh) | `v2.4.0` | GPU inventory and the pre-load delivery baseline. [Docs](docs/scripts/92-gpu-inventory.md) |
| 98 | [`98-nexgen-gpu-stress-test.sh`](commissioning-scripts/98-nexgen-gpu-stress-test.sh) | `v2.6.0` | DCGM diagnostic, levels 1–4. [Docs](docs/scripts/98-gpu-stress-test.md) |
| 99 | [`99-nexgen-gpu-burn-in.sh`](commissioning-scripts/99-nexgen-gpu-burn-in.sh) | `v1.7.0` | Sustained 1-hour load, optional. [Docs](docs/scripts/99-gpu-burn-in.md) |
| 95 | [`95-nexgen-storage-sanitize.sh`](commissioning-scripts/95-nexgen-storage-sanitize.sh) | `v1.0.0` | **Decommission only.** Sanitise all media and emit the evidence. [Docs](docs/scripts/95-storage-sanitize.md) |

`95` is the exception and is **not part of the commissioning sequence**. It is the
only destructive script here, it is uploaded on its own for a decommission, and it
defaults to audit mode so that an unedited copy sitting in a commissioning set
inventories the media and erases nothing. It is documented with the others because
it shares their idioms and their JSON contract, not because it runs with them.

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

A decommission is the other direction and shares none of this pipeline:

```
Release / Commission Machine in MAAS
         │
         ▼
   95 - Storage Sanitise ──► §6 environment attestation
         │                    §7 inventory of all media by serial
         │                    §8 method per device, strongest first
         │                    §9 erase, then verify on the device's own status
         │                    §10 evidence register as JSON
         ▼
   sanitisation_certificate.py ─► reports/<hostname>-sanitisation-certificate.html
         │
         ▼
   By hand afterwards: BMC/BIOS/TPM/RAID configuration, and an independent review
                       of the evidence
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

## Reporting

```bash
pip install requests jinja2
cp .env.example .env          # MAAS URL, API key
python3 reporting/device_certificate.py --host <hostname>
```

Full options, offline file mode, the Certificate of Sanitisation and the
acceptance rules are in [docs/report-generator.md](docs/report-generator.md).

## Example Report

### ▶ [Open the rendered report](https://luis15pt.github.io/Nexgen-MAAS-validation/examples/EXAMPLE-GPU-001-MAAS-validation.html)

Served from GitHub Pages, so it displays as a page.

The file itself is [`examples/EXAMPLE-GPU-001-MAAS-validation.html`](examples/EXAMPLE-GPU-001-MAAS-validation.html)
— note that GitHub shows `.html` files as **source code**, never rendered, so
use the Pages link above to actually read the report.

Rebuild it with [`./examples/regenerate.sh`](examples/regenerate.sh).

## Folder Structure

```
Nexgen-MAAS-validation/
├── README.md
├── .env.example                             # MAAS credentials template
├── commissioning-scripts/        # MAAS commissioning scripts (run in order)
│   ├── 80-nexgen-network-cabling-verify.sh # Step 0: LLDP vs NetBox cabling
│   ├── 89-nexgen-hw-inventory.sh           # Step 1: BMC, PSUs, drives, card serials
│   ├── 90-nexgen-gpu-install-595-13.sh     # Step 2: Driver + CUDA + DCGM + Fabric Manager
│   ├── 91-nexgen-gpu-mig-ecc-config.sh     # Step 3: Disable MIG, enable ECC
│   ├── 92-nexgen-gpu-inventory.sh          # Step 4: GPU inventory & health check
│   ├── 95-nexgen-storage-sanitize.sh       # Decommission only: sanitise all media
│   ├── 98-nexgen-gpu-stress-test.sh        # Step 5: DCGM stress test
│   └── 99-nexgen-gpu-burn-in.sh            # Step 6: Sustained load (optional)
├── docs/                         # Documentation
│   ├── scripts/                            # One document per commissioning script
│   ├── report-generator.md
│   ├── error-counters.md
│   ├── maas-setup.md
│   ├── testing.md
│   ├── troubleshooting.md
│   └── design-decisions.md
├── reporting/                    # Report generation tooling
│   ├── device_certificate.py               # HTML certification report generator
│   └── sanitisation_certificate.py         # Certificate of Sanitisation (decommission)
├── tests/                        # Offline test suite (no GPU required)
│   ├── run-all.sh                          # Everything
│   ├── run-stress-verdicts.sh              # DCGM verdict matrix
│   ├── run-burnin-verdicts.sh              # Burn-in verdict matrix
│   ├── run-acceptance.py                   # Acceptance adjudication matrix
│   ├── check-python-compat.py              # Generator still runs on the oldest Python we deploy to
│   ├── stubs/                              # Fake nvidia-smi, dcgmi, dmesg, lspci, systemctl, gpu_burn, dcgmproftester13
│   ├── stubs-hw/                           # Fake ipmitool, dmidecode, smartctl, lsblk, VPD-aware lspci (script 89 only)
│   ├── stubs-decomm/                       # Fake nvme, hdparm, sg_*, lsblk -J, findmnt (script 95 only)
│   ├── stubs-raidcli/                      # Fake storcli64, on its own so its ABSENCE is testable
│   └── fixtures/                           # Captured and synthetic evidence
├── reports/                      # Generated reports (git-ignored)
└── examples/                     # Example outputs (sanitized)
    └── EXAMPLE-GPU-001-MAAS-validation.html # Sample GPU commissioning report
```
