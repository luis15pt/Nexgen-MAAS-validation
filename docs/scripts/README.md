# Commissioning Scripts

[← Back to README](../../README.md)

One document per script. Each covers what the script collects, what it depends
on, and why each reading is adjudicated the way it is.

| | Script | Version | |
|---|---|---|---|
| 80 | Network Cabling Verification | `v1.1.0` | [80-network-cabling-verify.md](80-network-cabling-verify.md) |
| 89 | Chassis & Component Inventory | `v1.5.0` | [89-hw-inventory.md](89-hw-inventory.md) |
| 90 | GPU Driver Install | `v2.1.6` | [90-gpu-driver-install.md](90-gpu-driver-install.md) |
| 91 | MIG Disable & ECC Enable | `v1.3.0` | [91-mig-ecc-config.md](91-mig-ecc-config.md) |
| 92 | GPU Inventory | `v2.4.0` | [92-gpu-inventory.md](92-gpu-inventory.md) |
| 98 | GPU Stress Test | `v2.6.0` | [98-gpu-stress-test.md](98-gpu-stress-test.md) |
| 99 | Sustained Burn-In (optional) | `v1.7.0` | [99-gpu-burn-in.md](99-gpu-burn-in.md) |
| 95 | Storage Sanitisation & Evidence | `v1.0.0` | [95-storage-sanitize.md](95-storage-sanitize.md) |

`95` is **not part of the commissioning sequence** — it is the only destructive
script here and is uploaded on its own for a decommission.

Cross-cutting: [error counters across the pipeline](../error-counters.md).
