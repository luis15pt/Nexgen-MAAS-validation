# 90 - GPU Driver Install (`v2.1.6`)

[← Back to README](../../README.md)

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
