# Design Decisions

[← Back to README](../README.md)

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

**A Confidential Computing switch voids the run instead of rebooting** -- A GPU left in CC mode by a previous tenant keeps it across reboots and reimages, and the host driver refuses it. Script `90` switches it off with gpu-admin-tools before `modprobe`, because the tool needs the GPU unbound. NVIDIA then requires a full host reboot, which a commissioning script cannot do: the ephemeral image loses the driver it just installed and `maas_run_remote_scripts.py` has no resume logic. So the run is treated like an ECC enable in `91` -- `FAIL`, `revalidation_required`, halt marker, action-required banner -- and the second commissioning run is the clean boot. One extra run on the affected nodes, once, against evidence that would otherwise have been collected on a GPU NVIDIA says may hold stale state.
