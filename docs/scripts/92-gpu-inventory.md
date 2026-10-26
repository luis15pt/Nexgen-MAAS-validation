# 92 - GPU Inventory (`v2.4.1`)

[← Back to README](../../README.md)

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
- **NVLink topology**, read from the peer matrix: bridge count, links per bridge and bonded total (`NV12` between four pairs reads as 4 bridges of 12 = 48 links). Not from `nvlink --status`, which enumerates *ports* whether connected or not and reported 139 "active links" on a machine whose real topology is 48
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
