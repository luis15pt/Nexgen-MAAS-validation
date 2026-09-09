# NexGen MAAS GPU Validation

Automated GPU commissioning, validation, and certification pipeline for bare-metal servers managed by [Canonical MAAS](https://maas.io/). These scripts run during MAAS commissioning to install GPU drivers, collect hardware inventory, execute stress tests, and generate HTML certification reports.

## Folder Structure

```
Nexgen-MAAS-validation/
├── README.md
├── .gitignore
├── .env.example                             # MAAS credentials template
├── commissioning-scripts/        # MAAS commissioning scripts (run in order)
│   ├── 89-nexgen-hw-inventory.sh           # Step 0: BMC, PSUs, drives, card serials
│   ├── 90-nexgen-gpu-install-595-13.sh     # Step 1: Driver + CUDA + DCGM + Fabric Manager
│   ├── 91-nexgen-gpu-mig-ecc-config.sh     # Step 2: Disable MIG, enable ECC
│   ├── 92-nexgen-gpu-inventory.sh          # Step 3: GPU inventory & health check
│   ├── 95-nexgen-storage-sanitize.sh       # Decommission only: sanitise all media
│   ├── 98-nexgen-gpu-stress-test.sh        # Step 4: DCGM stress test
│   └── 99-nexgen-gpu-burn-in.sh            # Step 5: Sustained load (optional)
├── reporting/                    # Report generation tooling
│   ├── device_certificate.py               # HTML certification report generator
│   └── sanitisation_certificate.py         # Certificate of Sanitisation (decommission)
├── tests/                        # Offline test suite (no GPU required)
│   ├── run-all.sh                          # Everything
│   ├── run-stress-verdicts.sh              # DCGM verdict matrix
│   ├── run-burnin-verdicts.sh              # Burn-in verdict matrix
│   ├── run-acceptance.py                   # Acceptance adjudication matrix
│   ├── validate-maas-metadata.py           # Metadata block is internally consistent
│   ├── check-python-compat.py              # Generator still runs on the oldest Python we deploy to
│   ├── stubs/                              # Fake nvidia-smi, dcgmi, dmesg, lspci, systemctl, gpu_burn, dcgmproftester13
│   ├── stubs-hw/                           # Fake ipmitool, dmidecode, smartctl, lsblk, VPD-aware lspci (script 89 only)
│   ├── stubs-decomm/                       # Fake nvme, hdparm, sg_*, lsblk -J, findmnt (script 95 only)
│   ├── stubs-raidcli/                      # Fake storcli64, on its own so its ABSENCE is testable
│   └── fixtures/                           # Captured and synthetic evidence
├── reports/                      # Generated reports (git-ignored)
│   └── .gitkeep
└── examples/                     # Example outputs (sanitized)
    └── EXAMPLE-GPU-001-MAAS-validation.html # Sample GPU commissioning report
```

## Commissioning Scripts

All of these run as MAAS commissioning scripts and output structured JSON on stdout for the report generator to consume. The numeric prefix sets the execution order: `89` stands alone, `99` is optional, and every GPU script depends on `90` having run first. Cheap phases run first so a failure costs the least — chassis inventory (`89`) and GPU inventory (`92`) before any load, then the DCGM diagnostic (`98`), then the 1-hour burn-in (`99`) last.

`95` is the exception and is **not part of the commissioning sequence**. It is the
only destructive script here, it is uploaded on its own for a decommission, and it
defaults to audit mode so that an unedited copy sitting in a commissioning set
inventories the media and erases nothing. It is documented with the others because
it shares their idioms and their JSON contract, not because it runs with them.

### 89 - Chassis & Component Inventory (`v1.5.0`)

Collects the chassis-level facts the GPU scripts do not, for the parts of a
Server Report that describe the machine rather than its accelerators. Runs
**first**: it has no GPU dependency, so a run that never reaches the driver
install still yields a component inventory.

- **BMC firmware revision**, plus manufacturer, product and IPMI version
- **Board manufacture date** and FRU serials, which SMBIOS does not carry
- **Power-on hours**, where the BMC exposes a sensor for them, plus the SEL span
- Whether the **`nvbandwidth` binary is present** — a corroborating check only;
  see below for why a skip on these GPUs is by design rather than a missing tool
- **PSU inventory** from SMBIOS type 39 — make, model, serial, rating, per bay,
  cross-checked against the BMC's own per-bay sensors
- **Every drive**, including the ones **behind a hardware RAID controller**, with
  their power-on hours rolled up into a single server figure
- **Add-in card serial numbers**, from PCI VPD

#### No BMC credentials are needed, and Redfish is not used

The BMC is read **in band**, over the host-side KCS interface at `/dev/ipmi0`,
not over the network. There is no username, password or Redfish session
involved: the authorisation is being root on the machine the BMC is attached
to. This is the same path MAAS's own `30-maas-01-bmc-config` takes, which is
how MAAS configures the BMC on a node it has no credentials for in the first
place — and why that script logs `IPMI detected!` before any credential exists.

Redfish would need an account and a route to the BMC's network interface.
Neither is required here.

#### Drives behind a RAID controller

A hardware RAID volume presents one SCSI device and hides its members, so the
make, model, serial and power-on hours of the drives actually being sold are
invisible to `lsblk`, `lshw` and MAAS alike — the volume's own controller model
is all any of them report. `smartctl -d megaraid,N` reads the members directly.
Slots are probed rather than enumerated (`MEGARAID_MAX_DID`, default 32) because
nothing readable lists them. This avoids needing `storcli`, which is not in the
Ubuntu archive and would have to be fetched from Broadcom mid-commissioning.

The members answer with a **non-zero exit status** — the MegaRAID cannot pass the
SMART status read through (*"ATA return descriptor not supported by controller
firmware"*) — while returning model, serial, firmware and power-on hours
perfectly well. Treating that exit code as failure would discard the only record
of the drives actually being sold, so the script judges the response by whether
it carries an identity, not by the exit status.

**This is also where the server's power-on hours come from**, and the highest
drive is the figure to use. On a measured node
the NVMe reported 18,318 hours and both RAID members 18,317 — a spread of one
hour across three drives, which is what shows they were fitted from new and that
the figure is the chassis's own rather than a replacement part's. Set against the
SEL's 809 elapsed days, 763 powered days puts the machine at 94% powered for its
life, so the two independent records corroborate. `power_on` therefore carries
`drive_hours_max`, `drive_hours_min`, `drives_reporting_hours` and
`drive_hours_spread`: the Server Report cell can be filled from one field, with
the spread visible behind it.

Why the **highest**, and not the controller: an internal drive only accumulates
hours while the server is powered, so every drive's count is at or below the
server's own and `max()` is the tightest lower bound available. A *replacement*
drive reads low and cannot drag the figure down. The RAID controller is not a
candidate — it reports no lifetime hours at all, and it is a replaceable part.

The failure mode is a drive **moved in from another server**, which reads high
and would inflate the figure. Two guards catch it, both adjudicated in the report
generator rather than the script so that a report built from already-stored
commissioning data reads correctly without re-running anything:

- **Powered time cannot exceed elapsed time.** If the highest drive reports more
  hours than the chassis has existed per its event log, the report refuses the
  figure and says so — either that drive came from elsewhere or the SEL was
  cleared, and nothing readable from the host can tell those apart.
- **Drives that disagree were not all fitted at build time.** A spread beyond a
  day is flagged, and the figure is stated as what it then is: a lower bound.

#### Two sources for the PSU count, because one of them lies

SMBIOS type 39 is written by the BIOS, and on this fleet it has been seen to
**under-report**: a chassis with four PSUs fitted, physically confirmed, recorded
only two type-39 entries. A DMI-only count would have understated what is being
sold, in a document whose accuracy is warranted under Clause 10.1(i).

The BMC exposes a sensor pair per bay regardless of what the BIOS wrote, so the
script counts distinct `PSU<n>` sensors in the SDR as an independent check. Where
the two disagree the **BMC count is the presence evidence** and DMI remains the
only source of serials — so the report states the true bay count and names how
many serials have to come off the physical labels instead. Presence and identity
are different claims and are evidenced separately.

#### Card serial numbers

DMI carries no add-in card serials, which is why `lshw` reports a NIC's "serial"
as its MAC address and an HBA's as nothing at all. The cards carry Vital Product
Data with the board serial and part number, and `lspci -vv` reads it as root. A
card that publishes no VPD is omitted rather than logged blank.

How much this yields is per-card, and was measured rather than assumed: on these
nodes the **ConnectX-7 publishes full VPD** (part number `MCX755106AS-HEAT`, its
board serial, and a descriptive product name), while the **MegaRAID 9560-16i and
the Intel X710 publish none at all** -- their serials remain a physical label
check. A dual-port adapter answers with identical VPD on every PCI function, so
records are collapsed on board serial: one card, one row, keeping the functions
it answered on.

#### Everything is best-effort, and says which

No field here fails a commissioning run: this is inventory metadata, and a
missing BMC sensor is not a hardware fault. Absent values are `null`, never `""`
— a blank serial in a Server Report reads as "this part has no serial", which is
a statement of fact the script cannot make. The verdict degrades to `WARN` and
names what it could not collect.

Chassis power-on hours in particular are **not** universally available: there is
no standard IPMI sensor for them, and the OEM raw commands that read them differ
per vendor. **Measured on the ASUS ASMB11 in these ESC8000A-E12P nodes, no such
sensor exists** -- `sdr elist full` returns temperature, voltage, fan and
per-PSU wattage and nothing else. The script reports the sensor where one exists
and otherwise says so plainly, supplying `sel_span_days` -- the elapsed time
between the first System Event Log entry and the BMC clock, reported in its own
field because it is elapsed, not powered, time -- and leaving the per-drive hours — which Schedule 4 paragraph 1(f) asks for anyway —
as the evidenced figure.

| Env Override | Default | Description |
|---|---|---|
| `REQUIRED_PKGS` | `ipmitool smartmontools nvme-cli dmidecode pciutils jq` | Packages installed up front |
| `MEGARAID_MAX_DID` | `32` | RAID device slots probed per controller |

**Timeout**: 10 minutes

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

### 95 - Storage Sanitisation & Evidence (`v1.0.0`)

Sanitises every storage device in a host that is leaving NexGen custody, and
produces the serial-number-level evidence that it did. Implements **sections 6
to 10** of the *Remove Host for Sale — Secure Decommissioning Procedure*: the
part of the runbook that happens on the host, in a maintenance environment,
which is exactly where these scripts already run. Its job is to wipe the disks
and to evidence that the data is gone; clearing non-disk configuration (BMC,
BIOS/UEFI, TPM, RAID) is not covered, and the script says so in its own output
rather than staying silent about it.

- **Attests to the maintenance environment** (§6) and refuses to erase from an OS
  installed on a drive being sanitised, or with a target mounted
- **Inventories all media by serial** (§7) — NVMe, SATA, SAS, USB, SD, and the
  drives a RAID controller hides — and stops if the counts disagree
- **Selects a purge-capable method per device** (§8), strongest first, recording the
  NIST level reached and the reason as well as the choice
- **Executes and verifies** (§9), adjudicating on the device's own numeric status
- **Emits the whole §10 evidence register** as JSON: serial, WWN, firmware, method,
  NIST level, exact command, UTC start and finish, result, verification
- **Names the remediation** for anything short of a Purge, with commands — never a
  proposal to destroy a drive

**It defaults to audit mode and destroys nothing.** MAAS injects no per-script
environment, so the confirmation is a constant in the file:

```bash
SANITIZE_CONFIRM="${SANITIZE_CONFIRM:-CHANGEME}"   # must read: SANITIZE ALL MEDIA
EXPECT_SYSTEM_SERIAL="${EXPECT_SYSTEM_SERIAL:-}"   # optional target pin
```

Audit mode still does the full inventory and the full capability audit, and prints
the method it *would* use per device. **Run it that way first**: on a host with
media that cannot be purged in band, the audit says so before anything has been
destroyed. A machine in Commissioning or Released is pre-deployment, so an erase
there is the expected outcome rather than an accident — the gate exists to stop
the script running destructively in a context nobody chose, not to second-guess a
decommission that was.

#### Completion is a number, not a sentence

The runbook's own §9 script decides success by grepping the sanitise log for
`success`, and warns in the same breath that *"the wording returned by
`nvme sanitize-log` can vary between `nvme-cli` versions and drive vendors."* That
is a verdict resting on vendor prose.

This script reads **SSTAT** instead — bits 2:0 of the NVMe sanitise status: `1`
completed successfully, `2` in progress, `3` completed with failure. It prefers
`nvme sanitize-log -o json` and falls back to parsing the hex out of the text
form, recording in the evidence which it had to use. ATA status comes from
`hdparm --sanitize-status`, mapped explicitly, so a state the script does not
model is never read as success. The human-readable log is kept alongside as
evidence and is never adjudicated on. A drive whose log says *"the most recent
sanitize operation completed successfully"* while its status field reports failure
is a **failure**, and the test suite pins that case.

Two more departures from the runbook's script, both for the same reason — §14
requires every drive to carry either a successful sanitisation record or a
destruction certificate:

- Where it `exit 3`s on the first failure, this **continues the sweep**. Aborting
  guarantees that some drives end up with no record at all.
- Its evidence goes to `/root/nvme-sanitize-evidence`, which **does not survive an
  ephemeral boot** — that path is RAM in a MAAS maintenance environment. Here the
  audit log is also embedded in the JSON, which MAAS stores, so the copy that
  matters outlives the environment that produced it.

#### NIST Clear and NIST Purge are different claims

Levels are **NIST SP 800-88r1**, and every device records which one it reached:

- **Clear** — logical techniques over all *user-addressable* locations, resisting
  simple non-invasive recovery.
- **Purge** — techniques that render recovery infeasible *using state-of-the-art
  laboratory techniques*.

On flash the distinction is not academic. An overwrite reaches only
user-addressable blocks, so over-provisioned capacity, retired blocks and
unmapped FTL copies fall outside it — NIST warns that a device may report a
successful overwrite while data remains in reallocated blocks. A **Purge** on
flash is a firmware operation: ATA or NVMe SANITIZE, or a cryptographic erase,
because the drive controller reaches every physical block and a crypto erase
discards the media key so all ciphertext dies at once.

Which level is *required* depends on whether the media leaves organisational
control. Clear is aimed at media reused inside the organisation; a host being
sold leaves. So **Purge is the required level** — `required_level: "purge"` in
every report — and an overwrite is recorded as `clear`, never as `purge`. It is
off by default (`ALLOW_OVERWRITE_FALLBACK`), and even when a Clear completes and
verifies, the verdict still fails unless `ACCEPT_NIST_CLEAR=true` says the level
was accepted deliberately, in which case it degrades to a warning that names the
residual risk. A success at `clear` and a success at `purge` are different
claims, and collapsing them is the confusion this script exists to prevent.

> The Confluence runbook uses the word *purge-capable* without defining it or
> citing NIST, and its section 10 table lists "approved overwrite" as an
> acceptable method with no statement of when. On flash that is the loophole.
> Worth closing in the doc.

#### Drive capability is not controller capability

Measured on the MR9560-16i in these nodes: both SATA members report
`Sanitize Support = CryptoErase, BlockErase` and `Cryptographic Erase Capable =
Yes`, while the controller reports `Support Sanitize = No`, `Support Extended
Sanitize = No` and `Allow Ctrl Encryption = No`. The drives can be purged; this
controller cannot issue the command. Planning off the drive's word alone gets
`start sanitize` rejected at the controller, so the controller has the veto —
and `secureerase` is not offered either, because an instant secure erase needs
drive security and these are SEDs that were never locked (`Secured = No`) to a
controller that cannot manage encryption.

What remains is `Support JBOD = Yes`: exposed as its own SCSI target, the drive
can be addressed directly and `hdparm --sanitize-crypto-scramble` reaches it.
Whether ATA SANITIZE survives the SCSI-to-ATA translation the controller applies
is firmware-dependent, so the script establishes it rather than assuming:

Confirmed on the hardware: exposed in JBOD, both members report
`CRYPTO_SCRAMBLE_EXT` and `BLOCK_ERASE_EXT`, so ATA SANITIZE does survive the
translation. The erase is therefore addressed to the drive itself inside a JBOD
window the script opens and closes — `ata-sanitize-via-jbod`, a NIST Purge with
no cables moved and the drives kept.

Two details from that run shape the implementation. The controller-level
`set jbod=on`/`off` returned **exit 255** — unsupported on this controller and
not needed, because the per-drive call exposes the drive anyway, so its failure
is recorded rather than treated as fatal. And the first member came back as
**`/dev/sda`**, the name the deleted volume had: device names are recycled the
moment the volume goes, so the node is resolved **by serial at execution time**,
never by the name recorded during the inventory.

**The JBOD probe.** With `ALLOW_RAID_RECONFIG=true` and `SANITIZE_CONFIRM` left
at its default, the run captures the array geometry, deletes the volume, sets
the controller and drives to JBOD, matches each drive by serial to the device it
is now exposed as, reads `hdparm -I` and `sg_opcodes`, then returns the drives to
Unconfigured Good, turns JBOD off and recreates the array. It issues no erase,
and it restores the array unconditionally because nothing was wiped. The
conclusion says whether a purge is reachable and which command would do it.

#### Nothing here proposes destroying a drive

The runbook's section 8 says media that cannot be sanitised "must be removed from
the host, quarantined and physically destroyed". That is the wrong answer here,
and usually the wrong diagnosis: a drive that resists sanitisation from inside a
RAID volume is a **reachability** problem, not an unpurgeable drive. The SATA
SSDs in these chassis implement the ATA Security feature set — measured, not
assumed, because IDENTIFY DEVICE traverses the `megaraid` passthrough even though
SANITIZE does not, so `smartctl -j` reports `ata_security` for a drive the host
cannot otherwise address.

So instead of a quarantine list the report carries a **remediation register**:
per drive, the NIST level actually reached, what blocked a Purge, whether the
drive reports ATA Security, and the routes to a Purge in order, with commands.
For a drive behind a MegaRAID that is:

1. **Delete the array, purge the members, rebuild the array.** A controller
   sanitise needs the drive *Unconfigured Good*, so there is no way to purge the
   members with the volume in place — but the array configuration is not data.
   It is captured first and rebuilt afterwards, so the buyer receives the same
   RAID 1 with nothing on it:

   ```
   storcli64 /c0/vall show all                 # record the geometry -- deletion destroys it
   storcli64 /c0/v239 del force                # not a wipe: removes the mapping only
   storcli64 /c0/eall/sall show                # both drives must read UGood
   storcli64 /c0/eX/sY start sanitize blockErase ause
   storcli64 /c0/eX/sY show sanitize           # poll to 100%
   storcli64 /c0 add vd type=r1 drives=252:0,252:1 name=RAID
   ```

   `show sanitize` reports completion as a percentage, which is machine-readable
   evidence. Pass `ause` or a failed sanitize leaves the drive *Unconfigured Bad*.
   `cryptoerase` is stronger but Broadcom documents it as **not allowed on FDE and
   ISE drives** — read `Sanitize Support` first. The **VD id is read from the
   controller, never assumed**: on these nodes it is `239`, not `0`, because a
   volume configured through the UEFI utility does not number from zero
2. `storcli /c0/eX/sY secureerase force` — a cryptographic erase, **on an SED
   only**. Not to be confused with `start erase simple|normal|thorough`, which is
   controller-driven pattern writing: on flash it leaves over-provisioned and
   remapped NAND untouched, making it a Clear
3. Set the drives to **JBOD**, then `hdparm --sanitize-crypto-scramble` — the two
   preparations are not interchangeable: a controller-issued erase wants the drive
   **Unconfigured Good** and is documented as unsupported on JBOD, while the host
   can only address it in **JBOD**. Deleting the virtual disk is a prerequisite for
   both, and is not itself a wipe — it removes the mapping and leaves the data
4. Move the drive to a **direct SATA port** and erase it there — no vendor tooling
   and no firmware assumptions
5. A NIST **Clear** by overwrite, last, labelled as the weaker claim it is

Every route keeps the drive. A read-only `controller_survey` section records what
is blocking the hidden drives and, where a vendor CLI would be needed to ask,
names the read-only commands that would answer it — so the gap is stated rather
than guessed at.

#### Why `storcli` is probed for and never fetched

`sg3-utils` is in the Ubuntu archive; `storcli64` is not — `apt install storcli64`
returns *"Unable to locate package"*, because it is a Broadcom download rather than
a package. It is free and needs no login: `docs.broadcom.com/doc/1232745587`
("Latest Storcli for all OS") ships a native `.deb` whose only payload is
`/opt/MegaRAID/storcli/storcli64`, and which puts **nothing on `PATH`** — hence
`RAID_CLI` and the `/opt` probe. Take the **7.x / `007.xxxx`** branch: StorCLI2
(the 8.x tool) does not list the 9560, and the legacy 1.x branch will not drive it, and `megacli` is not a substitute on a tri-mode 95xx controller. Pulling
a vendor binary mid-commissioning is a dependency this script will not take on — the same
reasoning that kept it out of script `89`. So the SCSI route is tried **first**: if
a RAID member answers on its own `/dev/sg*` node, matched to it by serial, a purge
can be issued with tools that are already there. If an operator has staged a RAID
CLI, the script finds it — on `PATH`, at the vendor install paths under `/opt`
(which is where the Broadcom `.deb` lands, putting nothing on `PATH`, so a probe
built on `command -v` alone would report it missing), or wherever `RAID_CLI`
points — resolves the serial to an enclosure and slot, and uses it. If neither works the gap is **stated**, not papered over.

On the ESC8000A-E12P nodes this is the case that bites, and it was confirmed on a
real run: the NVMe purges cleanly by crypto erase, while the **two SATA
boot SSDs behind the MegaRAID** have no purge path this environment can address —
SANITIZE does not pass through the controller and no vendor CLI is staged. Both
drives nonetheless report the ATA Security feature set through the `megaraid`
passthrough, so the drives are not the problem; reaching them is. The run fails,
names both serials in the remediation register, and lists the routes to a Purge
in order. Nothing is destroyed, and the point of running an audit first is to
learn this before the machine is half-erased.

#### A process note is not a finding about the media

The verdict answers one question: was the data destroyed. Two things that say
nothing about that were degrading it — an unset `EXPECT_SYSTEM_SERIAL` and an
`nvme-cli` too old to emit a JSON sanitise log. A run that purged and verified
every drive came back `WARN`, which reads as a hedge on the erase rather than
what it was: a note about how the run was configured.

Those are recorded with severity `advisory`. They appear in the evidence and in
the certificate's findings, and they do not move the verdict. `warning` and
`critical` remain reserved for statements about the media.

#### Verification is identity, and says which

Three separate claims per device, kept separate:

- The **serial re-read** is the proof of identity — that this evidence belongs to
  the device that was erased. A serial that no longer matches downgrades a
  successful erase to `unverified`, because an evidence register attributing work
  to the wrong drive is worse than none.
- The device's own **status field** is the proof of the purge.
- The absence of filesystem signatures and the first/last mebibyte hashes are
  **corroborating only**. A cryptographic erase leaves random-looking data by
  design, so non-zero blocks are not a failure and zeroed blocks are not a proof.
  The record says so in as many words.

| Env Override | Default | Description |
|---|---|---|
| `SANITIZE_CONFIRM` | `CHANGEME` | Must read `SANITIZE ALL MEDIA` for anything to be erased |
| `EXPECT_SYSTEM_SERIAL` | *(unset)* | When set, must match the DMI system serial or the run refuses |
| `ALLOW_OVERWRITE_FALLBACK` | `false` | Permit a full-device overwrite — a NIST Clear — where no Purge can be reached |
| `ACCEPT_NIST_CLEAR` | `false` | Accept a verified Clear as sufficient. Off, because the host leaves organisational control |
| `SANITIZE_POLL_INTERVAL` | `30` | Seconds between status reads (clamped to ≥1) |
| `SANITIZE_MAX_WAIT` | `3600` | Per-device bound; past it the device is recorded as `timeout` |
| `OVERWRITE_MAX_SECONDS` | `7200` | Bound on the overwrite fallback |
| `ALLOW_RAID_RECONFIG` | `false` | Permit deleting the virtual disk to reach the drives inside it. Its own gate: consenting to wipe media is not consenting to rebuild an array. Set on its own, with `SANITIZE_CONFIRM` left alone, it runs the **JBOD probe** instead — reconfigure, read what the exposed drives accept, restore the array, erase nothing |
| `RAID_RESTORE_ARRAY` | `true` | Recreate the array afterwards from the geometry captured before deletion, so the buyer gets the same RAID 1 empty |
| `INSTALL_STORCLI` | `true` | Fetch storcli when a RAID controller needs one and the image has none, verified against `STORCLI_SHA256` first. A mismatch refuses to install |
| `STORCLI_SHA256` | pinned | The expected SHA-256 of the Broadcom `.deb`. Update it deliberately when the upstream package changes |
| `STORCLI_ZIP_URL` / `STORCLI_DEB_URL` | Broadcom / *(unset)* | Where to fetch it. Point `STORCLI_DEB_URL` at an internal mirror in production rather than reaching Broadcom from the commissioning VLAN |
| `STORCLI_INSTALL_PATH` | `/opt/MegaRAID/storcli/storcli64` | Where the package lands, checked after install |
| `STORCLI_FETCH_TIMEOUT` | `180` | Seconds allowed for the download |
| `RAID_CLI` | *(unset)* | Path to a staged `storcli64`/`perccli64`. Also probed on `PATH` and at the vendor install paths under `/opt`, which is where the Broadcom package lands and which `command -v` alone would miss |
| `MEGARAID_MAX_DID` | `32` | RAID device slots probed per controller |
| `JIRA_TICKET` / `ENGINEER` | *(unset)* | Recorded into the attestation; the reviewer is never filled in by the script |
| `LOG_DIR` | `/root/nexgen-sanitize-evidence` | Operator copy of the audit log. Does not survive an ephemeral boot; the copy that lasts is in the JSON |
| `ARTIFACT_MAX_BYTES` | `131072` | Cap on each embedded evidence blob |
| `REQUIRED_PKGS` | `nvme-cli smartmontools sg3-utils hdparm util-linux pciutils coreutils jq` | Packages installed up front |

**Timeout**: 4 hours declared — and remember MAAS does not enforce it. The real
bounds are `SANITIZE_MAX_WAIT` and `OVERWRITE_MAX_SECONDS` in the script. **Raise
the MAAS node timeout past the expected erase duration before running**, for the
same reason the burn-in needs it: past the 30-minute default MAAS marks the machine
Failed mid-erase and the evidence goes with the run.

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

#### Why `nvbandwidth` skips on H100 PCIe, and what that costs

DCGM reports `nvbandwidth` as **Skip** on every H100 PCIe with an empty `info`
field and no warning, which is easy to misread. Established from DCGM 4.6.1's
source and the SKU table compiled into its `nvvs` binary:

- the plugin has exactly one Skip path, `is_allowed` false;
- that parameter **defaults to false** and is enabled per-SKU;
- the table's entry for PCI device **`0x2331` (H100 PCIe)** has no `nvbandwidth`
  key at all, while `0x2330` (H100 SXM5) does.

So it skips on this SKU **whether or not NVLink bridges are fitted** — NVIDIA
has not qualified that plugin for the PCIe part. The empty `info` is because the
skip message goes through a log-only call that never reaches the JSON. A
*missing binary* would be FAIL (`DCGM_FR_INTERNAL`), not Skip — and the binary
is not missing: the DCGM packages ship it at
`/usr/libexec/datacenter-gpu-manager-4/plugins/cuda13/nvbandwidth`.

An earlier version of this rule keyed off NVLink presence instead. It gave the
right answer on a bridgeless host for the wrong reason and the **wrong** answer
on a bridged one, putting all eight GPUs of a machine with four working bridges
into review over a skip that was by design.

**What it costs:** on a machine sold *with* bridges, the NVLink fabric goes
unmeasured. The report says the skip is expected, which is an explanation and
not evidence. Peer bandwidth needs the standalone tool run directly —
`nvbandwidth -t device_to_device_memcpy_write_ce --format json` yields a
per-pair GB/s matrix in which the bridged pairs separate from the PCIe-only
ones. Forcing the DCGM plugin with `-p nvbandwidth.is_allowed=true` is *not*
the answer: it runs a path NVIDIA has not qualified for this SKU.

Fabric Manager is **not** required for bridges — it configures NVSwitch memory
fabrics, and there is no NVSwitch on a PCIe card. The bridges train with the
driver alone, which the `NV12` entries in the peer matrix confirm.

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
| `BURN_SAMPLE_INTERVAL` | `10` | Seconds between GPU telemetry samples |
| `CHASSIS_SAMPLE_INTERVAL` | `30` | Seconds between chassis sensor samples (fans, inlet air, PSU draw). `0` disables |
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

#### Chassis telemetry: why a hot GPU is not yet a finding

GPU telemetry says a card got hot. It cannot say **why**, and the three
explanations lead to three different conversations with a buyer:

- a fan turning slower than its siblings is a **serviceable fault**, not a bad GPU;
- a warm inlet is the **data centre's** condition, not the seller's defect —
  Schedule 2 specifies a 10–35 °C operating range;
- an even chassis with one hot card is the **card or its seating**.

None of that is recoverable once the run is over, so scripts `89`, `98` and `99`
record the chassis sensors alongside the GPU ones: every fan's RPM, inlet air
temperature, board thermal zones and per-PSU draw. `89` takes an **idle
baseline** before any load (free — it has already read the SDR for the
power-on-hours lookup), and `98` and `99` sample **under load**. Comparing the
two is what separates "this fan never spins up" from "this fan is dead".

**What the fan check is for: confirming every fan is turning, and that it
responds to load.** Absolute RPM on its own answers neither, because the reading
depends on the BMC's fan-control mode — 9,880 RPM might be full throttle or an
energy-saving curve, and nothing readable from the host says which. So the
primitives are:

- `stopped` — any fan reporting 0 RPM. Unambiguous, and mode-independent.
- `all_spinning` — the roll-up of that.
- the **idle-to-load rise**, from script `89`'s idle baseline against `98` and
  `99` under load. A fan that does not speed up when the GPUs heat is either
  stuck at fixed duty or not responding, whatever curve the BMC is running.

Spread is reported too, but **per bank** and as secondary information. On a live
ESC8000A-E12P the GPU bank sits around 9,880 RPM and the system bank around
8,300 — different fans on different duty curves — so pooling them shows a ~21%
spread with nothing wrong. `worst_spread_pct` and `slowest_sensor` name the
specific fan when one bank does drift apart.

The first version of this metric pooled the banks, and the stub had both banks
at nearly the same RPM, which hid the flaw until real `ipmitool` output was
compared against it. The stub now carries measured values.

These are sampled on their **own slower cadence** (`CHASSIS_SAMPLE_INTERVAL`,
default 30 s) rather than in the GPU loop. The BMC is read over the in-band KCS
interface, which serialises requests and is far slower than a local `nvidia-smi`
call, and fan RPM and ambient air move on a timescale of minutes rather than
seconds. Polling it every few seconds for an hour would load the BMC for no
extra information.

Chassis telemetry never gates a run: an unresponsive BMC yields `chassis: null`
and the load verdict is unaffected. Inventory data must not be able to fail a
one-hour test.

Both samplers run with **stdout detached** (`) >/dev/null 2>&1 &`), and that is
load-bearing rather than tidiness. A backgrounded subshell inherits the script's
stdout; killing the subshell orphans the `sleep` it spawned, which keeps that
descriptor open. Anything reading the script's output — MAAS capturing the
result, or a command substitution — then blocks until every writer closes, so a
finished run appears to hang for a full sample interval. The samples reach their
file by explicit redirect, so detaching stdout costs nothing. This was latent in
the original GPU sampler at its 10 s interval and only became obvious at the
chassis sampler's 30 s: the offline suite went from minutes to nineteen of them.

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
  --hwinv     89-output.json \
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
- **Chassis and components** from script `89` &mdash; BMC firmware, FRU board
  manufacture date, power-on hours, every PSU bay, every drive including the
  ones behind the RAID controller, and add-in card serials from PCI VPD
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

### Certificate of Sanitisation

`reporting/sanitisation_certificate.py` (v1.0.0) is a **separate document**, from
script `95`'s output alone:

```bash
python3 reporting/sanitisation_certificate.py --sanitize 95-output.json \
  -o reports/EXAMPLE-NODE-01-sanitisation-certificate.html

# or straight from MAAS
python3 reporting/sanitisation_certificate.py --host EXAMPLE-NODE-01
```

Separate, because the subject is the media rather than the accelerators, and the
question it answers is *was the data on this machine destroyed*. `REQUIRED_STAGES`
and `evaluate_gpu_acceptance` have nothing to say about that. It reuses
`device_certificate.py` by import rather than forking it — the raw-log embedding,
the base64 decode, the file loader and the MAAS client all come from there — and
is held to the same Python 3.8 floor by `tests/check-python-compat.py`.

It opens with the answer and nothing else: host, chassis serial, date, a verdict
badge, one sentence, and **one table** of what was destroyed — a row per physical
device carrying serial and WWN, firmware, capacity, method with its NIST level,
the exact command, UTC start and completion, result and how it was verified.

Everything else is behind two expanders: **Technical detail** (the Clear/Purge
explanation, the environment attestation, inventory counts, per-device method
reasoning, the controller survey and the findings) and **Raw output**. A drive
still short of a Purge gets an *Outstanding* section, which is absent when there
is nothing outstanding.

**The badge is derived from the evidence, not read from the verdict.** It reads
`PASS` when every physical device reached Purge and was verified, `FAIL` when
not, and `N/A` in audit mode. An earlier version took the script's own verdict
and so displayed `WARN` beside a sentence saying all three drives were purged —
a document that contradicts itself is worse than either half alone.

There is no sign-off block. Section 14 of the runbook wants an independent
reviewer, but that is a release-approval step recorded in the ticket; this
document's job is to show what was destroyed, and a checklist of things for
someone to tick is a different artefact.

Like the commissioning certificate, it links nowhere: the audit log and every
device's raw command output are embedded, which is also the only way the log
survives, since the `/root` it was written to is RAM in an ephemeral boot.

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
  and reports `sw_power_cap`. That is the limiter working, not a defect, and it
  is reported rather than penalised. Thermal slowdown
  (`hw_thermal_slowdown`, `sw_thermal_slowdown`) is graded **WARN → REVIEW, not
  REJECT**: it evidences that clocks dropped under load, not that the card is
  faulty — the DCGM diagnostic passes and gpu-burn returns OK with no
  computation errors — and throttling is not one of the Schedule 4 paragraph 4
  health criteria. It is recorded as a measured observation and disclosed under
  limb 1(g). The detail deliberately states no cause: "restricted airflow" or
  "poor seating" is inference, and the column it feeds is warranted under
  Clause 10.1(i). `hw_power_brake_slowdown` stays a **FAIL** — the brake is
  asserted by power delivery rather than temperature, so it is a different fault
  and no airflow work addresses it.
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
  name=89-nexgen-hw-inventory \
  script_type=commissioning \
  hardware_type=node \
  content@=commissioning-scripts/89-nexgen-hw-inventory.sh

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

**Script `95` is uploaded separately, for a decommission only.** It is the one
destructive script here, and it is not part of the commissioning set above:

```bash
# Edit SANITIZE_CONFIRM in the script first, and set EXPECT_SYSTEM_SERIAL to the
# chassis you intend to erase. Without that edit the script runs in audit mode
# and destroys nothing -- which is how you should run it the first time.
maas $PROFILE commissioning-scripts create \
  name=95-nexgen-storage-sanitize \
  script_type=commissioning \
  hardware_type=node \
  content@=commissioning-scripts/95-nexgen-storage-sanitize.sh
```

> **Raise the MAAS node timeout past the expected erase duration first.** Same trap
> as the burn-in: at the 30-minute default MAAS marks the machine Failed while the
> sanitise is still running, and the run's evidence is lost with it. A crypto erase
> is seconds; a block erase or an overwrite is not.
>
> Delete the script from MAAS when the decommission is done. Its default is inert,
> but a destructive script with no current purpose does not need to be sitting in
> the commissioning-script list.

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

## Testing

The whole suite runs offline against fixtures and stub tooling — no GPU, no MAAS:

```bash
./tests/run-all.sh          # everything: 339 assertions
```

`run-all.sh` is the entry point and invokes the rest, so its count is the total.
Each can also be run on its own when iterating on one area:

```bash
./tests/run-stress-verdicts.sh   # DCGM verdict matrix
./tests/run-burnin-verdicts.sh   # burn-in verdict matrix (~60 s, real load windows)
./tests/run-acceptance.py        # per-GPU acceptance adjudication
./tests/check-python-compat.py   # both generators still run on Python 3.8
./tests/validate-maas-metadata.py
```

> `run-all.sh` needs `dmidecode` on `PATH`: script `92`'s preflight requires it and
> `tests/stubs/` does not fake it, so on a host without it the baseline-counter
> assertions fail for want of a binary rather than for a defect.



`tests/stubs/` holds fixture-driven fakes for `nvidia-smi`, `dcgmi`,
`dcgmproftester13`, `gpu_burn`, `dmesg`, `systemctl` and `lspci`, so the
commissioning scripts run end to end on a machine with no GPU. `tests/stubs-hw/`
does the same for script `89`, and `tests/stubs-decomm/` for script `95` — an
`nvme` that models `SANICAP` and the sanitise log, an `lsblk` that answers `-J`
with a `TRAN` column, and a `findmnt` that can claim root is on a drive about to
be erased. `tests/stubs-raidcli/` holds only `storcli64`, kept apart on purpose:
the script finds a RAID CLI with `command -v`, so a stub beside the others could
never be *absent*, and absent is the case that matters.

`stubs-decomm/` also fakes the things a sanitisation run reaches for beyond the
storage tools — `curl`, `unzip` and `dpkg`, so the storcli fetch and its hash
check are exercised offline against `tests/fixtures/storcli-fake.zip`; `id`,
because the script refuses to run unprivileged and the suite is not root; and
`hostname`, so a generated fixture does not carry the name of whichever
workstation produced it. **No stub ever executes a
destructive command** — each appends what it would have run to
`/tmp/stub-destructive-calls`, which is how the suite proves audit mode issues
none. `tests/fixtures/` holds captured `nvidia-smi -q` documents, synthetic DCGM
result sets, and report-level JSON generated from real script output.

Beyond the verdict matrices, the suite pins the specific defects that reached
production, because each one produced a plausible-looking pass:

| Guard | The defect it pins |
|---|---|
| Report assembly under a large artifact | A single argv entry is capped at 131072 bytes, so `jq --arg <2 MB base64>` died with `E2BIG`. With pipefail but no `errexit` the script then printed `Verdict: PASS` and no JSON at all |
| Counter columns resolved by name | `SNAP_FIELDS` is discovered at runtime; when driver 595 dropped `pcie.replay_counter` every later column shifted, and the uncorrectable-remap delta became permanently null — silently disabling the FAIL gate that depends on it |
| Error count across a 250 KB log | `cmd \| head -1 \|\| echo 0` is racy under pipefail: once `sort` is still writing when `head` closes the pipe, the fallback *also* fires and the value becomes `"0\n0"` — invalid JSON, which took the whole report down |
| CPU cores are physical cores | MAAS NUMA `cores` lists hold *logical* cpu ids, so summing them reported a 2×64C/256T machine as "256 cores" |
| nvbandwidth skip adjudication | Excused only for a SKU where DCGM is known not to qualify the plugin (`NVBANDWIDTH_UNQUALIFIED_SKUS`) — never on an unrecognised SKU, and never when another test also skipped |
| Raw logs embedded, not truncated | Oversized logs keep their tail, where the per-GPU verdict is printed; log text is escaped; unusable base64 degrades to empty |
| Python 3.8 compatibility | `str.removesuffix` imported fine and failed mid-run |
| Sanitise adjudicated on `SSTAT`, not log text | A vendor log reading *"completed successfully"* while its status field reports failure. The runbook's own script greps that text and warns that the wording varies by `nvme-cli` version and by vendor |
| Audit mode issues no destructive command | The default is inert, so an unedited upload in a commissioning set inventories the media instead of erasing it. Pinned by asserting `/tmp/stub-destructive-calls` stays empty, and that a near-miss confirmation phrase does not arm the erase |
| A bounded wait that reads the clock | The bound was a sum of poll intervals, so a zero-second interval never reached it and a drive that never completes spun forever |
| Every device keeps a record | Aborting on the first failure leaves later drives with no record at all, which §14 requires each of them to have |
| An overwrite is recorded as `clear` | A clear passing itself off as a purge. On an SSD an overwrite does not reach over-provisioned blocks, so the level is stated on every row and a cleared drive still blocks release |
| Success read from the drive's own wording | `hdparm` returns to `SD0 Sanitize Idle` on completion and puts the outcome on the *next* line. Matching on "succeeded" marked two genuinely successful crypto scrambles as failures, and withheld the array rebuild on the strength of it |
| The chassis pin gates looking, not just erasing | The pin was evaluated after the confirmation check, so an audit-mode run pinned to another machine skipped it entirely and the JBOD probe deleted and rebuilt that machine's array |
| A process note does not move the verdict | An unset serial pin and an old `nvme-cli` degraded a run that purged and verified every drive to `WARN` |
| The certificate does not contradict itself | Its badge came from the stored verdict while its headline came from the evidence, so it displayed `WARN` beside a sentence saying all three drives were purged |
| A fixture carries no real identifier | Two stubs built serials with `printf`, so a scrub of literal strings missed them; base64 evidence blobs re-encoded the originals after the plaintext was scrubbed; and `hostname -f` put the generating workstation into the committed evidence |
| A device is re-identified after the erase | Evidence attributed to the wrong serial: a successful erase whose serial no longer matches is downgraded to `unverified` |
| BMC virtual media is not a drive | Found on the first real run: the ASMB11 emulates a USB disk and CDROM whether or not anything is attached, and the register listed one as needing work. A non-zero size now stops the run instead, because an image mounted over Redfish is neither identifiable nor erasable from here |
| JSON sanitise-log support is probed, not read off help text | nvme-cli 1.16 on the ephemeral image takes `-o json` for `id-ctrl` but does not advertise it for `sanitize-log`, so a capability read from a usage string got it wrong |
| An audit run still produces an audit log | `audit()` was only called from the destructive path, so an audit left `audit_log_b64` null and its own findings out of the evidence |
| A Clear never passes as a Purge | `nist_level` is recorded per device and a verified Clear still fails, unless `ACCEPT_NIST_CLEAR` marks the level as deliberately accepted |
| No finding proposes destroying a drive | Asserted per field on blockers and route actions, so the remediation register cannot regress into a quarantine list |

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
