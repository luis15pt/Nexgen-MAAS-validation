# 89 - Chassis & Component Inventory (`v1.5.0`)

[← Back to README](../../README.md)

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

## No BMC credentials are needed, and Redfish is not used

The BMC is read **in band**, over the host-side KCS interface at `/dev/ipmi0`,
not over the network. There is no username, password or Redfish session
involved: the authorisation is being root on the machine the BMC is attached
to. This is the same path MAAS's own `30-maas-01-bmc-config` takes, which is
how MAAS configures the BMC on a node it has no credentials for in the first
place — and why that script logs `IPMI detected!` before any credential exists.

Redfish would need an account and a route to the BMC's network interface.
Neither is required here.

## Drives behind a RAID controller

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

## Two sources for the PSU count, because one of them lies

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

## Card serial numbers

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

## Everything is best-effort, and says which

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
