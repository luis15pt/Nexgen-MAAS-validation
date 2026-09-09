# 95 - Storage Sanitisation & Evidence (`v1.0.0`)

[← Back to README](../../README.md)

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

## Completion is a number, not a sentence

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

## NIST Clear and NIST Purge are different claims

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

## Drive capability is not controller capability

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

## Nothing here proposes destroying a drive

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

## Why `storcli` is probed for and never fetched

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

## A process note is not a finding about the media

The verdict answers one question: was the data destroyed. Two things that say
nothing about that were degrading it — an unset `EXPECT_SYSTEM_SERIAL` and an
`nvme-cli` too old to emit a JSON sanitise log. A run that purged and verified
every drive came back `WARN`, which reads as a hedge on the erase rather than
what it was: a note about how the run was configured.

Those are recorded with severity `advisory`. They appear in the evidence and in
the certificate's findings, and they do not move the verdict. `warning` and
`critical` remain reserved for statements about the media.

## Verification is identity, and says which

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
