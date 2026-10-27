# Testing

[← Back to README](../README.md)

The whole suite runs offline against fixtures and stub tooling — no GPU, no MAAS:

```bash
./tests/run-all.sh          # everything: 355 assertions
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
commissioning scripts run end to end on a machine with no GPU. `tests/stubs-cc/`
fakes NVIDIA's `nvidia_gpu_tools.py` (one CC mode per GPU, a mode that refuses
to switch, a GPU the tool cannot read) plus `mokutil`, `lsmod` and an inert
`modprobe`, so script `90`'s Confidential Computing stage runs against it; `90`
guards its `main`, so the suite sources its functions rather than running an
install. `tests/stubs-hw/`
does the same for script `89`, and `tests/stubs-decomm/` for script `70` — an
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
