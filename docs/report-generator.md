# Report Generator

[← Back to README](../README.md)

`reporting/device_certificate.py` (v3.3.0) generates a consolidated HTML certification report from MAAS commissioning data.

## Prerequisites

```bash
pip install requests-oauthlib
```

Python **3.8 or newer**. The generator is often run from a different host than
it is developed on, and `from __future__ import annotations` hides most of that
difference — the `X | None` annotations are never evaluated, so the module
imports cleanly on 3.8 and an incompatibility surfaces mid-run instead. That is
what `tests/check-python-compat.py` exists to catch; it checks syntax *and*
runtime API use, because a syntax check cannot see an attribute call.

## Configuration

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

## Usage

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

## Certificate of Sanitisation

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

## Raw evidence in the report

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

## Hardware acceptance

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

## Regenerating the published example

`examples/EXAMPLE-GPU-001-MAAS-validation.html` is built from the committed test
fixtures, not from a real machine:

```bash
./examples/regenerate.sh
```

It runs the generator in file mode — no MAAS, no network — over
`tests/fixtures/`, rewriting only the hostname to `EXAMPLE-GPU-001` so the
published filename and URL stay stable. Output is deterministic apart from the
generation timestamp in the footer. Re-run it whenever the generator changes, so
the published example does not drift behind the code again.
