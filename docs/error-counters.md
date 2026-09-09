# Error counters across the pipeline

[← Back to README](../README.md)

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
