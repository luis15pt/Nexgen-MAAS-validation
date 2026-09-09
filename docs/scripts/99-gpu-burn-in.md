# 99 - Sustained Burn-In (`v1.7.0`, optional)

[← Back to README](../../README.md)

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

## Chassis telemetry: why a hot GPU is not yet a finding

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

## Load source

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
