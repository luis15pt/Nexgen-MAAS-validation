#!/usr/bin/env python3
"""Acceptance adjudication matrix.

Mutates healthy evidence to induce each REJECT condition in the customer
specification and asserts the per-GPU verdict. Runs without a GPU.

Inverse cases matter as much as the positive ones: a healthy card must be
ACCEPTed, and the two conditions most likely to false-fail -- a power-capped
card at full load, and a Gen5 card in a Gen4 host -- must not be rejected.
"""
import copy
import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent / "reporting"))
import device_certificate as dc  # noqa: E402

F = pathlib.Path(__file__).resolve().parent / "fixtures" / "reports"
INV = json.loads((F / "inventory-healthy.json").read_text())
STRESS = json.loads((F / "stress-pass.json").read_text())
STRESS_CFG = json.loads((F / "stress-config.json").read_text())
BURN = json.loads((F / "burnin-full.json").read_text())

CASES = []


def case(label, want, reject=None, gpu=None, stress="ok", burn="ok"):
    def deco(fn):
        CASES.append((label, want, reject, fn, stress, burn))
        return fn
    return deco


def mut(fn):
    """Apply fn to a deep copy of GPU 0 and return the modified inventory GPU."""
    g = copy.deepcopy(INV["gpus"][0])
    fn(g)
    return g


# ---- baseline -------------------------------------------------------------
CASES.append(("healthy card", "ACCEPT", None, lambda: (INV["gpus"][0], STRESS, BURN), None, None))

# ---- #1 remapping failure / uncorrectable remapped rows -------------------
CASES.append(("#1 remapping failure occurred", "REJECT", 1,
              lambda: (mut(lambda g: g["remapped_rows"].update(failure_occurred=True)), STRESS, BURN), None, None))
CASES.append(("#1 uncorrectable remapped rows", "REJECT", 1,
              lambda: (mut(lambda g: g["remapped_rows"].update(uncorrectable=4)), STRESS, BURN), None, None))
CASES.append(("#1 uncorrectable ECC aggregate", "REJECT", 1,
              lambda: (mut(lambda g: g["ecc"].update(uncorrected_aggregate=7)), STRESS, BURN), None, None))
CASES.append(("#1 SRAM threshold exceeded", "REJECT", 1,
              lambda: (mut(lambda g: g.update(sram_threshold_exceeded=True)), STRESS, BURN), None, None))
CASES.append(("#1 SRAM uncorrectable count", "REJECT", 1,
              lambda: (mut(lambda g: g["ecc"].update(sram_uncorrected_aggregate=3)), STRESS, BURN), None, None))

# ---- #2 pending remap / bank histogram ------------------------------------
CASES.append(("#2 pending remap", "REJECT", 2,
              lambda: (mut(lambda g: g["remapped_rows"].update(pending=True)), STRESS, BURN), None, None))
CASES.append(("#2 banks below High", "REJECT", 2,
              lambda: (mut(lambda g: g["bank_remap_availability"].update(none=2)), STRESS, BURN), None, None))

# ---- #3 ECC disabled / volatile-only -------------------------------------
CASES.append(("#3 ECC disabled", "REJECT", 3,
              lambda: (mut(lambda g: g.update(ecc_mode="Disabled")), STRESS, BURN), None, None))
CASES.append(("#3 ECC mode N/A", "REJECT", 3,
              lambda: (mut(lambda g: g.update(ecc_mode=None)), STRESS, BURN), None, None))
CASES.append(("#3 volatile-only counters", "REJECT", 3,
              lambda: (mut(lambda g: g["ecc"].update(corrected_aggregate=None,
                                                     uncorrected_aggregate=None)), STRESS, BURN), None, None))

# ---- #4 Xid + load degradation ------------------------------------------
def _burn_xid(crit=None, other=None):
    b = copy.deepcopy(BURN)
    b["burn_in"]["xid"]["critical"] = crit or []
    b["burn_in"]["xid"]["other"] = other or []
    return b


for x in (48, 63, 64, 79, 92, 94, 95):
    CASES.append((f"#4 disqualifying Xid {x}", "REJECT", 4,
                  lambda x=x: (INV["gpus"][0], STRESS, _burn_xid(crit=[x])), None, None))
CASES.append(("#4 non-disqualifying Xid 13", "REVIEW", 4,
              lambda: (INV["gpus"][0], STRESS, _burn_xid(other=[13])), None, None))


def _burn_mut(fn):
    b = copy.deepcopy(BURN)
    fn(b)
    return b


CASES.append(("#4 new uncorrectable ECC under load", "REJECT", 4,
              lambda: (INV["gpus"][0], STRESS,
                       _burn_mut(lambda b: b["burn_in"]["counter_deltas"][0]
                                 .update(ecc_uncorrected_aggregate_delta=2))), None, None))
CASES.append(("#4 no load generator available", "REJECT", 4,
              lambda: (INV["gpus"][0], STRESS,
                       _burn_mut(lambda b: b["burn_in"].update(tool="none"))), None, None))
CASES.append(("#4 burn-in not run at all", "REVIEW", 4,
              lambda: (INV["gpus"][0], STRESS, None), None, None))

# ---- #5 stress evidence -------------------------------------------------
CASES.append(("#5 no stress result at all", "REJECT", 5,
              lambda: (INV["gpus"][0], None, BURN), None, None))
CASES.append(("#5 CONFIG presented as a pass", "REJECT", 5,
              lambda: (INV["gpus"][0], STRESS_CFG, BURN), None, None))


def _stress_level(lvl):
    s = copy.deepcopy(STRESS)
    s["dcgm_diagnostics"]["run_level"] = lvl
    return s


CASES.append(("#5 diagnostic level below 3", "REJECT", 5,
              lambda: (INV["gpus"][0], _stress_level("1"), BURN), None, None))
CASES.append(("#5 no results for this GPU", "REJECT", 5,
              lambda: (INV["gpus"][0], {"dcgm_diagnostics": {"run_level": 3, "test_results": []}}, BURN), None, None))

# ---- #6 throttling ------------------------------------------------------
def _burn_throttle(**kw):
    b = copy.deepcopy(BURN)
    b["burn_in"]["telemetry"][0]["throttle_samples"].update(kw)
    return b


CASES.append(("#6 hw thermal slowdown", "REJECT", 6,
              lambda: (INV["gpus"][0], STRESS, _burn_throttle(hw_thermal_slowdown=5)), None, None))
CASES.append(("#6 hw power brake slowdown", "REJECT", 6,
              lambda: (INV["gpus"][0], STRESS, _burn_throttle(hw_power_brake_slowdown=1)), None, None))
CASES.append(("#6 sw thermal slowdown", "REJECT", 6,
              lambda: (INV["gpus"][0], STRESS, _burn_throttle(sw_thermal_slowdown=2)), None, None))
CASES.append(("#6 INVERSE: sw_power_cap only", "ACCEPT", 6,
              lambda: (INV["gpus"][0], STRESS, _burn_throttle(sw_power_cap=180)), None, None))

# ---- #7 PCIe -----------------------------------------------------------
CASES.append(("#7 width trained below x16", "REJECT", 7,
              lambda: (mut(lambda g: g["pcie"].update(width_current=8)), STRESS, BURN), None, None))
CASES.append(("#7 gen below what both ends support", "REJECT", 7,
              lambda: (mut(lambda g: g["pcie"].update(gen_negotiated_max=3,
                                                      gen_device_max=5,
                                                      gen_host_max=5)), STRESS, BURN), None, None))
CASES.append(("#7 INVERSE: Gen5 card in a Gen4 host", "ACCEPT", 7,
              lambda: (mut(lambda g: g["pcie"].update(gen_negotiated_max=4,
                                                      gen_device_max=5,
                                                      gen_host_max=4)), STRESS, BURN), None, None))
CASES.append(("#7 INVERSE: idle link at Gen1", "ACCEPT", 7,
              lambda: (mut(lambda g: g["pcie"].update(gen_current=1)), STRESS, BURN), None, None))
CASES.append(("#7 elevated replay delta", "REJECT", 7,
              lambda: (INV["gpus"][0], STRESS,
                       _burn_mut(lambda b: b["burn_in"]["counter_deltas"][0]
                                 .update(pcie_replay_delta=500))), None, None))

# ---- #8 identity -------------------------------------------------------
CASES.append(("#8 serial mismatch across sources", "REJECT", 8,
              lambda: (mut(lambda g: g["identity"].update(serial_from_query="9999999999999")), STRESS, BURN), None, None))
CASES.append(("#8 InfoROM corrupted", "REJECT", 8,
              lambda: (mut(lambda g: g["identity"].update(inforom_corrupted=True)), STRESS, BURN), None, None))
CASES.append(("#8 serial absent", "REJECT", 8,
              lambda: (mut(lambda g: g.update(serial=None)), STRESS, BURN), None, None))
CASES.append(("#8 UUID absent", "REJECT", 8,
              lambda: (mut(lambda g: g.update(uuid=None)), STRESS, BURN), None, None))


# ---- #3 ECC provenance: enabling ECC now does not backfill history --------
CFG_ON = {"gpu_config": [{"gpu_index": 0, "ecc_before": "Enabled", "ecc_changed": False}]}
CFG_ENABLED_NOW = {"gpu_config": [{"gpu_index": 0, "ecc_before": "Disabled", "ecc_changed": True}]}


def _with_cfg(cfg):
    def fn():
        return (INV["gpus"][0], STRESS, BURN, cfg)
    return fn


def _burn_loaded(secs, gpu=0):
    b = copy.deepcopy(BURN)
    for t in b["burn_in"]["telemetry"]:
        if t["gpu_index"] == gpu:
            t["loaded_seconds"] = secs
    return b


# Regression from a real run: the load generator covered only some GPUs while
# the run's wall-clock duration looked correct.
CASES.append(("#4 this GPU barely loaded", "REJECT", 4,
              lambda: (INV["gpus"][0], STRESS, _burn_loaded(290)), None, None))
CASES.append(("#4 this GPU fully loaded", "ACCEPT", 4,
              lambda: (INV["gpus"][0], STRESS, _burn_loaded(1810)), None, None))

def _burn_window(requested, loaded, gpu=0):
    b = copy.deepcopy(BURN)
    b["burn_in"]["duration_requested_seconds"] = requested
    b["burn_in"]["duration_actual_seconds"] = requested + 3
    for t in b["burn_in"]["telemetry"]:
        if t["gpu_index"] == gpu:
            t["loaded_seconds"] = loaded
    return b


# A short validation run must not read as acceptance evidence, and a full run
# must not be downgraded for it.
CASES.append(("#4 5-minute validation window", "REVIEW", 4,
              lambda: (INV["gpus"][0], STRESS, _burn_window(300, 300)), None, None))
CASES.append(("#4 full 30-minute window", "ACCEPT", 4,
              lambda: (INV["gpus"][0], STRESS, _burn_window(1800, 1800)), None, None))

def _burn_gb(results, errors=0, tool="gpu-burn"):
    b = copy.deepcopy(BURN)
    b["burn_in"]["tool"] = tool
    b["burn_in"]["gpu_burn_results"] = results
    b["burn_in"]["gpu_burn_error_count"] = errors
    return b


_ALL_OK = [{"gpu_index": g, "status": "OK"} for g in range(8)]
CASES.append(("#4 gpu-burn OK on this GPU", "ACCEPT", 4,
              lambda: (INV["gpus"][0], STRESS, _burn_gb(_ALL_OK)), None, None))
CASES.append(("#4 gpu-burn FAULTY on this GPU", "REJECT", 4,
              lambda: (INV["gpus"][0], STRESS,
                       _burn_gb([{"gpu_index": 0, "status": "FAULTY"}] + _ALL_OK[1:])), None, None))
CASES.append(("#4 gpu-burn computation errors", "REJECT", 4,
              lambda: (INV["gpus"][0], STRESS, _burn_gb(_ALL_OK, errors=17)), None, None))
CASES.append(("#4 gpu-burn no verdict for this GPU", "REJECT", 4,
              lambda: (INV["gpus"][0], STRESS, _burn_gb(_ALL_OK[1:])), None, None))

EXTRA = [
    ("#3 ECC already on before commissioning", "ACCEPT", _with_cfg(CFG_ON)),
    ("#3 ECC enabled by script 91 this run", "REJECT", _with_cfg(CFG_ENABLED_NOW)),
    ("#3 script 91 result unavailable", "REVIEW", _with_cfg(None)),
]


def main():
    rc = 0
    print(f"{'case':44} {'want':8} {'got':8}")
    print("-" * 64)
    for label, want, _reject, fn, _s, _b in CASES:
        gpu, stress, burn = fn()
        got = dc.evaluate_gpu_acceptance(gpu, stress, burn, CFG_ON)["verdict"]
        ok = got == want
        print(f"{'ok  ' if ok else 'FAIL'} {label:38} {want:8} {got:8}")
        if not ok:
            rc = 1
            for c in dc.evaluate_gpu_acceptance(gpu, stress, burn)["criteria"]:
                if c["status"] != "PASS":
                    print(f"       {c['status']:5} #{c['reject']} {c['label']}: {c['detail']}")
    for label, want, fn in EXTRA:
        gpu, stress, burn, cfg = fn()
        e = dc.evaluate_gpu_acceptance(gpu, stress, burn, cfg)
        ok = e["verdict"] == want
        print(f"{'ok  ' if ok else 'FAIL'} {label:38} {want:8} {e['verdict']:8}")
        if not ok:
            rc = 1
            for c in e["criteria"]:
                if c["status"] != "PASS":
                    print(f"       {c['status']:5} #{c['reject']} {c['label']}: {c['detail']}")

    # Test-age reporting is separate: we know the test date, not the ship date.
    age = dc.acceptance_test_age_days(INV, STRESS, BURN)
    print("-" * 64)
    print(f"test evidence age: {age:.2f} day(s)" if age is not None else "test age unavailable")
    return rc


if __name__ == "__main__":
    sys.exit(main())
