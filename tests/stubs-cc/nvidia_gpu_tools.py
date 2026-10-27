#!/usr/bin/env python3
"""Test stub for NVIDIA gpu-admin-tools (nvidia_gpu_tools.py).

Models exactly the surface script 90 depends on:
  --devices <bdf>                 one full BDF, as the script passes it
  --query-cc-mode                 logs "<gpu> CC mode is <mode>" on stderr
  --set-cc-mode=off               writes the new mode to the state file
  --reset-after-cc-mode-switch    re-reads the mode and fails if it did not take
Exit 1 on any error, as the real tool does, with nothing on stdout.

  STUB_CC_MODES   comma list, one mode per GPU in lspci order (default: all off)
  STUB_CC_STATE   file holding the current mode per BDF across invocations
  STUB_CC_STICKY  BDF (or "all") whose mode refuses to change
  STUB_CC_BROKEN  BDF the tool cannot initialise at all
"""
import json
import os
import sys

def log(level, msg):
    sys.stderr.write("2026-10-27,12:00:00.000 %-8s %s\n" % (level, msg))

args = sys.argv[1:]
bdf = None
query = False
set_mode = None
reset = False
for a in args:
    if a == "--devices":
        bdf = args[args.index(a) + 1]
    elif a == "--query-cc-mode":
        query = True
    elif a.startswith("--set-cc-mode="):
        set_mode = a.split("=", 1)[1]
    elif a == "--reset-after-cc-mode-switch":
        reset = True
    elif a == "--ignore-nvidia-driver":
        log("ERROR", "stub: --ignore-nvidia-driver must not be used")
        sys.exit(1)

if not bdf:
    log("ERROR", "stub: --devices <bdf> required")
    sys.exit(1)

state_path = os.environ.get("STUB_CC_STATE", "/tmp/stub-cc-state.json")
try:
    with open(state_path) as fh:
        state = json.load(fh)
except Exception:
    state = {}

# First sight of a BDF: seed from STUB_CC_MODES by lspci order (bus 0x1b + i).
if bdf not in state:
    modes = [m.strip() for m in os.environ.get("STUB_CC_MODES", "").split(",") if m.strip()]
    try:
        idx = int(bdf.split(":")[1], 16) - 0x1b
    except Exception:
        idx = 0
    state[bdf] = modes[idx] if 0 <= idx < len(modes) else "off"

if os.environ.get("STUB_CC_BROKEN") == bdf:
    log("ERROR", "GPU %s BAR0 is not accessible, bad?" % bdf)
    sys.exit(1)

gpu = "GPU %s [NVIDIA RTX PRO 6000 Blackwell Server Edition]" % bdf
log("INFO", "Selected %s" % gpu)

sticky = os.environ.get("STUB_CC_STICKY", "")
if set_mode:
    if sticky in ("all", bdf):
        log("INFO", "%s CC mode set to %s. It will be active after GPU reset." % (gpu, set_mode))
        if reset:
            log("ERROR", "%s failed to switch to CC mode %s, current mode is %s." % (gpu, set_mode, state[bdf]))
            sys.exit(1)
    else:
        state[bdf] = set_mode
        log("INFO", "%s CC mode set to %s. It will be active after GPU reset." % (gpu, set_mode))
        if reset:
            log("INFO", "%s was reset to apply the new CC mode." % gpu)
    with open(state_path, "w") as fh:
        json.dump(state, fh)

if query:
    log("INFO", "%s CC mode is %s" % (gpu, state[bdf]))
with open(state_path, "w") as fh:
    json.dump(state, fh)
sys.exit(0)
