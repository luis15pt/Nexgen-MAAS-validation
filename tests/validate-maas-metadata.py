#!/usr/bin/env python3
"""Validate the metadata comment block in every commissioning script.

Important: this block is DOCUMENTATION. It uses the delimiter
`# --- Start MAAS Metadata ---`, which MAAS does *not* match -- MAAS looks for
`# --- Start MAAS 1.0 script metadata ---` -- so MAAS ignores it entirely and
takes name, script_type and everything else from the upload command.

That is deliberate. Switching to the delimiter MAAS parses made MAAS start
validating the block, and uploads stopped working against MAAS 3.1.4 in this
environment. Reverting restored them. The consequence to keep in mind is that
the declared `timeout` values are NOT enforced by MAAS: Script.timeout defaults
to 0, which means no timeout. The in-script `timeout` wrappers around dcgmi and
the load generator are the real protection.

So this checks the block is internally consistent and readable, not that MAAS
will act on it. The one thing that genuinely matters is that `name` matches the
filename, because the upload command is derived from the filename and a stale
name silently registers a second script.
"""
import pathlib
import re
import sys

DELIM = re.compile(r"^#\s*-+\s*(Start|End) MAAS Metadata\s*-+\s*$")
# The delimiter MAAS itself matches. Its presence would mean the block is live
# again, which is the state that broke uploads here.
MAAS_LIVE = re.compile(
    r"\s*#\s*-+\s*(Start|End) MAAS (?P<version>\d+\.\d+) script metadata\s+-+"
)
REQUIRED = ("name", "title", "description", "script_type", "timeout")
TIMEOUT_RE = re.compile(r"^\d{1,2}:\d{2}:\d{2}$")

root = pathlib.Path(__file__).resolve().parent.parent
rc = 0

for p in sorted((root / "commissioning-scripts").glob("*.sh")):
    lines = p.read_text().splitlines()
    problems = []

    if any(MAAS_LIVE.match(l) for l in lines):
        problems.append(
            "uses the delimiter MAAS parses; that made MAAS validate the block "
            "and broke uploads on 3.1.4 -- see this file's docstring")

    idx = [i for i, l in enumerate(lines) if DELIM.match(l)]
    if len(idx) != 2:
        print(f"  FAIL {p.name}: found {len(idx)} metadata delimiters, expected 2")
        rc = 1
        continue

    meta = {}
    key = None
    for l in lines[idx[0] + 1: idx[1]]:
        m = re.match(r"^#\s+([a-z_]+):\s*(.*)$", l)
        if m:
            key = m.group(1)
            meta[key] = m.group(2)
        elif key and l.startswith("#"):
            meta[key] += " " + l.lstrip("#").strip()

    problems += [f"missing {k}" for k in REQUIRED if not meta.get(k)]
    if meta.get("name") != p.stem:
        problems.append(f"name {meta.get('name')!r} != filename stem {p.stem!r}")
    if meta.get("script_type") != "commissioning":
        problems.append(f"script_type is {meta.get('script_type')!r}")
    if not TIMEOUT_RE.match(str(meta.get("timeout", ""))):
        problems.append(f"timeout {meta.get('timeout')!r} is not HH:MM:SS")

    if problems:
        print(f"  FAIL {p.name}: " + "; ".join(problems))
        rc = 1
    else:
        print(f"  ok   {meta['name']:34} {meta['timeout']}  {meta['title'][:42]}")

print("\n" + ("metadata OK (documentation only -- MAAS does not parse it)"
              if rc == 0 else "METADATA PROBLEMS"))
sys.exit(rc)
