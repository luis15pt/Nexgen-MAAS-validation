#!/usr/bin/env python3
"""Validate the embedded MAAS metadata block in every commissioning script.

MAAS looks for a very specific delimiter and silently ignores the block when it
does not match, so a typo costs you the title, description, timeout and
hardware_type with no error anywhere. The repo shipped
`# --- Start MAAS Metadata ---` for a long time, which MAAS never parsed.

This asserts, against MAAS's own regex and a real YAML parser:
  * exactly two delimiters, in the form MAAS matches
  * the body parses as a YAML mapping (prose containing ':' or '|' has broken
    this before, hence the folded block scalars)
  * required keys are present with plausible values
"""
import pathlib
import re
import sys

try:
    import yaml
except ImportError:
    print("SKIP: PyYAML not installed", file=sys.stderr)
    sys.exit(0)

# src/maasserver/forms/script.py
MAAS_RE = re.compile(
    r"\s*#\s*-+\s*(Start|End) MAAS (?P<version>\d+\.\d+) script metadata\s+-+"
)
REQUIRED = ("title", "description", "script_type", "timeout")
TIMEOUT_RE = re.compile(r"^\d{1,2}:\d{2}:\d{2}$")

root = pathlib.Path(__file__).resolve().parent.parent
rc = 0

for p in sorted((root / "commissioning-scripts").glob("*.sh")):
    lines = p.read_text().splitlines()
    idx = [i for i, l in enumerate(lines) if MAAS_RE.match(l)]
    if len(idx) != 2:
        print(f"  FAIL {p.name}: found {len(idx)} MAAS delimiters, expected 2")
        rc = 1
        continue

    body = "\n".join(re.sub(r"^\s*#", "", l) for l in lines[idx[0] + 1: idx[1]])
    try:
        meta = yaml.safe_load(body)
    except yaml.YAMLError as e:
        print(f"  FAIL {p.name}: metadata is not valid YAML -- "
              f"{str(e).splitlines()[0]}")
        rc = 1
        continue

    if not isinstance(meta, dict):
        print(f"  FAIL {p.name}: metadata parsed as {type(meta).__name__}; "
              "MAAS would ignore it")
        rc = 1
        continue

    problems = [f"missing {k}" for k in REQUIRED if not meta.get(k)]
    # `name` must NOT be declared here. MAAS rejects the whole upload with "May
    # not override values defined in embedded YAML" whenever the embedded name
    # and the name given on the upload command differ -- so declaring it turns
    # any rename, renumber or stale automation into a hard upload failure for no
    # benefit. The upload command is the single source of the name.
    if "name" in meta:
        problems.append("declares 'name', which conflicts with the upload command")
    if meta.get("script_type") != "commissioning":
        problems.append(f"script_type is {meta.get('script_type')!r}")
    if not TIMEOUT_RE.match(str(meta.get("timeout", ""))):
        problems.append(f"timeout {meta.get('timeout')!r} is not HH:MM:SS")
    if meta.get("parallel") not in (None, "disabled"):
        problems.append(f"parallel is {meta.get('parallel')!r}, expected disabled")

    if problems:
        print(f"  FAIL {p.name}: " + "; ".join(problems))
        rc = 1
    else:
        print(f"  ok   {p.name:38} {meta['timeout']}  {meta['title'][:46]}")

print("\n" + ("metadata OK" if rc == 0 else "METADATA PROBLEMS"))
sys.exit(rc)
