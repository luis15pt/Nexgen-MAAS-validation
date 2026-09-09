#!/bin/bash
# Rebuild examples/EXAMPLE-GPU-001-MAAS-validation.html from the committed test
# fixtures, so the published example does not drift behind the generator again.
#
# Uses file-based mode -- no MAAS, no network, no real machine. The fixtures are
# synthetic; the only edit is the hostname, which becomes EXAMPLE-GPU-001 so the
# published filename and URL stay stable.
set -euo pipefail
cd "$(dirname "$0")/.."

F=tests/fixtures
TMP=$(mktemp -d); trap 'rm -rf "$TMP"' EXIT

cp "$F"/reports/{install-pass,config-ecc-already-on,inventory-healthy,stress-pass,burnin-full}.json \
   "$F"/hwinv-183.json "$TMP/"

python3 - "$TMP" <<'PY'
import json, pathlib, sys
for f in pathlib.Path(sys.argv[1]).glob('*.json'):
    o = json.loads(f.read_text())
    def fix(x):
        if isinstance(x, dict):
            for k, v in x.items():
                if k == 'hostname' and isinstance(v, str):
                    x[k] = 'EXAMPLE-GPU-001'
                else:
                    fix(v)
        elif isinstance(x, list):
            for v in x:
                fix(v)
    fix(o)
    f.write_text(json.dumps(o, indent=2))
PY

python3 reporting/device_certificate.py \
    --hwinv     "$TMP/hwinv-183.json" \
    --install   "$TMP/install-pass.json" \
    --config    "$TMP/config-ecc-already-on.json" \
    --inventory "$TMP/inventory-healthy.json" \
    --stress    "$TMP/stress-pass.json" \
    --burnin    "$TMP/burnin-full.json" \
    -o examples/EXAMPLE-GPU-001-MAAS-validation.html

echo "Regenerated examples/EXAMPLE-GPU-001-MAAS-validation.html"
