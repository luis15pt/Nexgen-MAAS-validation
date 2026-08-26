#!/bin/bash
# Full offline test suite. No GPU required -- everything runs against fixtures
# and stub tooling.
cd "$(dirname "$0")/.." || exit 1
rc=0
hdr() { printf '\n\033[1m== %s\033[0m\n' "$1"; }

hdr "Syntax"
for f in commissioning-scripts/*.sh; do
    bash -n "$f" && printf '  ok    %s\n' "$f" || { printf '  FAIL  %s\n' "$f"; rc=1; }
done
python3 -m py_compile reporting/device_certificate.py \
    && echo "  ok    reporting/device_certificate.py" || rc=1

hdr "Script stdout must be parseable JSON with nothing leaked ahead of it"
export PATH="$PWD/tests/stubs:$PATH" STUB_GPU_COUNT=2
rm -f /tmp/stub-*
for spec in "91-nexgen-gpu-mig-ecc-config.sh:" \
            "98-nexgen-gpu-inventory.sh:" \
            "99-nexgen-gpu-stress-test.sh:DCGM_FIXTURE=tests/fixtures/dcgm/all-pass.json" \
            "92-nexgen-gpu-burn-in.sh:BURN_DURATION=2 BURN_SAMPLE_INTERVAL=1 STUB_LOAD_SLEEP=1"; do
    s="${spec%%:*}"; envs="${spec#*:}"
    rm -f /tmp/stub-*
    if env $envs bash "commissioning-scripts/$s" 2>/dev/null | jq -e . >/dev/null 2>&1; then
        printf '  ok    %s\n' "$s"
    else
        printf '  FAIL  %s -- stdout is not clean JSON\n' "$s"; rc=1
    fi
done

hdr "Script 99 verdict matrix"
./tests/run-99-verdicts.sh || rc=1

hdr "Script 92 burn-in matrix"
./tests/run-92-burnin.sh || rc=1

hdr "Acceptance adjudication matrix"
python3 tests/run-acceptance.py || rc=1

hdr "End-to-end report rendering"
F=tests/fixtures/reports
check_report() { # label, expected headline, extra args...
    local label="$1" want="$2"; shift 2
    local out=/tmp/rep-test.html got
    python3 reporting/device_certificate.py "$@" -o "$out" --quiet 2>/dev/null
    got=$(grep -oE 'class="badge badge-[a-z]+">[A-Z/]+' "$out" | head -1 | grep -oE '[A-Z/]+$')
    if [[ "$got" == "$want" ]]; then printf '  ok    %-36s -> %s\n' "$label" "$got"
    else printf '  FAIL  %-36s -> got %s, want %s\n' "$label" "${got:-none}" "$want"; rc=1; fi
}
check_report "healthy, all stages"    PASS --install $F/install-pass.json --inventory $F/inventory-healthy.json --stress $F/stress-pass.json --burnin $F/burnin-full.json
check_report "two rejectable cards"   FAIL --install $F/install-pass.json --inventory $F/inventory-mixed.json   --stress $F/stress-pass.json --burnin $F/burnin-full.json
check_report "stress returned CONFIG" FAIL --install $F/install-pass.json --inventory $F/inventory-healthy.json --stress $F/stress-config.json --burnin $F/burnin-full.json
check_report "evidence missing"       WARN --install $F/install-pass.json
check_report "no burn-in evidence"    WARN --install $F/install-pass.json --inventory $F/inventory-healthy.json --stress $F/stress-pass.json

hdr "Rendered HTML is well-formed"
python3 - <<'PY' || rc=1
from html.parser import HTMLParser
import sys
class P(HTMLParser):
    def __init__(s):
        super().__init__(); s.stack=[]; s.err=0
        s.void={'br','img','hr','meta','link','input','col'}
    def handle_starttag(s,t,a):
        if t not in s.void: s.stack.append(t)
    def handle_endtag(s,t):
        if t in s.void: return
        if s.stack and s.stack[-1]==t: s.stack.pop()
        elif t in s.stack:
            while s.stack and s.stack.pop()!=t: s.err+=1
        else: s.err+=1
p=P(); p.feed(open('/tmp/rep-test.html').read())
bad = p.err or p.stack
print(f"  {'FAIL' if bad else 'ok  '}  unbalanced={p.err} unclosed={len(p.stack)}")
sys.exit(1 if bad else 0)
PY

printf '\n%s\n' "$([[ $rc -eq 0 ]] && echo 'ALL TESTS PASSED' || echo 'FAILURES PRESENT')"
exit $rc
