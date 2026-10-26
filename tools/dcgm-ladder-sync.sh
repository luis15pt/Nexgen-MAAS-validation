#!/bin/bash
# The four DCGM ladder scripts (93-96) differ only in their MAAS metadata
# header.  Everything after "# --- End MAAS Metadata ---" is one body, and the
# script reads its level from its own file name.  Edit any one of them, then:
#
#   tools/dcgm-ladder-sync.sh sync commissioning-scripts/95-nexgen-dcgm-level3.sh
#
# copies that body onto the other three, keeping each one's header.  `check`
# (run by tests/run-all.sh) exits 1 if the bodies have drifted apart.
set -o pipefail
cd "$(dirname "$0")/.." || exit 1
END='# --- End MAAS Metadata ---'
FILES=(commissioning-scripts/9[3456]-nexgen-dcgm-level[1234].sh)

header() { awk -v e="$END" '{print} $0==e{exit}' "$1"; }
body()   { awk -v e="$END" 'f{print} $0==e{f=1}' "$1"; }

case "${1:-check}" in
    check)
        ref="${FILES[0]}"; rc=0
        for f in "${FILES[@]:1}"; do
            if ! diff -q <(body "$ref") <(body "$f") >/dev/null; then
                echo "  FAIL  $f body differs from $ref -- run: tools/dcgm-ladder-sync.sh sync <the one you edited>"
                diff <(body "$ref") <(body "$f") | head -20 | sed 's/^/          /'
                rc=1
            fi
        done
        [[ $rc -eq 0 ]] && echo "  ok    ${#FILES[@]} ladder scripts share one body"
        exit $rc ;;
    sync)
        src="$2"; [[ -f "$src" ]] || { echo "usage: $0 sync <98x-...sh>" >&2; exit 2; }
        for f in "${FILES[@]}"; do
            [[ "$f" -ef "$src" ]] && continue
            { header "$f"; body "$src"; } > "$f.tmp" && mv "$f.tmp" "$f" && chmod +x "$f"
            echo "synced $f from $src"
        done ;;
    *) echo "usage: $0 check | sync <file>" >&2; exit 2 ;;
esac
