#!/usr/bin/env bash
# Fails if any relative markdown link or CLAUDE.md @import points at a missing
# file. Catches link rot from renames — the one doc failure that IS mechanical.
# It cannot tell whether content is accurate; that's what the update discipline
# in docs/knowledge/INDEX.md is for.
set -uo pipefail
cd "$(dirname "$0")/.."

fail=0
report() { echo "DANGLING: $1 -> $2"; fail=1; }

# Relative markdown links: [text](path), ignoring URLs and #anchors
while IFS= read -r f; do
    while IFS= read -r link; do
        link=${link#*](}; link=${link%)}; link=${link%%#*}
        case "$link" in http*|mailto:*|"") continue ;; esac
        [ -e "$(dirname "$f")/$link" ] || report "$f" "$link"
    done < <(grep -oE '\]\([^)]+\)' "$f")
done < <(find . -name '*.md' -not -path './.git/*')

# CLAUDE.md @imports resolve from the repo root
while IFS= read -r link; do
    [ -e "$link" ] || report "CLAUDE.md" "$link"
done < <(grep -oE '@[A-Za-z0-9_./-]+\.md' CLAUDE.md | tr -d '@')

[ "$fail" -eq 0 ] && echo "docs ok: all links resolve"
exit "$fail"
