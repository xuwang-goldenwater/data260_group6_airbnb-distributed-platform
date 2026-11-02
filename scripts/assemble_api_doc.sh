#!/bin/bash
# Assemble docs/api/*.md into one API design document: docs/api_design.md
set -e
OUT=docs/api_design.md
FILES="docs/api/00_conventions.md docs/entities.md docs/api/user_property.md docs/api/booking.md docs/api/messaging_analytics.md docs/api/review.md docs/api/ai.md docs/api/agent_payloads.md"
TODAY=$(date +%Y-%m-%d)

{
  echo "# Airbnb Distributed Platform — API Design Document"
  echo
  echo "DATA 260 · Group 6 · Version 1.0 · $TODAY"
  echo
  echo "Source files live in \`docs/api/\` and \`docs/entities.md\`; this document is assembled from them."
  echo
  echo "## Contents"
  echo
  n=1
  for f in $FILES; do
    t=$(head -1 "$f" | sed 's/^# *//')
    a=$(echo "$t" | tr 'A-Z' 'a-z' | sed 's/[^a-z0-9 -]//g; s/ /-/g')
    echo "$n. [$t](#$a)"
    n=$((n+1))
  done
  for f in $FILES; do
    echo
    echo "---"
    echo
    # demote every heading one level, but not lines inside fenced code blocks
    # file title -> level 2; every other heading goes two levels deeper (max level 6)
    awk 'NR==1{sub(/^# /,"## "); print; next}
         /^```/{fence=!fence}
         !fence && /^#{1,4} /{print "##" $0; next}
         !fence && /^#{5} /{print "#" $0; next}
         {print}' "$f" \
      | sed -E 's/^Status: [Dd]raft for team review$/Status: Final v1.0/'
    echo
  done
} > "$OUT"

echo "Wrote $OUT ($(wc -l < "$OUT") lines)"
grep -c '^## ' "$OUT" | xargs echo "Top-level sections:"
