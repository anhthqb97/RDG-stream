#!/usr/bin/env bash
# Validate commit messages match project format:
#   initial commit
#   TASK-XXX: <action> <message>
set -euo pipefail

RANGE="${1:-HEAD~1..HEAD}"
SUBJECT_PATTERN='^(initial commit|TASK-[0-9]{3}: (add|update|fix|remove|refactor|docs|test|chore) .+)$'

echo "Checking commits in range: ${RANGE}"
FAILED=0

while IFS= read -r subject; do
  [[ -z "${subject}" ]] && continue
  if [[ ! "${subject}" =~ ${SUBJECT_PATTERN} ]]; then
    echo "❌ Invalid commit subject: ${subject}"
    echo "   Expected: initial commit"
    echo "   Or:       TASK-XXX: <action> <message>"
    echo "   Actions:  add | update | fix | remove | refactor | docs | test | chore"
    FAILED=1
  else
    echo "✅ ${subject}"
  fi
done < <(git log "${RANGE}" --pretty=format:%s)

if [[ "${FAILED}" -ne 0 ]]; then
  exit 1
fi

echo "All commit messages valid."
