#!/usr/bin/env bash
# Validate commit messages match project format:
#   initial commit
#   TASK-XXX: <action> <message>
# Reject Co-authored-by: Cursor trailers in commit bodies.
set -euo pipefail

RANGE="${1:-HEAD~1..HEAD}"
SUBJECT_PATTERN='^(initial commit|TASK-[0-9]{3}: (add|update|fix|remove|refactor|docs|test|chore) .+)$'
COAUTHOR_PATTERN='^Co-authored-by: Cursor <cursoragent@cursor.com>$'

echo "Checking commits in range: ${RANGE}"
FAILED=0

while IFS= read -r sha; do
  [[ -z "${sha}" ]] && continue
  subject="$(git log -1 --pretty=format:%s "${sha}")"
  body="$(git log -1 --pretty=format:%b "${sha}")"

  if [[ ! "${subject}" =~ ${SUBJECT_PATTERN} ]]; then
    echo "❌ Invalid commit subject (${sha:0:7}): ${subject}"
    echo "   Expected: initial commit"
    echo "   Or:       TASK-XXX: <action> <message>"
    echo "   Actions:  add | update | fix | remove | refactor | docs | test | chore"
    FAILED=1
  else
    echo "✅ ${subject}"
  fi

  if [[ -n "${body}" ]] && grep -qE "${COAUTHOR_PATTERN}" <<< "${body}"; then
    echo "❌ Forbidden co-author trailer in ${sha:0:7}: Co-authored-by: Cursor"
    FAILED=1
  fi
done < <(git log "${RANGE}" --pretty=format:%H)

if [[ "${FAILED}" -ne 0 ]]; then
  exit 1
fi

echo "All commit messages valid."
