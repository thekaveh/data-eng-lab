#!/usr/bin/env bash
# Seed the nyc-taxi-etl pipeline job into Atlas's Jenkins (which ships JCasC but no jobs).
# Requires: JENKINS_URL (default http://localhost:${JENKINS_PORT:-63080}), JENKINS_ADMIN_USER, JENKINS_ADMIN_PASSWORD.
set -euo pipefail
JENKINS_URL="${JENKINS_URL:-http://localhost:${JENKINS_PORT:-63080}}"
JOB="nyc-taxi-etl"
XML="$(dirname "$0")/nyc-taxi-etl-job.xml"
auth=(-u "${JENKINS_ADMIN_USER:?}:${JENKINS_ADMIN_PASSWORD:?}")
# Atlas Jenkins issues session-bound CSRF crumbs: the crumb and all subsequent
# authenticated POSTs must share the same session cookie jar, otherwise Jenkins
# returns HTTP 403 "No valid crumb was included in the request".
CJ=$(mktemp)
crumb=$(curl -fsS "${auth[@]}" -c "$CJ" "$JENKINS_URL/crumbIssuer/api/json" | sed -n 's/.*"crumb":"\([^"]*\)".*/\1/p' || true)
hdr=(); [ -n "$crumb" ] && hdr=(-H "Jenkins-Crumb: $crumb")
if curl -fsS "${auth[@]}" -b "$CJ" "$JENKINS_URL/job/$JOB/api/json" >/dev/null 2>&1; then
  curl -fsS "${auth[@]}" -b "$CJ" "${hdr[@]}" -H "Content-Type: application/xml" --data-binary "@$XML" "$JENKINS_URL/job/$JOB/config.xml"
else
  curl -fsS "${auth[@]}" -b "$CJ" "${hdr[@]}" -H "Content-Type: application/xml" --data-binary "@$XML" "$JENKINS_URL/createItem?name=$JOB"
fi
echo "seeded job: $JOB"
