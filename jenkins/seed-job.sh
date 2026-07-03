#!/usr/bin/env bash
# Seed the nyc-taxi-etl pipeline job into Atlas's Jenkins (which ships JCasC but no jobs).
# Requires: JENKINS_URL (default http://localhost:${JENKINS_PORT:-63080}), JENKINS_ADMIN_USER, JENKINS_ADMIN_PASSWORD.
set -euo pipefail
JENKINS_URL="${JENKINS_URL:-http://localhost:${JENKINS_PORT:-63080}}"
JOB="nyc-taxi-etl"
XML="$(dirname "$0")/nyc-taxi-etl-job.xml"
auth=(-u "${JENKINS_ADMIN_USER:?}:${JENKINS_ADMIN_PASSWORD:?}")
crumb=$(curl -fsS "${auth[@]}" "$JENKINS_URL/crumbIssuer/api/json" | sed -n 's/.*"crumb":"\([^"]*\)".*/\1/p' || true)
hdr=(); [ -n "$crumb" ] && hdr=(-H "Jenkins-Crumb: $crumb")
if curl -fsS "${auth[@]}" "$JENKINS_URL/job/$JOB/api/json" >/dev/null 2>&1; then
  curl -fsS "${auth[@]}" "${hdr[@]}" -H "Content-Type: application/xml" --data-binary "@$XML" "$JENKINS_URL/job/$JOB/config.xml"
else
  curl -fsS "${auth[@]}" "${hdr[@]}" -H "Content-Type: application/xml" --data-binary "@$XML" "$JENKINS_URL/createItem?name=$JOB"
fi
echo "seeded job: $JOB"
