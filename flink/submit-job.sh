#!/usr/bin/env bash
# Submit the RDG Stream PyFlink job to the running Flink cluster.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}"

if ! docker compose ps flink-jobmanager --status running >/dev/null 2>&1; then
  echo "flink-jobmanager is not running. Start the stack first:"
  echo "  docker compose up -d"
  exit 1
fi

docker compose exec flink-jobmanager flink run -d -py /opt/flink/jobs/rdg_job.py
echo "Job submitted. Open http://localhost:8081 to verify RUNNING status."
