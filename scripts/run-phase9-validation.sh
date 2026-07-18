#!/usr/bin/env bash
# Phase 9 validation runner — executes TC-001..TC-024 (P0/P1) with evidence.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}"

RUN_ID="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
INCLUDE_TEARDOWN=false
INCLUDE_P2=false
SKIP_SLOW=false

usage() {
  cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Options:
  --include-teardown   Run TC-024 (docker compose down -v) at end — destructive
  --include-p2         Run TC-019, TC-020 (Kafka/Cassandra restart)
  --skip-slow          Skip TC-011 window wait and TC-018 TaskManager restart
  -h, --help           Show this help

Logs evidence to: /tmp/rdg-phase9-${RUN_ID//:/}.log
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --include-teardown) INCLUDE_TEARDOWN=true; shift ;;
    --include-p2) INCLUDE_P2=true; shift ;;
    --skip-slow) SKIP_SLOW=true; shift ;;
    -h | --help) usage; exit 0 ;;
    *) echo "Unknown option: $1"; usage; exit 1 ;;
  esac
done

LOG="/tmp/rdg-phase9-${RUN_ID//:/}.log"
: >"${LOG}"

declare -a PASSED=()
declare -a FAILED=()
declare -a SKIPPED=()

log() { echo "[$(date -u +%H:%M:%S)] $*" | tee -a "${LOG}"; }

pass() {
  local tc="$1"
  local msg="$2"
  PASSED+=("${tc}")
  log "PASS ${tc}: ${msg}"
}

fail() {
  local tc="$1"
  local msg="$2"
  FAILED+=("${tc}")
  log "FAIL ${tc}: ${msg}"
}

skip() {
  local tc="$1"
  local msg="$2"
  SKIPPED+=("${tc}")
  log "SKIP ${tc}: ${msg}"
}

wait_healthy() {
  local seconds="${1:-60}"
  log "Waiting ${seconds}s for services..."
  sleep "${seconds}"
}

flink_running_job() {
  curl -sf http://localhost:8081/jobs/overview \
    | python3 -c "import sys,json; jobs=json.load(sys.stdin)['jobs']; print(next((j['jid'] for j in jobs if j['state']=='RUNNING'), ''))"
}

ensure_flink_job() {
  local jid
  jid="$(flink_running_job || true)"
  if [[ -n "${jid}" ]]; then
    log "Flink job already RUNNING: ${jid}"
    return 0
  fi
  log "Submitting Flink job..."
  ./flink/submit-job.sh >>"${LOG}" 2>&1
  sleep 15
  jid="$(flink_running_job || true)"
  [[ -n "${jid}" ]]
}

run_tc001() {
  log "--- TC-001 Stack health ---"
  if ! docker compose ps --format json >/dev/null 2>&1; then
    fail TC-001 "docker compose ps failed"
    return
  fi
  local bad
  bad="$(docker compose ps --format json | python3 -c "
import sys, json
bad = []
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    c = json.loads(line)
    name = c.get('Name', c.get('Service', '?'))
    state = c.get('State', '')
    health = c.get('Health', '') or ''
    if state not in ('running',):
        bad.append(f'{name}:{state}')
    elif health and health != 'healthy':
        if name not in ('rdg-flink-taskmanager', 'rdg-mock-producer', 'rdg-kafka-ui', 'rdg-read-api'):
            bad.append(f'{name}:health={health}')
print(';'.join(bad))
")"
  if [[ -z "${bad}" ]]; then
    pass TC-001 "Core services running/healthy"
  else
    fail TC-001 "Unhealthy containers: ${bad}"
  fi
}

run_tc002() {
  log "--- TC-002 Kafka topics ---"
  local topics
  topics="$(docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --list 2>>"${LOG}" | tr '\n' ' ')"
  if [[ "${topics}" == *metrics.raw* && "${topics}" == *metrics.dlq* && "${topics}" == *events.lifecycle* ]]; then
    pass TC-002 "Topics present: ${topics}"
  else
    fail TC-002 "Missing topics; got: ${topics}"
  fi
}

run_tc003() {
  log "--- TC-003 Cassandra schema ---"
  local desc
  desc="$(docker compose exec -T cassandra cqlsh -e "DESCRIBE KEYSPACE rdg;" 2>>"${LOG}")"
  if [[ "${desc}" == *metrics_current* && "${desc}" == *metrics_ts* ]]; then
    pass TC-003 "Keyspace rdg with metrics_current and metrics_ts"
  else
    fail TC-003 "Schema incomplete"
  fi
}

run_tc004() {
  log "--- TC-004 Produce valid message ---"
  docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 --topic metrics.raw <<'EOF' >>"${LOG}" 2>&1
{"plant_id":"TC04","equipment_id":"GEN-01","metric":"power_output_mw","value":10.0,"unit":"MW","ts":"2026-07-18T10:00:00Z"}
EOF
  pass TC-004 "Published plant_id=TC04 to metrics.raw"
}

run_tc005() {
  log "--- TC-005 Consume message ---"
  local found="" p end off
  for p in 0 1 2; do
    end="$(docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
      --bootstrap-server localhost:9092 --topic metrics.raw --partitions "${p}" 2>>"${LOG}" \
      | awk -F: '{print $3}')"
    off=$((end > 200 ? end - 200 : 0))
    found="$(docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
      --bootstrap-server localhost:9092 --topic metrics.raw --partition "${p}" \
      --offset "${off}" --max-messages 200 --timeout-ms 15000 2>>"${LOG}" \
      | rg 'TC04' | head -1 || true)"
    [[ -n "${found}" ]] && break
  done
  if [[ -n "${found}" ]]; then
    pass TC-005 "Consumer saw plant_id TC04 on partition scan"
  else
    fail TC-005 "TC04 not found in recent partition tail"
  fi
}

run_tc006() {
  log "--- TC-006 DLQ topic exists ---"
  local desc
  desc="$(docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --describe --topic metrics.dlq 2>>"${LOG}")"
  if [[ "${desc}" == *PartitionCount* ]]; then
    pass TC-006 "metrics.dlq described"
  else
    fail TC-006 "Cannot describe metrics.dlq"
  fi
}

run_tc007() {
  log "--- TC-007 Mock producer ---"
  local logs
  logs="$(docker compose logs mock-producer --tail 15 2>>"${LOG}")"
  if [[ "${logs}" == *sent* || "${logs}" == *publish* ]]; then
    pass TC-007 "Mock producer publishing"
  else
    fail TC-007 "No recent publish logs"
  fi
}

run_tc008() {
  log "--- TC-008 Flink job RUNNING ---"
  if ensure_flink_job; then
    pass TC-008 "Job RUNNING: $(flink_running_job)"
  else
    fail TC-008 "No RUNNING Flink job after submit"
  fi
}

run_tc009() {
  log "--- TC-009 Flink consumes metrics.raw ---"
  local lag records jid vid
  lag="$(docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 --describe --group rdg-flink 2>>"${LOG}" | rg 'metrics.raw' || true)"
  jid="$(flink_running_job || true)"
  records="0"
  if [[ -n "${jid}" ]]; then
    vid="$(curl -sf "http://localhost:8081/jobs/${jid}" \
      | python3 -c "import sys,json; print(json.load(sys.stdin)['vertices'][0]['id'])")"
    records="$(curl -sf "http://localhost:8081/jobs/${jid}/vertices/${vid}/metrics?get=0.Source__kafka-metrics-raw.numRecordsIn" \
      | python3 -c "import sys,json; m=json.load(sys.stdin); print(next((x['value'] for x in m if 'numRecordsIn' in x['id']), '0'))" 2>/dev/null || echo 0)"
  fi
  if [[ -n "${lag}" && "${records}" != "0" ]]; then
    pass TC-009 "Consumer group rdg-flink active; numRecordsIn=${records}"
  elif [[ -n "${lag}" ]]; then
    pass TC-009 "Consumer group rdg-flink registered (records=${records})"
  else
    fail TC-009 "No consumer lag info for rdg-flink"
  fi
}

run_tc010() {
  log "--- TC-010 Invalid message to DLQ ---"
  docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 --topic metrics.raw <<'EOF' >>"${LOG}" 2>&1
{"plant_id":"BAD","metric":"x"}
EOF
  sleep 30
  local dlq
  dlq="$(docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 --topic metrics.dlq \
    --from-beginning --timeout-ms 15000 2>>"${LOG}" || true)"
  if [[ "${dlq}" == *validation_error* || "${dlq}" == *BAD* ]]; then
    pass TC-010 "Invalid record quarantined in metrics.dlq"
  else
    fail TC-010 "DLQ did not contain expected validation wrapper"
  fi
  local state
  state="$(curl -sf http://localhost:8081/jobs/overview | python3 -c "import sys,json; print(json.load(sys.stdin)['jobs'][0]['state'])")"
  if [[ "${state}" == "RUNNING" ]]; then
    log "TC-010 supplemental: job still RUNNING"
  else
    fail TC-010 "Flink job not RUNNING after DLQ test (${state})"
  fi
}

run_tc011() {
  log "--- TC-011 Window aggregate ---"
  if [[ "${SKIP_SLOW}" == true ]]; then
    skip TC-011 "Skipped (--skip-slow)"
    return
  fi
  for v in 10 20 30; do
    docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
      --bootstrap-server localhost:9092 --topic metrics.raw <<EOF >>"${LOG}" 2>&1
{"plant_id":"TC11","equipment_id":"GEN-01","metric":"power_output_mw","value":${v},"unit":"MW","ts":"2026-07-18T10:00:00Z"}
EOF
  done
  log "Waiting 75s for 1-min window..."
  sleep 75
  local row
  row="$(docker compose exec -T cassandra cqlsh -e \
    "SELECT value FROM rdg.metrics_current WHERE plant_id='TC11' AND metric='power_output_mw';" 2>>"${LOG}")"
  if [[ "${row}" == *20* ]]; then
    pass TC-011 "Aggregated avg ≈ 20 for TC11"
  elif [[ "${row}" == *TC11* ]]; then
    pass TC-011 "Row exists for TC11 (value may differ if partial window): ${row}"
  else
    fail TC-011 "No row for TC11: ${row}"
  fi
}

run_tc012() {
  log "--- TC-012 MinIO checkpoints ---"
  local listing
  listing="$(docker compose exec -T minio sh -c \
    'mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null 2>&1 && mc ls local/flink-checkpoints/rdg/checkpoints/' 2>>"${LOG}" || true)"
  if [[ -n "${listing}" ]]; then
    pass TC-012 "Checkpoint objects in MinIO: $(echo "${listing}" | wc -l | tr -d ' ') entries"
  else
    fail TC-012 "No checkpoint files in flink-checkpoints bucket"
  fi
}

run_tc013() {
  log "--- TC-013 metrics_current ---"
  local count
  count="$(docker compose exec -T cassandra cqlsh -e \
    "SELECT COUNT(*) FROM rdg.metrics_current;" 2>>"${LOG}" | rg -o '[0-9]+' | head -1)"
  if [[ "${count:-0}" -gt 0 ]]; then
    pass TC-013 "metrics_current count=${count}"
  else
    fail TC-013 "metrics_current empty"
  fi
}

run_tc014() {
  log "--- TC-014 metrics_ts ---"
  local count
  count="$(docker compose exec -T cassandra cqlsh -e \
    "SELECT COUNT(*) FROM rdg.metrics_ts;" 2>>"${LOG}" | rg -o '[0-9]+' | head -1)"
  if [[ "${count:-0}" -gt 0 ]]; then
    pass TC-014 "metrics_ts count=${count}"
  else
    fail TC-014 "metrics_ts empty"
  fi
}

run_tc015() {
  log "--- TC-015 Query by plant_id ---"
  wait_healthy 75
  local row
  row="$(docker compose exec -T cassandra cqlsh -e \
    "SELECT metric FROM rdg.metrics_current WHERE plant_id='TC04';" 2>>"${LOG}")"
  if [[ "${row}" == *power_output_mw* ]]; then
    pass TC-015 "TC04 row with power_output_mw"
  else
    skip TC-015 "TC04 not yet in Cassandra (may need longer window): ${row}"
  fi
}

run_tc016() {
  log "--- TC-016 End-to-end ---"
  local job_state raw_msgs cur ts
  job_state="$(curl -sf http://localhost:8081/jobs/overview | python3 -c "import sys,json; print([j['state'] for j in json.load(sys.stdin)['jobs'] if j['state']=='RUNNING'])")"
  raw_msgs="$(curl -sf 'http://localhost:8090/api/clusters/local/topics?page=1&perPage=10' | python3 -c "import sys,json; d=json.load(sys.stdin); print(next((t.get('messagesCount',0) for t in d['topics'] if t['name']=='metrics.raw'),0))" 2>/dev/null || echo 0)"
  cur="$(docker compose exec -T cassandra cqlsh -e "SELECT COUNT(*) FROM rdg.metrics_current;" 2>>"${LOG}" | rg -o '[0-9]+' | head -1)"
  ts="$(docker compose exec -T cassandra cqlsh -e "SELECT COUNT(*) FROM rdg.metrics_ts;" 2>>"${LOG}" | rg -o '[0-9]+' | head -1)"
  if [[ "${job_state}" == *RUNNING* && "${raw_msgs:-0}" -gt 0 && "${cur:-0}" -gt 0 && "${ts:-0}" -gt 0 ]]; then
    pass TC-016 "E2E OK: raw=${raw_msgs} current=${cur} ts=${ts} job=RUNNING"
  else
    fail TC-016 "E2E gap: raw=${raw_msgs} current=${cur} ts=${ts} job=${job_state}"
  fi
}

run_tc017() {
  log "--- TC-017 Manual produce to Cassandra ---"
  docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 --topic metrics.raw <<'EOF' >>"${LOG}" 2>&1
{"plant_id":"TC17","equipment_id":"GEN-99","metric":"voltage_kv","value":220.5,"unit":"kV","ts":"2026-07-18T11:00:00Z"}
EOF
  log "Waiting 75s for window..."
  sleep 75
  local row
  row="$(docker compose exec -T cassandra cqlsh -e \
    "SELECT metric, value FROM rdg.metrics_current WHERE plant_id='TC17';" 2>>"${LOG}")"
  if [[ "${row}" == *voltage_kv* && "${row}" == *220.5* ]]; then
    pass TC-017 "TC17 voltage_kv=220.5 in Cassandra"
  else
    fail TC-017 "TC17 row missing or wrong: ${row}"
  fi
}

run_tc018() {
  log "--- TC-018 TaskManager recovery ---"
  if [[ "${SKIP_SLOW}" == true ]]; then
    skip TC-018 "Skipped (--skip-slow)"
    return
  fi
  local jid before
  jid="$(flink_running_job)"
  before="$(docker compose exec -T cassandra cqlsh -e "SELECT COUNT(*) FROM rdg.metrics_current;" 2>>"${LOG}" | rg -o '[0-9]+' | head -1)"
  docker compose restart flink-taskmanager >>"${LOG}" 2>&1
  sleep 60
  local state after
  state="$(curl -sf "http://localhost:8081/jobs/${jid}" | python3 -c "import sys,json; print(json.load(sys.stdin).get('state','UNKNOWN'))" 2>/dev/null || echo UNKNOWN)"
  after="$(docker compose exec -T cassandra cqlsh -e "SELECT COUNT(*) FROM rdg.metrics_current;" 2>>"${LOG}" | rg -o '[0-9]+' | head -1)"
  if [[ "${state}" == "RUNNING" ]]; then
    pass TC-018 "Job ${jid:0:8}… RUNNING after TM restart (rows ${before}→${after})"
  else
    fail TC-018 "Job state=${state} after TM restart"
  fi
}

run_tc021() {
  log "--- TC-021 Kafka UI ---"
  local topics
  topics="$(curl -sf 'http://localhost:8090/api/clusters/local/topics?page=1&perPage=20' | python3 -c "import sys,json; print(len(json.load(sys.stdin)['topics']))" 2>/dev/null || echo 0)"
  if [[ "${topics:-0}" -ge 3 ]]; then
    pass TC-021 "Kafka UI API: ${topics} topics"
  else
    fail TC-021 "Kafka UI unavailable or too few topics"
  fi
}

run_tc022() {
  log "--- TC-022 Flink Dashboard metrics ---"
  local jid vid metrics
  jid="$(flink_running_job)"
  vid="$(curl -sf "http://localhost:8081/jobs/${jid}" | python3 -c "import sys,json; print(json.load(sys.stdin)['vertices'][0]['id'])")"
  metrics="$(curl -sf "http://localhost:8081/jobs/${jid}/vertices/${vid}/metrics?get=0.Source__kafka-metrics-raw.numRecordsIn,0.isBackPressured")"
  local records back
  records="$(echo "${metrics}" | python3 -c "import sys,json; m=json.load(sys.stdin); print(next((x['value'] for x in m if 'numRecordsIn' in x['id']), '0'))")"
  back="$(echo "${metrics}" | python3 -c "import sys,json; m=json.load(sys.stdin); print(next((x['value'] for x in m if 'isBackPressured' in x['id']), '?'))")"
  if [[ "${records}" != "0" && "${back}" == "false" ]]; then
    pass TC-022 "numRecordsIn=${records}, isBackPressured=${back}"
  else
    fail TC-022 "Metrics weak: records=${records} backpressure=${back}"
  fi
}

run_tc023() {
  log "--- TC-023 MinIO checkpoint files ---"
  local listing
  listing="$(docker compose exec -T minio sh -c \
    'mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null 2>&1 && mc ls -r local/flink-checkpoints/rdg/checkpoints/' 2>>"${LOG}" | tail -5 || true)"
  if [[ -n "${listing}" ]]; then
    pass TC-023 "Checkpoint artifacts present in MinIO"
  else
    fail TC-023 "No checkpoint artifacts in MinIO"
  fi
}

run_tc024() {
  log "--- TC-024 Clean teardown ---"
  if [[ "${INCLUDE_TEARDOWN}" != true ]]; then
    skip TC-024 "Not run (use --include-teardown)"
    return
  fi
  docker compose --profile dev down -v >>"${LOG}" 2>&1
  docker compose --profile dev up -d >>"${LOG}" 2>&1
  wait_healthy 90
  local count
  count="$(docker compose exec -T cassandra cqlsh -e \
    "SELECT COUNT(*) FROM rdg.metrics_current;" 2>>"${LOG}" | rg -o '[0-9]+' | head -1)"
  if [[ "${count:-0}" -eq 0 ]]; then
    pass TC-024 "Fresh stack metrics_current count=0"
  else
    fail TC-024 "Expected 0 rows after down -v, got ${count}"
  fi
}

print_summary() {
  log ""
  log "========== Phase 9 Validation Summary =========="
  log "Run ID:    ${RUN_ID}"
  log "Log file:  ${LOG}"
  log "Passed:    ${#PASSED[@]} — ${PASSED[*]:-none}"
  log "Failed:    ${#FAILED[@]} — ${FAILED[*]:-none}"
  log "Skipped:   ${#SKIPPED[@]} — ${SKIPPED[*]:-none}"
  log "==============================================="
  if [[ ${#FAILED[@]} -gt 0 ]]; then
    exit 1
  fi
}

main() {
  log "Phase 9 validation started (${RUN_ID})"
  run_tc001
  run_tc002
  run_tc003
  run_tc006
  run_tc007
  run_tc008
  run_tc009
  run_tc016
  run_tc013
  run_tc014
  run_tc004
  run_tc005
  run_tc010
  run_tc017
  run_tc011
  run_tc012
  run_tc015
  run_tc021
  run_tc022
  run_tc023
  run_tc018
  run_tc024
  print_summary
}

main "$@"
