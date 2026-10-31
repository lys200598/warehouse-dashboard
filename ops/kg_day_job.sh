#!/usr/bin/env bash
# Launch with nohup only after the final short checks and writer restart.
set -uo pipefail
cd /data/workspace/wz/warehouse-integration-on-lys
day_run_id=${1:-wz-test-day-20261030-overnight}
day_output_dir=${2:-/tmp/wz-kg-evidence/full-day-run-20261030-overnight}
writer_pid_file=/tmp/wz-kg-evidence/writer.pid
if [[ -e "$day_output_dir" ]]; then
  echo "Existing day output preserved: $day_output_dir" >&2
  exit 1
fi
if [[ ! -r "$writer_pid_file" ]]; then
  echo 'Cannot capture the owned global writer PID' >&2
  exit 1
fi
day_writer_pid=$(< "$writer_pid_file")
if [[ ! "$day_writer_pid" =~ ^[0-9]+$ ]]; then
  echo 'Cannot capture the owned global writer PID' >&2
  exit 1
fi
day_child_pid=''
finish_day_job() {
  local day_result=$?
  trap - EXIT
  bash ops/kg_dev_env.sh python ops/kg_long_run.py stop-writer \
    --expected-pid "$day_writer_pid" --pid-file "$writer_pid_file"
  local day_stop_result=$?
  if (( day_result == 0 && day_stop_result != 0 )); then
    day_result=$day_stop_result
  fi
  echo "Day job finished: run=$day_run_id exit=$day_result writer_snapshot=$day_writer_pid"
  exit "$day_result"
}
interrupt_day_job() {
  if [[ -n "$day_child_pid" ]] && kill -0 "$day_child_pid" 2>/dev/null; then
    kill -TERM "$day_child_pid"
  fi
}
trap finish_day_job EXIT
trap interrupt_day_job TERM INT
echo "Day job starting: run=$day_run_id output=$day_output_dir writer_snapshot=$day_writer_pid"
bash ops/kg_dev_env.sh python ops/kg_long_run.py run \
  --scene data/layouts/expansion-candidates/medium/layout.json \
  --dataset /tmp/wz-kg-evidence/full-day-dataset \
  --config config/wheeled-medium-100.json \
  --output "$day_output_dir" --run-id "$day_run_id" \
  --ray-address 127.0.0.1:27380 --robot-context-interval 10 \
  --kg-state-source memory --robot-graph-backlog "${ROBOT_GRAPH_BACKLOG:-120}" \
  --global-writer-url http://127.0.0.1:14591 --cleanup &
day_child_pid=$!
wait "$day_child_pid"
day_result=$?
# A trapped signal interrupts wait before the Python finally block has finished.
if kill -0 "$day_child_pid" 2>/dev/null; then
  wait "$day_child_pid"
  day_result=$?
fi
exit "$day_result"
