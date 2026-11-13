#!/usr/bin/env bash
# Starts, runs and stops Module 31's lab stack: one Kafka broker under the Compose project name
# seatwave-analytics, port 23192 on 127.0.0.1 only. The programs run on the host with plain java.
#
# Usage: scripts/stack.sh <up|down|run|crash|status> [--run] [--yes]
#   default  a dry run: print the plan, change nothing (and need no Docker)
#   --run    do it; asks for confirmation on the terminal first
#   --yes    skip the question
#
#   up      start the broker, wait for its healthcheck, create the four topics
#   down    remove the project's containers, network and volume (down -v), then check nothing is left
#   run     up; start the stream job and the lag probe; publish a tenth of the seeded on-sale (with
#           the stalled shard-37 publisher and 3 unparseable records) at 1,500 records/s; stop the
#           job when it has caught up; archive the topic into lake/; load the day twice; reconcile;
#           then down, on every path
#   crash   the same, but the stream job is killed by its process id (KILL, no clean close) 40 s into
#           the publish, mid-window, and started again 5 s later
#   status  list the project's containers (read-only)
#
# The outputs of run and crash go to build/stack/<action>/; nothing else on the machine changes.
set -euo pipefail

readonly PROJECT="seatwave-analytics"
readonly BOOTSTRAP="127.0.0.1:23192"
readonly WAIT_SECONDS=180
readonly DOCKER_TIMEOUT_SECONDS=10
readonly CONFIRMS_PER_SECOND=150   # a tenth of the on-sale's 1,500
readonly RATE=1500                 # records a second on the wall clock: the on-sale's peak
readonly UNPARSEABLE=3
readonly PROBE_SECONDS=150
readonly CRASH_AFTER_SECONDS=40
readonly RESTART_AFTER_SECONDS=5
readonly ARCHIVER_QUIET_SECONDS=15
readonly SALE_DAY="2026-11-20"

usage() {
  echo "usage: $0 <up|down|run|crash|status> [--run] [--yes]" >&2
  exit 2
}

[[ $# -ge 1 ]] || usage
action="$1"
shift
mode="dry-run"
confirmed="no"
for option in "$@"; do
  case "$option" in
    --run) mode="run" ;;
    --yes) confirmed="yes" ;;
    *) usage ;;
  esac
done
case "$action" in up|down|run|crash|status) ;; *) usage ;; esac

cd "$(dirname "$0")/.."
compose=(docker compose -p "$PROJECT")

echo "plan: $action the Compose project $PROJECT"
case "$action" in
  up) echo "  ${compose[*]} up -d --wait kafka; kafka-init creates 4 topics" ;;
  down) echo "  ${compose[*]} down -v --remove-orphans; check nothing of $PROJECT is left" ;;
  run|crash) echo "  delete lake/ build/warehouse build/streams-state; up; stream job, lag probe, publisher; archiver; load twice; reconcile; down -v (always)" ;;
  status) echo "  ${compose[*]} ps --all" ;;
esac
# The dry run comes before the Docker check: it changes nothing, so it needs nothing.
if [[ "$mode" == "dry-run" && "$action" != "status" ]]; then
  echo "dry run: nothing changed. Add --run to do it."
  exit 0
fi

# Docker answers only if "docker info" prints a server version: it exits 0 with empty output when
# the engine is down. macOS has no timeout command, so the check runs in the background and is
# killed if it hangs.
docker_answers() {
  local answer_file="build/stack/docker-version"
  mkdir -p build/stack
  rm -f "$answer_file"
  docker info --format '{{.ServerVersion}}' > "$answer_file" 2> /dev/null &
  local checker=$!
  local waited
  for (( waited = 0; waited < DOCKER_TIMEOUT_SECONDS; waited++ )); do
    if ! kill -0 "$checker" 2> /dev/null; then
      wait "$checker" || return 1
      grep -q "[0-9]" "$answer_file"
      return $?
    fi
    sleep 1
  done
  kill "$checker" 2> /dev/null || true
  return 1
}
if ! command -v docker > /dev/null || ! docker_answers; then
  echo "Docker is not running (or does not answer within ${DOCKER_TIMEOUT_SECONDS} s); nothing was changed." >&2
  exit 1
fi
if [[ "$action" == "status" ]]; then
  "${compose[@]}" ps --all
  exit 0
fi
if [[ "$confirmed" != "yes" ]]; then
  read -r -p "Go ahead with '$action' for $PROJECT? [y/N] " answer < /dev/tty
  [[ "$answer" == "y" ]] || { echo "not confirmed; nothing changed" >&2; exit 1; }
fi

mkdir -p build/stack
audit() {
  echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) $1" >> build/stack/audit.log
}

up() {
  "${compose[@]}" up -d --wait --wait-timeout "$WAIT_SECONDS" --quiet-pull kafka
  "${compose[@]}" up --quiet-pull --exit-code-from kafka-init kafka-init
  audit "up: started $PROJECT and created its topics"
  echo "up: $PROJECT is healthy; topics created"
}

down() {
  local status=0
  "${compose[@]}" down -v --remove-orphans || status=$?
  audit "down: removed $PROJECT (compose exit $status)"
  local left=""
  left+="$(docker ps -aq --filter "label=com.docker.compose.project=$PROJECT") "
  left+="$(docker network ls -q --filter "label=com.docker.compose.project=$PROJECT") "
  left+="$(docker volume ls -q --filter "label=com.docker.compose.project=$PROJECT")"
  if [[ "$status" -ne 0 || -n "${left// /}" ]]; then
    echo "down failed (compose exit $status) or left behind by $PROJECT: $left" >&2
    return 1
  fi
  echo "nothing of $PROJECT is left (containers, networks, volumes)"
}

# Starts the stream job in the background; its process id goes to $1.
start_stream() {
  java -Xmx1g --enable-native-access=ALL-UNNAMED -cp "$classpath" \
    io.plainround.seatwave.analytics.kafka.StreamCli --bootstrap "$BOOTSTRAP" \
    --state build/streams-state >> "$out/stream.log" 2>&1 &
  stream_pid=$!
  echo "stream job started, pid $stream_pid"
}

stop_stream() {
  if [[ -n "${stream_pid:-}" ]] && kill -0 "$stream_pid" 2> /dev/null; then
    kill -TERM "$stream_pid"
    wait "$stream_pid" || true
  fi
  stream_pid=""
}

java_run() {
  java -Xmx3g --enable-native-access=ALL-UNNAMED -cp "$classpath" "$@"
}

measure() {
  local crash="$1"
  rm -rf lake build/warehouse build/streams-state "$out"
  mkdir -p "$out"
  up
  ./gradlew --quiet stackClasspath
  classpath="$(cat build/stack/runtime.classpath)"
  start_stream
  java_run io.plainround.seatwave.analytics.kafka.LagProbeCli --bootstrap "$BOOTSTRAP" \
    --rate "$RATE" --seconds "$PROBE_SECONDS" > "$out/lag.txt" 2>&1 &
  local probe_pid=$!
  java_run io.plainround.seatwave.analytics.kafka.PublishCli --bootstrap "$BOOTSTRAP" \
    --confirms-per-second "$CONFIRMS_PER_SECOND" --rate "$RATE" --unparseable "$UNPARSEABLE" \
    > "$out/publish.txt" 2>&1 &
  local publish_pid=$!
  if [[ "$crash" == "yes" ]]; then
    sleep "$CRASH_AFTER_SECONDS"
    echo "KILL of the stream job, pid $stream_pid, $CRASH_AFTER_SECONDS s into the publish" | tee "$out/crash.txt"
    kill -KILL "$stream_pid"
    wait "$stream_pid" || true
    sleep "$RESTART_AFTER_SECONDS"
    start_stream
    echo "restarted as pid $stream_pid after $RESTART_AFTER_SECONDS s" | tee -a "$out/crash.txt"
  fi
  wait "$publish_pid"
  wait "$probe_pid"
  stop_stream
  java_run io.plainround.seatwave.analytics.kafka.ArchiverCli --bootstrap "$BOOTSTRAP" \
    --lake lake --quiet-seconds "$ARCHIVER_QUIET_SECONDS" > "$out/archive.txt" 2>&1
  local load
  for load in first second; do
    java_run io.plainround.seatwave.analytics.warehouse.NightlyLoadCli \
      --db build/warehouse/seatwave.duckdb --lake-parent . --day "$SALE_DAY" > "$out/load-$load.txt" 2>&1
  done
  du -sk lake/bookings > "$out/lake-size.txt"
  java_run io.plainround.seatwave.analytics.warehouse.SalesReconciliationCli --bootstrap "$BOOTSTRAP" \
    --db build/warehouse/seatwave.duckdb --lake-parent . > "$out/reconcile.txt" 2>&1
  audit "$action: measured into $out; the reconciliation agreed"
}

case "$action" in
  up) up ;;
  down) down ;;
  run|crash)
    out="build/stack/$action"
    crash="no"
    [[ "$action" == "crash" ]] && crash="yes"
    # On every path, a failed step included (set -e stops at it): stop the job, then down -v.
    trap 'stop_stream || true; down' EXIT
    measure "$crash"
    ;;
esac
