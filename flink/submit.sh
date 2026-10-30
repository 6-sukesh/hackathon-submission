#!/usr/bin/env bash
# Build the Mongo->Kafka CDC job (in Docker) and (re)submit it to the Flink cluster.
# A running job is stopped with a savepoint and the new one resumes from it, so the change stream
# continues where it left off (no new snapshot). With no running job, or with --fresh, it starts clean:
# snapshot of users/tracks, then the change stream.
# Usage: flink/submit.sh [--no-build] [--fresh]
set -euo pipefail
cd "$(dirname "$0")"
JOB_NAME="mongo-cdc-to-kafka"
JAR="mongo-cdc-kafka.jar"
SAVEPOINTS="file:///flink-state/savepoints"   # ./flink/state, shared by jobmanager and taskmanager

BUILD=1; FRESH=0
for arg in "$@"; do
  case "$arg" in
    --no-build) BUILD=0 ;;
    --fresh) FRESH=1 ;;
    *) echo "unknown option $arg (usage: $0 [--no-build] [--fresh])" >&2; exit 2 ;;
  esac
done

if [[ $BUILD == 1 ]]; then
  echo ">> building jar"
  docker volume create flink-cdc-m2 >/dev/null
  docker run --rm -v "$PWD/cdc-job":/build -v flink-cdc-m2:/root/.m2 -w /build \
    maven:3.9-eclipse-temurin-11 mvn -q -B -DskipTests package
  mkdir -p usrlib
  cp cdc-job/target/$JAR usrlib/$JAR
fi

echo ">> ensuring flink cluster is up"
(cd .. && docker compose up -d flink-jobmanager flink-taskmanager)
for i in $(seq 1 60); do
  curl -sf localhost:8081/overview >/dev/null && break; sleep 2
done

RUNNING=$(curl -s localhost:8081/jobs/overview | python3 -c "
import json,sys
for j in json.load(sys.stdin)['jobs']:
    if j['name']=='$JOB_NAME' and j['state'] in ('RUNNING','RESTARTING','CREATED','INITIALIZING'): print(j['jid'])")

RESUME=()
for id in $RUNNING; do
  if [[ $FRESH == 1 ]]; then
    echo ">> --fresh: cancelling $id"
    docker exec flink-jobmanager flink cancel "$id"
  else
    echo ">> stopping $id with a savepoint"
    out=$(docker exec flink-jobmanager flink stop --savepointPath "$SAVEPOINTS" "$id")
    echo "$out"
    sp=$(sed -n 's/.*Savepoint completed\. Path: \(.*\)$/\1/p' <<<"$out" | tail -1)
    [[ -n "$sp" ]] || { echo "!! no savepoint path in flink stop output" >&2; exit 1; }
    RESUME=(-s "$sp")
  fi
done

if [[ ${#RESUME[@]} -gt 0 ]]; then
  echo ">> submitting, resuming from ${RESUME[1]}"
else
  echo ">> submitting (clean start: snapshot, then change stream)"
fi
docker exec flink-jobmanager flink run -d ${RESUME[@]+"${RESUME[@]}"} /opt/flink/usrlib/$JAR
