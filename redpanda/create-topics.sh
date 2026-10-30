#!/usr/bin/env bash
# Creates the pipeline topics if they don't exist (idempotent). Runs as the one-shot `redpanda-init`
# compose service; also safe to run by hand: docker exec -i redpanda bash < redpanda/create-topics.sh
#   events.plays / events.clicks  published by other teams (contract: contracts/events.md), key tenant_id:user_id.
#                                 LogAppendTime: the broker stamps each message, ClickHouse stores it as cdc_ts.
#   cdc.music                     users/tracks change envelopes from the Flink CDC job, key collection:doc_id
# write.caching=false: dev-container mode turns write caching on, which acks writes before they are written
#   to disk; with a single broker a crash could then lose acknowledged events.
set -euo pipefail
BROKERS=${BROKERS:-redpanda:9092}

create() { # topic partitions [key=value config...]
  local topic=$1 partitions=$2; shift 2
  if rpk topic describe "$topic" -X brokers="$BROKERS" >/dev/null 2>&1; then
    echo "topic $topic exists"
    for kv in "$@"; do rpk topic alter-config "$topic" --set "$kv" -X brokers="$BROKERS"; done
  else
    local cfg=()
    for kv in "$@"; do cfg+=(-c "$kv"); done
    rpk topic create "$topic" -p "$partitions" -r 1 "${cfg[@]}" -X brokers="$BROKERS"
  fi
}

create events.plays 6 message.timestamp.type=LogAppendTime write.caching=false
create events.clicks 6 message.timestamp.type=LogAppendTime write.caching=false
create cdc.music 3 write.caching=false
rpk topic list -X brokers="$BROKERS"
