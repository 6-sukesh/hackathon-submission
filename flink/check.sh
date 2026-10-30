#!/usr/bin/env bash
# Quick pipeline health check: Flink job state, Mongo vs ClickHouse (FINAL) counts for users/tracks,
# Kafka topic offsets + ClickHouse consumer-group lag, plays/clicks counts, dead-letter count.
CH() { curl -s -u default:clickhouse localhost:8123 --data-binary "$1"; }
RPK() { docker exec redpanda rpk "$@"; }

echo "── flink"
curl -s localhost:8081/jobs/overview | python3 -c "import json,sys;[print('job',j['name'],j['state'],j['jid']) for j in json.load(sys.stdin)['jobs']]"

echo "── mongo vs clickhouse (users/tracks via cdc.music)"
for c in users tracks; do
  m=$(docker exec mongo mongosh --quiet music --eval "db.$c.countDocuments()")
  echo "$c mongo=$m clickhouse=$(CH "SELECT count() FROM music.$c FINAL WHERE is_deleted=0")"
done

echo "── kafka topics (sum of partition high watermarks = messages ever written)"
for t in events.plays events.clicks cdc.music; do
  echo "$t $(RPK topic describe "$t" -p | awk 'NR>1 {sum+=$NF} END {print "offsets=" sum+0}')"
done

echo "── clickhouse consumer groups (lag = messages not yet in ClickHouse)"
for g in clickhouse-events clickhouse-cdc; do
  RPK group describe "$g" | awk -v g="$g" '$1=="STATE" {s=$2} $1=="TOTAL-LAG" {l=$2} END {print g, "state=" s, "lag=" l}'
done

echo "── clickhouse"
for c in plays clicks; do echo "$c $(CH "SELECT count() FROM music.$c FINAL")"; done
echo "ingest_errors $(CH "SELECT count() FROM music.ingest_errors")"
CH "SELECT source_db, collection, op, count() FROM music.events_raw GROUP BY 1,2,3 ORDER BY 1,2,3 FORMAT PrettyCompactMonoBlock"
