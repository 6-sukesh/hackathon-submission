import { Injectable } from '@nestjs/common';
import { ClickHouseService } from '../clickhouse/clickhouse.service';
import { COLLECTIONS, PipelineLagQueryDto, SchemaFieldsQueryDto } from './dto/ops-query.dto';
import { OPS_SQL as SQL } from './ops.sql';

type Row = Record<string, any>;

/** Max raw documents scanned by /ops/schema-fields. */
const SCHEMA_SAMPLE_ROWS = 200_000;

@Injectable()
export class OpsService {
  constructor(private readonly ch: ClickHouseService) {}

  async pipelineLag(tenant: string, q: PipelineLagQueryDto) {
    const rows = await this.ch.query<Row>(SQL.pipelineLag, { tenant, minutes: q.minutes });
    const collections = Object.fromEntries(
      COLLECTIONS.map((c) => {
        const r = rows.find((x) => x.collection === c);
        if (!r) return [c, { events: 0, kafka_to_clickhouse_ms: null, producer_to_kafka_ms: null, last_ingested_at: null }];
        return [
          c,
          {
            events: r.events,
            first_ingested_at: r.first_ingested_at,
            last_ingested_at: r.last_ingested_at,
            kafka_to_clickhouse_ms: { p50: r.kafka_q[0], p95: r.kafka_q[1], max: r.kafka_max },
            producer_to_kafka_ms: { p50: r.producer_q[0], p95: r.producer_q[1], max: r.producer_max },
            // Producers that buffer (offline sync, retries) publish long after received_at; keep them out of the
            // streaming figure so it reflects transport latency.
            producer_to_kafka_over_1h: r.producer_over_1h,
            producer_to_kafka_streaming_ms:
              r.events > r.producer_over_1h
                ? { p50: r.producer_streaming_q[0], p95: r.producer_streaming_q[1], max: r.producer_streaming_max }
                : null,
          },
        ];
      }),
    );
    return {
      tenant_id: tenant,
      minutes: q.minutes,
      window_column: 'ingested_at',
      definitions: {
        kafka_to_clickhouse_ms:
          'ingested_at - cdc_ts (Kafka append -> ClickHouse). ingested_at is fixed at the start of each consume block, so small negative values are normal',
        producer_to_kafka_ms:
          'cdc_ts - received_at (producer receive -> Kafka append); producer_to_kafka_streaming_ms excludes rows over 1h (buffered publishes)',
      },
      collections,
    };
  }

  async schemaFields(tenant: string, q: SchemaFieldsQueryDto) {
    const p = { tenant, days: q.days, table: q.collection, sample: SCHEMA_SAMPLE_ROWS };
    const [fields, stats] = await Promise.all([
      this.ch.query<Row>(SQL.schemaFields[q.collection], p),
      this.ch.query<Row>(SQL.sampleStats[q.collection], p),
    ]);
    const sampled = stats.reduce((n, s) => n + s.events, 0);
    const byVersion = new Map(stats.map((s) => [s.schema_version, s.events]));
    return {
      tenant_id: tenant,
      collection: q.collection,
      days: q.days,
      sampled_events: sampled,
      sampled_by_schema_version: stats,
      // Keys in raw with no typed column: candidates to promote (ALTER TABLE ADD COLUMN + update the MV).
      candidates: fields.map((f) => {
        const versionEvents = f.schema_versions.reduce((n: number, v: number) => n + (byVersion.get(v) ?? 0), 0);
        return {
          ...f,
          share_pct: sampled ? Math.round((1000 * f.occurrences) / sampled) / 10 : 0,
          share_of_versions_pct: versionEvents ? Math.round((1000 * f.occurrences) / versionEvents) / 10 : 0,
        };
      }),
    };
  }
}
