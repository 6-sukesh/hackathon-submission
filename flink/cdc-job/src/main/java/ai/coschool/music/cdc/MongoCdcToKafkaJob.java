package ai.coschool.music.cdc;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.mongodb.source.MongoDBSource;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * MongoDB (music.users, music.tracks) --Flink CDC--> envelope JSON --Kafka--> topic cdc.music.
 * ClickHouse consumes the topic into music.events_raw (clickhouse/init/02-kafka.sql).
 * Snapshot first (op=r), then change stream (c/u/d). At-least-once; CH ReplacingMergeTree dedups.
 * Operator uids are fixed so the job can be stopped with a savepoint and resumed from it (flink/submit.sh).
 */
public class MongoCdcToKafkaJob {
    public static final String JOB_NAME = "mongo-cdc-to-kafka";

    public static void main(String[] args) throws Exception {
        ParameterTool p = ParameterTool.fromArgs(args);
        String hosts = p.get("mongo.hosts", "mongo:27017");
        String db = p.get("mongo.db", "music");
        String[] colls = p.get("mongo.collections", "users,tracks").split(",");
        String brokers = p.get("kafka.brokers", "redpanda:9092");
        String topic = p.get("kafka.topic", "cdc.music");

        String[] qualified = Arrays.stream(colls).map(String::trim).map(c -> db + "." + c).toArray(String[]::new);

        MongoDBSource<String> source = MongoDBSource.<String>builder()
                .hosts(hosts)
                .databaseList(db)
                .collectionList(qualified)
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        KafkaSink<Tuple2<String, String>> sink = KafkaSink.<Tuple2<String, String>>builder()
                .setBootstrapServers(brokers)
                .setRecordSerializer(new EnvelopeSerializer(topic))
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE) // flushes pending records on every checkpoint
                .setProperty("acks", "all")
                .setProperty("enable.idempotence", "true")              // retries keep per-key order
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // checkpointing is configured cluster-wide (execution.checkpointing.interval: 10s); enforce if missing
        if (!env.getCheckpointConfig().isCheckpointingEnabled()) {
            env.enableCheckpointing(10_000L);
        }

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "mongodb-cdc-source")
                .uid("mongodb-cdc-source").setParallelism(1)
                .flatMap(new ChangeEventToRow(db)).name("to-cdc-envelope")
                .uid("to-cdc-envelope").setParallelism(1)
                .sinkTo(sink).name("kafka-" + topic)
                .uid("kafka-cdc-sink").setParallelism(1);

        env.execute(JOB_NAME);
    }
}
