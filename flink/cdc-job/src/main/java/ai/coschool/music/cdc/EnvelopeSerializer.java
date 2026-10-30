package ai.coschool.music.cdc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;

/** (key, envelope JSON) -> Kafka record on one topic, both as UTF-8. */
public class EnvelopeSerializer implements KafkaRecordSerializationSchema<Tuple2<String, String>> {
    private static final long serialVersionUID = 1L;

    private final String topic;

    public EnvelopeSerializer(String topic) { this.topic = topic; }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(Tuple2<String, String> row, KafkaSinkContext ctx, Long timestamp) {
        return new ProducerRecord<>(topic,
                row.f0.getBytes(StandardCharsets.UTF_8),
                row.f1.getBytes(StandardCharsets.UTF_8));
    }
}
