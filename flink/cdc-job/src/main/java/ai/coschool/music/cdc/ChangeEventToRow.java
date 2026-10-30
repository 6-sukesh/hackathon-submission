package ai.coschool.music.cdc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Debezium-style Mongo change record (JSON) -> (Kafka key, envelope JSON) for topic cdc.music.
 * The envelope is one music.events_raw row; key = collection:doc_id keeps each document's changes ordered.
 */
public class ChangeEventToRow extends RichFlatMapFunction<String, Tuple2<String, String>> {
    private static final Logger LOG = LoggerFactory.getLogger(ChangeEventToRow.class);
    private static final long serialVersionUID = 1L;
    private static final ObjectMapper M = new ObjectMapper();

    private final String sourceDb;

    public ChangeEventToRow(String sourceDb) { this.sourceDb = sourceDb; }

    @Override
    public void flatMap(String value, Collector<Tuple2<String, String>> out) {
        try {
            Tuple2<String, String> row = convert(value);
            if (row != null) out.collect(row);
        } catch (Exception e) {
            LOG.error("Skipping unparseable change record: {}", value, e);
        }
    }

    Tuple2<String, String> convert(String value) throws Exception {
        JsonNode rec = M.readTree(value);
        String opType = rec.path("operationType").asText("");
        JsonNode source = rec.path("source");
        boolean snapshot = "true".equalsIgnoreCase(source.path("snapshot").asText("false"));

        String op;
        switch (opType) {
            case "insert": op = snapshot ? "r" : "c"; break;
            case "update":
            case "replace": op = "u"; break;
            case "delete": op = "d"; break;
            default: return null; // drop/rename/invalidate etc.
        }

        String coll = rec.path("ns").path("coll").asText("");
        JsonNode docKey = ExtJson.normalize(asJson(rec.get("documentKey")));
        JsonNode id = docKey.path("_id");
        JsonNode full = asJson(rec.get("fullDocument"));

        String raw;
        if ("d".equals(op)) {
            raw = "{}";
        } else {
            if (full == null || full.isNull() || full.isMissingNode()) {
                // update whose doc was deleted before lookup — the delete event follows
                LOG.warn("No fullDocument for {} on {}.{} id={}, skipping", opType, sourceDb, coll, id);
                return null;
            }
            JsonNode norm = ExtJson.normalize(full);
            raw = M.writeValueAsString(norm);
            if (id.isMissingNode()) id = norm.path("_id");
        }
        String docId = id.isMissingNode() || id.isNull() ? "" : (id.isTextual() ? id.asText() : M.writeValueAsString(id));

        long ts = source.path("ts_ms").asLong(0L);
        if (ts <= 0L) {
            JsonNode ct = asJson(rec.get("clusterTime"));
            if (ct != null && ct.has("$timestamp")) ts = ct.path("$timestamp").path("t").asLong() * 1000L;
        }
        if (snapshot || ts <= 0L) ts = System.currentTimeMillis();

        ObjectNode row = M.createObjectNode();
        row.put("source_db", sourceDb);
        row.put("collection", coll);
        row.put("op", op);
        row.put("doc_id", docId);
        row.put("raw", raw);
        row.put("cdc_ts_ms", ts);
        return Tuple2.of(coll + ":" + docId, M.writeValueAsString(row));
    }

    /** Fields like fullDocument/documentKey arrive as JSON strings; parse them, pass objects through. */
    private static JsonNode asJson(JsonNode n) throws Exception {
        if (n == null || n.isNull()) return null;
        if (n.isTextual()) {
            String s = n.asText();
            if (s.isEmpty()) return null;
            return M.readTree(s);
        }
        return n;
    }
}
