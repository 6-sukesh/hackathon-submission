package ai.coschool.music.cdc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Iterator;
import java.util.Map;

/** Converts MongoDB Extended JSON (canonical or relaxed) into plain JSON. */
public final class ExtJson {
    private static final JsonNodeFactory F = JsonNodeFactory.instance;

    private ExtJson() {}

    public static JsonNode normalize(JsonNode n) {
        if (n == null || n.isNull() || n.isMissingNode()) return F.nullNode();
        if (n.isArray()) {
            ArrayNode out = F.arrayNode();
            for (JsonNode e : n) out.add(normalize(e));
            return out;
        }
        if (!n.isObject()) return n;

        if (n.size() == 1 || (n.size() == 2 && n.has("$regularExpression"))) {
            String k = n.fieldNames().next();
            JsonNode v = n.get(k);
            switch (k) {
                case "$oid": return F.textNode(v.asText());
                case "$date": return dateToMillis(v);
                case "$numberInt":
                case "$numberLong": return integral(v);
                case "$numberDouble": return dbl(v);
                case "$numberDecimal": return decimal(v);
                case "$timestamp": return F.numberNode(v.path("t").asLong() * 1000L);
                case "$binary": return F.textNode(v.isObject() ? v.path("base64").asText() : v.asText());
                case "$symbol": return F.textNode(v.asText());
                case "$uuid": return F.textNode(v.asText());
                case "$undefined": return F.nullNode();
                case "$minKey":
                case "$maxKey": return F.nullNode();
                default: break;
            }
        }
        ObjectNode out = F.objectNode();
        Iterator<Map.Entry<String, JsonNode>> it = n.fields();
        while (it.hasNext()) {
            Map.Entry<String, JsonNode> e = it.next();
            out.set(e.getKey(), normalize(e.getValue()));
        }
        return out;
    }

    static JsonNode dateToMillis(JsonNode v) {
        if (v.isNumber()) return F.numberNode(v.asLong());
        if (v.isObject() && v.has("$numberLong")) return F.numberNode(Long.parseLong(v.get("$numberLong").asText()));
        if (v.isTextual()) {
            String s = v.asText();
            try {
                return F.numberNode(Instant.parse(s).toEpochMilli());
            } catch (Exception ignore) {
                try {
                    return F.numberNode(OffsetDateTime.parse(s).toInstant().toEpochMilli());
                } catch (Exception ignore2) {
                    try { return F.numberNode(Long.parseLong(s)); } catch (Exception e3) { return F.textNode(s); }
                }
            }
        }
        return normalize(v);
    }

    private static JsonNode integral(JsonNode v) {
        if (v.isNumber()) return F.numberNode(v.asLong());
        try { return F.numberNode(Long.parseLong(v.asText())); } catch (NumberFormatException e) { return F.textNode(v.asText()); }
    }

    private static JsonNode dbl(JsonNode v) {
        if (v.isNumber()) return F.numberNode(v.asDouble());
        try {
            double d = Double.parseDouble(v.asText());
            return (Double.isNaN(d) || Double.isInfinite(d)) ? F.nullNode() : F.numberNode(d);
        } catch (NumberFormatException e) { return F.nullNode(); }
    }

    private static JsonNode decimal(JsonNode v) {
        try { return F.numberNode(new BigDecimal(v.asText())); } catch (NumberFormatException e) { return F.nullNode(); }
    }
}
