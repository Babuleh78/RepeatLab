package io.repeatlab.ingestion;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

final class StrictJson {
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    private StrictJson() {}

    static JsonNode read(Path path) throws IOException { return parse(Files.readString(path)); }
    static JsonNode parse(String value) throws IOException {
        JsonNode node = MAPPER.readTree(value);
        require(node != null, "Empty JSON");
        return node;
    }
    static void require(boolean valid, String message) throws IOException {
        if (!valid) throw new IOException(message);
    }
    static void keys(JsonNode node, String... expected) throws IOException {
        require(node != null && node.isObject(), "Expected JSON object");
        Set<String> actual = new HashSet<>();
        node.fieldNames().forEachRemaining(actual::add);
        require(actual.equals(Set.of(expected)), "Unexpected or missing object fields");
    }
    static String text(JsonNode node) throws IOException {
        require(node != null && node.isTextual(), "Expected string");
        String value = node.textValue();
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (Character.isHighSurrogate(c)) {
                require(i + 1 < value.length() && Character.isLowSurrogate(value.charAt(i + 1)), "Invalid Unicode");
                i++;
            } else require(!Character.isLowSurrogate(c), "Invalid Unicode");
        }
        return value;
    }
    static String nullableText(JsonNode node) throws IOException {
        require(node != null, "Missing nullable field");
        return node.isNull() ? null : text(node);
    }
    static long integer(JsonNode node, long minimum, long maximum) throws IOException {
        require(node != null && node.isIntegralNumber() && node.canConvertToLong(), "Expected integer");
        long value = node.longValue();
        require(value >= minimum && value <= maximum, "Integer outside bounds");
        return value;
    }
    static String hash(JsonNode node) throws IOException {
        String value = text(node);
        require(value.matches("[0-9a-f]{64}"), "Invalid SHA-256");
        return value;
    }
    static void version(JsonNode node) throws IOException { integer(node.get("schema_version"), 1, 1); }
}
