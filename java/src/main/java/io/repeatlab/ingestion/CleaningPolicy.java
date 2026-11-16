package io.repeatlab.ingestion;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

record CleaningPolicy(List<String> serviceCodes) {
    static final String VERSION = "clean-v1";
    static final String SCHEMA = "repeatlab-clean";

    static CleaningPolicy read(Path path) throws IOException {
        JsonNode node = StrictJson.read(path);
        StrictJson.keys(node, "policy_version", "service_codes");
        StrictJson.require(VERSION.equals(StrictJson.text(node.get("policy_version"))), "Unsupported cleaning policy");
        StrictJson.require(node.get("service_codes").isArray(), "service_codes must be an array");
        TreeSet<String> codes = new TreeSet<>((left, right) ->
                Arrays.compare(left.codePoints().toArray(), right.codePoints().toArray()));
        for (JsonNode code : node.get("service_codes")) codes.add(StrictJson.text(code));
        return new CleaningPolicy(List.copyOf(codes));
    }

    Map<String, Object> canonical() { return Json.object("policy_version", VERSION, "service_codes", serviceCodes); }
    String hash() {
        StringBuilder value = new StringBuilder("{\"policy_version\":\"clean-v1\",\"service_codes\":[");
        for (int i = 0; i < serviceCodes.size(); i++) {
            if (i > 0) value.append(',');
            value.append('"');
            serviceCodes.get(i).codePoints().forEach(c -> {
                switch (c) {
                    case '"' -> value.append("\\\"");
                    case '\\' -> value.append("\\\\");
                    case '\b' -> value.append("\\b");
                    case '\f' -> value.append("\\f");
                    case '\n' -> value.append("\\n");
                    case '\r' -> value.append("\\r");
                    case '\t' -> value.append("\\t");
                    default -> { if (c < 32) value.append(String.format("\\u%04x", c)); else value.appendCodePoint(c); }
                }
            });
            value.append('"');
        }
        return Json.hashText(value.append("]}").toString());
    }
    boolean service(String code) { return serviceCodes.contains(code); }

    static boolean missing(String text) {
        return text == null || text.codePoints().allMatch(c ->
                (c >= 9 && c <= 13) || c == 32 || c == 0x85 || c == 0xa0 || c == 0x1680
                || (c >= 0x2000 && c <= 0x200a) || c == 0x2028 || c == 0x2029
                || c == 0x202f || c == 0x205f || c == 0x3000);
    }
}
