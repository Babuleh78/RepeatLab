package io.repeatlab.ingestion;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

final class CleaningPolicy {
    static final String VERSION = "clean-v1";
    static final String SCHEMA = "repeatlab-clean";
    private final List<String> serviceCodes;
    private final Set<String> serviceIndex;

    private CleaningPolicy(List<String> codes) {
        serviceIndex = Set.copyOf(codes);
        serviceCodes = serviceIndex.stream().sorted(OrdersDataset.CODEPOINT).toList();
    }

    static CleaningPolicy read(Path path) throws IOException {
        return parse(StrictJson.read(path.toRealPath()), false);
    }

    static CleaningPolicy fromManifest(JsonNode node) throws IOException {
        return parse(node, true);
    }

    private static CleaningPolicy parse(JsonNode node, boolean requireCanonical) throws IOException {
        StrictJson.keys(node, "policy_version", "service_codes");
        StrictJson.require(VERSION.equals(StrictJson.text(node.get("policy_version"))), "Unsupported cleaning policy");
        StrictJson.require(node.get("service_codes").isArray(), "service_codes must be an array");
        List<String> codes = new ArrayList<>();
        for (JsonNode code : node.get("service_codes")) codes.add(StrictJson.text(code));
        var policy = new CleaningPolicy(codes);
        if (requireCanonical) StrictJson.require(codes.equals(policy.serviceCodes), "Clean config is not canonical");
        return policy;
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
    boolean service(String code) { return serviceIndex.contains(code); }

    static boolean missing(String text) {
        return text == null || text.codePoints().allMatch(c ->
                (c >= 9 && c <= 13) || c == 32 || c == 0x85 || c == 0xa0 || c == 0x1680
                || (c >= 0x2000 && c <= 0x200a) || c == 0x2028 || c == 0x2029
                || c == 0x202f || c == 0x205f || c == 0x3000);
    }
}
