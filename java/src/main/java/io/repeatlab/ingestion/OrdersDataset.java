package io.repeatlab.ingestion;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.StreamReadConstraints;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/** Strict, exact exchange boundary. No SQL precision restriction belongs here. */
final class OrdersDataset {
    private static final ObjectMapper ROW_JSON = new ObjectMapper(JsonFactory.builder()
            .streamReadConstraints(StreamReadConstraints.builder().maxNumberLength(Integer.MAX_VALUE).build()).build())
            .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    static final List<String> ARTIFACTS = List.of("orders.jsonl", "lineage.jsonl", "duplicates.jsonl", "audit.json");
    static final List<String> CLEAN_FILES = List.of("manifest.json", "clean_events.jsonl", "dispositions.jsonl", "audit.json");
    static final Comparator<String> CODEPOINT = (a, b) -> {
        var left = a.codePoints().iterator();
        var right = b.codePoints().iterator();
        while (left.hasNext() && right.hasNext()) {
            int c = Integer.compare(left.nextInt(), right.nextInt());
            if (c != 0) return c;
        }
        return Boolean.compare(left.hasNext(), right.hasNext());
    };
    static final Pattern DECIMAL = Pattern.compile("[+-]?(?:[0-9]+(?:\\.[0-9]*)?|\\.[0-9]+)(?:[eE][+-]?[0-9]+)?");
    record Order(String invoice, String customer, LocalDateTime time, String kind,
                 long lines, BigInteger quantity, long stocks, String amount) {}
    final List<Order> orders = new ArrayList<>();
    final JsonNode manifest;
    final JsonNode counts;
    final JsonNode sourceCounts;
    final String manifestHash;
    final String cleanHash;
    final String dataStatus;

    OrdersDataset(Path source, Path clean, Path copy) throws IOException {
        Files.createDirectory(copy);
        Files.copy(source.resolve("manifest.json"), copy.resolve("manifest.json"));
        manifestHash = Json.hash(copy.resolve("manifest.json"));
        manifest = StrictJson.read(copy.resolve("manifest.json"));
        StrictJson.keys(manifest, "schema", "schema_version", "policy_version", "worker_protocol_version",
                "status", "data_status", "input", "counts", "source_clean_counts", "artifacts");
        schema(manifest);
        equalText(manifest, "policy_version", "orders-v1");
        StrictJson.integer(manifest.get("worker_protocol_version"), 1, 1);
        equalText(manifest, "status", "completed");
        dataStatus = StrictJson.text(manifest.get("data_status"));
        StrictJson.require(Set.of("ok", "warnings", "quarantined").contains(dataStatus), "Invalid quality");
        JsonNode cleanManifest = StrictJson.read(clean.resolve("manifest.json"));
        cleanHash = Json.hash(clean.resolve("manifest.json"));
        equalText(cleanManifest, "schema", "repeatlab-clean");
        StrictJson.version(cleanManifest);
        equalText(cleanManifest, "status", "completed");
        equalText(cleanManifest, "data_status", dataStatus);
        JsonNode input = manifest.get("input");
        StrictJson.keys(input, "clean_manifest_sha256", "clean_artifacts", "config_sha256");
        StrictJson.require(StrictJson.hash(input.get("clean_manifest_sha256")).equals(cleanHash), "Clean manifest hash mismatch");
        StrictJson.require(input.get("clean_artifacts").equals(cleanManifest.get("artifacts")), "Clean artifact lineage mismatch");
        StrictJson.require(StrictJson.hash(input.get("config_sha256")).equals(StrictJson.hash(cleanManifest.get("config_sha256"))), "Config lineage mismatch");
        StrictJson.keys(cleanManifest.get("artifacts"), "clean_events.jsonl", "dispositions.jsonl", "audit.json");
        for (String name : CLEAN_FILES.subList(1, CLEAN_FILES.size()))
            StrictJson.require(Json.hash(clean.resolve(name)).equals(StrictJson.hash(cleanManifest.get("artifacts").get(name))), "Clean snapshot hash mismatch");
        StrictJson.keys(manifest.get("artifacts"), ARTIFACTS.toArray(String[]::new));
        for (String name : ARTIFACTS) {
            Files.copy(source.resolve(name), copy.resolve(name));
            StrictJson.require(Json.hash(copy.resolve(name)).equals(StrictJson.hash(manifest.get("artifacts").get(name))), "Orders artifact hash mismatch: " + name);
        }
        counts = manifest.get("counts");
        StrictJson.keys(counts, "orders", "sale_orders", "cancellation_orders", "missing_customer_orders",
                "included_product_rows", "excluded_service_rows", "duplicate_groups");
        for (var values = counts.elements(); values.hasNext();) StrictJson.integer(values.next(), 0, Long.MAX_VALUE);
        sourceCounts = manifest.get("source_clean_counts");
        StrictJson.require(sourceCounts.equals(cleanManifest.get("counts")), "Source counts mismatch");
        StrictJson.keys(sourceCounts, "data_rows", "accepted_rows", "accepted_product_rows", "accepted_service_rows",
                "quarantined_rows", "missing_customer_rows", "header_rows", "empty_rows", "warning_rows");
        for (var values = sourceCounts.elements(); values.hasNext();) StrictJson.integer(values.next(), 0, Long.MAX_VALUE);
        long quarantined = count(sourceCounts, "quarantined_rows");
        String quality = quarantined > 0 ? "quarantined" : count(sourceCounts, "warning_rows") > 0 ? "warnings" : "ok";
        StrictJson.require(dataStatus.equals(quality), "Quality accounting mismatch");
        StrictJson.require(Math.addExact(count(sourceCounts, "accepted_rows"), quarantined) == count(sourceCounts, "data_rows"), "Source row accounting mismatch");
        JsonNode audit = StrictJson.read(copy.resolve("audit.json"));
        StrictJson.keys(audit, "schema", "schema_version", "counts", "source_clean_counts");
        schema(audit);
        StrictJson.require(counts.equals(audit.get("counts")) && sourceCounts.equals(audit.get("source_clean_counts")), "Audit mismatch");
        readOrders(copy);
        readLineage(copy, clean);
    }

    private void readOrders(Path copy) throws IOException {
        String previous = null;
        long sales = 0, cancellations = 0, missing = 0, lines = 0;
        for (JsonNode node : rows(copy.resolve("orders.jsonl"))) {
            StrictJson.keys(node, "schema", "schema_version", "order_key", "invoice_id", "customer_id", "event_time", "kind",
                    "line_count", "quantity", "stock_code_count", "total_amount");
            schema(node);
            String invoice = identifier(node.get("invoice_id"));
            equalText(node, "order_key", invoice);
            StrictJson.require(previous == null || CODEPOINT.compare(previous, invoice) < 0, "Orders not uniquely sorted");
            previous = invoice;
            String customer = StrictJson.nullableText(node.get("customer_id"));
            if (customer != null) identifier(node.get("customer_id"));
            String timestamp = StrictJson.text(node.get("event_time"));
            StrictJson.require(timestamp.matches("[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\\.[0-9]{1,6})?"), "Invalid timestamp precision");
            LocalDateTime time;
            try { time = LocalDateTime.parse(timestamp); }
            catch (DateTimeParseException failure) { throw new IOException("Invalid timestamp", failure); }
            StrictJson.require(time.getYear() >= 1 && time.getYear() <= 9999, "Invalid timestamp year");
            String kind = StrictJson.text(node.get("kind"));
            StrictJson.require(Set.of("sale", "cancellation").contains(kind), "Invalid kind");
            JsonNode qty = node.get("quantity");
            StrictJson.require(qty.isIntegralNumber(), "Quantity must be an integer");
            BigInteger quantity = qty.bigIntegerValue();
            String amount = StrictJson.text(node.get("total_amount"));
            StrictJson.require(DECIMAL.matcher(amount).matches(), "Invalid exact monetary string");
            String coefficient = amount.split("[eE]", 2)[0].replace(".", "").replace("+", "").replace("-", "");
            boolean zero = coefficient.chars().allMatch(c -> c == '0');
            int sign = zero ? 0 : amount.startsWith("-") ? -1 : 1;
            int expectedSign = kind.equals("sale") ? 1 : -1;
            StrictJson.require(quantity.signum() == expectedSign && sign == expectedSign, "Invalid quantity/amount sign");
            long lineCount = StrictJson.integer(node.get("line_count"), 1, Long.MAX_VALUE);
            long stocks = StrictJson.integer(node.get("stock_code_count"), 1, lineCount);
            orders.add(new Order(invoice, customer, time, kind, lineCount, quantity, stocks, amount));
            if (kind.equals("sale")) sales++; else cancellations++;
            if (customer == null) missing++;
            lines = Math.addExact(lines, lineCount);
        }
        requireCount("orders", orders.size()); requireCount("sale_orders", sales);
        requireCount("cancellation_orders", cancellations); requireCount("missing_customer_orders", missing);
        requireCount("included_product_rows", lines);
    }

    private void readLineage(Path copy, Path clean) throws IOException {
        Map<String, JsonNode> events = new HashMap<>();
        for (JsonNode node : rows(clean.resolve("clean_events.jsonl")))
            StrictJson.require(events.putIfAbsent(StrictJson.text(node.get("event_id")), node) == null, "Duplicate clean event");
        Map<String, Order> byInvoice = new HashMap<>();
        for (Order order : orders) byInvoice.put(order.invoice(), order);
        Map<String, Long> lines = new HashMap<>();
        Map<String, BigInteger> quantities = new HashMap<>();
        Map<String, Set<String>> stocks = new HashMap<>();
        Set<String> seen = new HashSet<>();
        String previous = null;
        long services = 0;
        for (JsonNode node : rows(copy.resolve("lineage.jsonl"))) {
            StrictJson.keys(node, "schema", "schema_version", "event_id", "invoice_id", "order_key", "disposition");
            schema(node);
            String id = StrictJson.text(node.get("event_id"));
            StrictJson.require(previous == null || CODEPOINT.compare(previous, id) < 0, "Lineage not uniquely sorted");
            previous = id;
            JsonNode event = events.get(id);
            StrictJson.require(event != null && seen.add(id), "Unknown or duplicate lineage event");
            String invoice = StrictJson.text(event.get("invoice_id"));
            equalText(node, "invoice_id", invoice);
            StrictJson.require(event.get("is_service").isBoolean(), "Invalid clean service flag");
            if (event.get("is_service").booleanValue()) {
                equalText(node, "disposition", "excluded_service");
                StrictJson.require(node.get("order_key").isNull(), "Service maps to order");
                services++;
            } else {
                equalText(node, "disposition", "included_product"); equalText(node, "order_key", invoice);
                Order order = byInvoice.get(invoice);
                StrictJson.require(order != null, "Lineage maps to missing order");
                StrictJson.require(Objects.equals(order.customer(), StrictJson.nullableText(event.get("customer_id")))
                        && order.kind().equals(StrictJson.text(event.get("kind")))
                        && order.time().equals(LocalDateTime.parse(StrictJson.text(event.get("event_time")))), "Order/clean metadata mismatch");
                StrictJson.require(event.get("quantity").isIntegralNumber(), "Invalid clean quantity");
                lines.merge(invoice, 1L, Math::addExact);
                quantities.merge(invoice, event.get("quantity").bigIntegerValue(), BigInteger::add);
                stocks.computeIfAbsent(invoice, ignored -> new HashSet<>()).add(StrictJson.text(event.get("stock_code")));
            }
        }
        StrictJson.require(seen.equals(events.keySet()), "Incomplete lineage");
        for (Order order : orders) StrictJson.require(Objects.equals(lines.get(order.invoice()), order.lines())
                && order.quantity().equals(quantities.get(order.invoice()))
                && stocks.get(order.invoice()).size() == order.stocks(), "Order membership counts mismatch");
        requireCount("excluded_service_rows", services);
        StrictJson.require(count(counts, "included_product_rows") == count(sourceCounts, "accepted_product_rows")
                && services == count(sourceCounts, "accepted_service_rows")
                && events.size() == count(sourceCounts, "accepted_rows"), "Source accepted accounting mismatch");
        long groups = 0;
        Set<String> duplicated = new HashSet<>();
        String previousGroup = null;
        for (JsonNode node : rows(copy.resolve("duplicates.jsonl"))) {
            StrictJson.keys(node, "schema", "schema_version", "event_ids"); schema(node);
            JsonNode ids = node.get("event_ids");
            StrictJson.require(ids.isArray() && ids.size() >= 2, "Invalid duplicate group");
            String last = null;
            for (JsonNode item : ids) {
                String id = StrictJson.text(item);
                StrictJson.require(events.containsKey(id) && duplicated.add(id) && (last == null || CODEPOINT.compare(last, id) < 0), "Invalid duplicate membership");
                last = id;
            }
            String first = ids.get(0).textValue();
            StrictJson.require(previousGroup == null || CODEPOINT.compare(previousGroup, first) < 0, "Duplicate groups not sorted");
            previousGroup = first; groups++;
        }
        requireCount("duplicate_groups", groups);
    }

    static List<JsonNode> rows(Path path) throws IOException {
        List<JsonNode> result = new ArrayList<>();
        try (var reader = Files.newBufferedReader(path)) {
            for (String line; (line = reader.readLine()) != null;) {
                JsonNode node = ROW_JSON.readTree(line);
                StrictJson.require(node != null, "Empty JSONL record");
                result.add(node);
            }
        }
        return result;
    }
    static void schema(JsonNode node) throws IOException { StrictJson.version(node); equalText(node, "schema", "repeatlab-orders"); }
    static void equalText(JsonNode node, String field, String expected) throws IOException {
        StrictJson.require(StrictJson.text(node.get(field)).equals(expected), "Invalid " + field);
    }
    static String identifier(JsonNode node) throws IOException {
        String value = StrictJson.text(node);
        StrictJson.require(!value.isEmpty() && value.codePoints().anyMatch(c -> !Character.isWhitespace(c) && !Character.isSpaceChar(c) && c != 0x85), "Empty identifier");
        return value;
    }
    static long count(JsonNode node, String key) throws IOException { return StrictJson.integer(node.get(key), 0, Long.MAX_VALUE); }
    private void requireCount(String key, long expected) throws IOException { StrictJson.require(count(counts, key) == expected, "Count mismatch: " + key); }
}
