package io.repeatlab.ingestion;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.StreamReadConstraints;
import java.io.BufferedReader;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
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
    private record Lineage(String invoice, boolean service) {}
    final List<Order> orders = new ArrayList<>();
    final JsonNode manifest;
    final JsonNode counts;
    final JsonNode sourceCounts;
    final String manifestHash;
    final String cleanHash;
    final String dataStatus;

    OrdersDataset(Path source, Path copy) throws IOException {
        this(source, null, copy);
    }

    OrdersDataset(Path source, Path clean, Path copy) throws IOException {
        this(source, clean, copy, null);
    }

    OrdersDataset(Path source, Path clean, Path copy, Map<String, Set<String>> products) throws IOException {
        StrictJson.require(products == null || clean != null && products.isEmpty(),
                "Product memberships require a clean snapshot and an empty destination");
        TransactionalOutput.privateDirectory(copy);
        ArtifactFiles.copy(source.resolve("manifest.json"), copy.resolve("manifest.json"));
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
        JsonNode input = manifest.get("input");
        StrictJson.keys(input, "clean_manifest_sha256", "clean_artifacts", "config_sha256");
        cleanHash = StrictJson.hash(input.get("clean_manifest_sha256"));
        StrictJson.hash(input.get("config_sha256"));
        JsonNode cleanArtifacts = input.get("clean_artifacts");
        StrictJson.keys(cleanArtifacts, CleanWriter.ARTIFACTS.toArray(String[]::new));
        for (String name : CleanWriter.ARTIFACTS) StrictJson.hash(cleanArtifacts.get(name));
        StrictJson.keys(manifest.get("artifacts"), ARTIFACTS.toArray(String[]::new));
        for (String name : ARTIFACTS) {
            ArtifactFiles.copy(source.resolve(name), copy.resolve(name));
            StrictJson.require(Json.hash(copy.resolve(name)).equals(StrictJson.hash(manifest.get("artifacts").get(name))), "Orders artifact hash mismatch: " + name);
        }
        counts = manifest.get("counts");
        StrictJson.keys(counts, "orders", "sale_orders", "cancellation_orders", "missing_customer_orders",
                "included_product_rows", "excluded_service_rows", "duplicate_groups");
        for (var values = counts.elements(); values.hasNext();) StrictJson.integer(values.next(), 0, Long.MAX_VALUE);
        sourceCounts = manifest.get("source_clean_counts");
        StrictJson.keys(sourceCounts, "data_rows", "accepted_rows", "accepted_product_rows", "accepted_service_rows",
                "quarantined_rows", "missing_customer_rows", "header_rows", "empty_rows", "warning_rows");
        for (var values = sourceCounts.elements(); values.hasNext();) StrictJson.integer(values.next(), 0, Long.MAX_VALUE);
        long quarantined = count(sourceCounts, "quarantined_rows");
        long sourceRows = count(sourceCounts, "data_rows");
        long warnings = count(sourceCounts, "warning_rows");
        String quality = quarantined > 0 ? "quarantined" : warnings > 0 ? "warnings" : "ok";
        StrictJson.require(dataStatus.equals(quality), "Quality accounting mismatch");
        StrictJson.require(count(sourceCounts, "accepted_rows") == sourceRows - quarantined, "Source row accounting mismatch");
        StrictJson.require(count(sourceCounts, "missing_customer_rows") <= warnings && warnings <= sourceRows,
                "Source warning accounting mismatch");
        JsonNode audit = StrictJson.read(copy.resolve("audit.json"));
        StrictJson.keys(audit, "schema", "schema_version", "counts", "source_clean_counts");
        schema(audit);
        StrictJson.require(counts.equals(audit.get("counts")) && sourceCounts.equals(audit.get("source_clean_counts")), "Audit mismatch");
        CleaningPolicy policy = clean == null ? null : verifyCleanMetadata(clean, input);
        readOrders(copy);
        readLineage(copy, clean == null ? null : readCleanEvents(clean, policy), products);
    }

    private CleaningPolicy verifyCleanMetadata(Path clean, JsonNode input) throws IOException {
        Path manifestPath = clean.resolve("manifest.json");
        StrictJson.require(Json.hash(manifestPath).equals(cleanHash), "Clean manifest hash mismatch");
        JsonNode cleanManifest = StrictJson.read(manifestPath);
        equalText(cleanManifest, "schema", "repeatlab-clean");
        StrictJson.version(cleanManifest);
        equalText(cleanManifest, "status", "completed");
        equalText(cleanManifest, "policy_version", CleaningPolicy.VERSION);
        equalText(cleanManifest, "data_status", dataStatus);
        StrictJson.require(sourceCounts.equals(cleanManifest.get("counts")), "Source counts mismatch");
        StrictJson.require(input.get("config_sha256").textValue().equals(StrictJson.hash(cleanManifest.get("config_sha256"))),
                "Config lineage mismatch");
        CleaningPolicy policy = CleaningPolicy.fromManifest(cleanManifest.get("config"));
        StrictJson.require(policy.hash().equals(input.get("config_sha256").textValue()), "Clean config checksum mismatch");
        JsonNode artifacts = input.get("clean_artifacts");
        StrictJson.require(artifacts.equals(cleanManifest.get("artifacts")), "Clean artifact lineage mismatch");
        for (String name : CleanWriter.ARTIFACTS)
            StrictJson.require(Json.hash(clean.resolve(name)).equals(artifacts.get(name).textValue()), "Clean snapshot hash mismatch");
        return policy;
    }

    private void readOrders(Path copy) throws IOException {
        String previous = null;
        long sales = 0, cancellations = 0, missing = 0, lines = 0;
        try (var reader = ArtifactFiles.reader(copy.resolve("orders.jsonl"))) {
            for (JsonNode node; (node = nextRow(reader)) != null;) {
                StrictJson.keys(node, "schema", "schema_version", "order_key", "invoice_id", "customer_id", "event_time", "kind",
                        "line_count", "quantity", "stock_code_count", "total_amount");
                schema(node);
                String invoice = identifier(node.get("invoice_id"));
                equalText(node, "order_key", invoice);
                StrictJson.require(previous == null || CODEPOINT.compare(previous, invoice) < 0, "Orders not uniquely sorted");
                previous = invoice;
                String customer = StrictJson.nullableText(node.get("customer_id"));
                if (customer != null) identifier(node.get("customer_id"));
                LocalDateTime time = CleanOrderLine.timestamp(StrictJson.text(node.get("event_time")));
                String kind = StrictJson.text(node.get("kind"));
                boolean cancellation = invoice.startsWith("c") || invoice.startsWith("C");
                StrictJson.require(kind.equals(cancellation ? "cancellation" : "sale"), "Order kind does not match invoice prefix");
                JsonNode qty = node.get("quantity");
                StrictJson.require(qty.isIntegralNumber(), "Quantity must be an integer");
                BigInteger quantity = qty.bigIntegerValue();
                String amount = StrictJson.text(node.get("total_amount"));
                int sign = ExactDecimal.parse(amount).coefficient().signum();
                int expectedSign = cancellation ? -1 : 1;
                StrictJson.require(quantity.signum() == expectedSign && sign == expectedSign, "Invalid quantity/amount sign");
                long lineCount = StrictJson.integer(node.get("line_count"), 1, Long.MAX_VALUE);
                StrictJson.require(quantity.abs().compareTo(BigInteger.valueOf(lineCount)) >= 0,
                        "Order quantity magnitude is less than its product line count");
                long stocks = StrictJson.integer(node.get("stock_code_count"), 1, lineCount);
                orders.add(new Order(invoice, customer, time, kind, lineCount, quantity, stocks, amount));
                if (cancellation) cancellations++; else sales++;
                if (customer == null) missing++;
                lines = Math.addExact(lines, lineCount);
            }
        }
        requireCount("orders", orders.size()); requireCount("sale_orders", sales);
        requireCount("cancellation_orders", cancellations); requireCount("missing_customer_orders", missing);
        requireCount("included_product_rows", lines);
    }

    private static Map<String, CleanOrderLine> readCleanEvents(Path clean, CleaningPolicy policy) throws IOException {
        Map<String, CleanOrderLine> events = new HashMap<>();
        try (var reader = ArtifactFiles.reader(clean.resolve("clean_events.jsonl"))) {
            for (JsonNode node; (node = nextRow(reader)) != null;) {
                String id = StrictJson.text(node.get("event_id"));
                StrictJson.require(events.putIfAbsent(id, CleanOrderLine.read(node, policy)) == null, "Duplicate clean event");
            }
        }
        return events;
    }

    private void readLineage(Path copy, Map<String, CleanOrderLine> events, Map<String, Set<String>> products) throws IOException {
        Map<String, Order> byInvoice = new HashMap<>();
        for (Order order : orders) byInvoice.put(order.invoice(), order);
        Map<String, Long> lines = new HashMap<>();
        Map<String, BigInteger> quantities = new HashMap<>();
        Map<String, ExactDecimal.Sum> amounts = new HashMap<>();
        Map<String, Set<String>> stocks = new HashMap<>();
        Map<String, Lineage> lineage = new HashMap<>();
        String previous = null;
        long services = 0, serviceOnlyRows = 0, knownMissingCustomerRows = 0;
        try (var reader = ArtifactFiles.reader(copy.resolve("lineage.jsonl"))) {
            for (JsonNode node; (node = nextRow(reader)) != null;) {
                StrictJson.keys(node, "schema", "schema_version", "event_id", "invoice_id", "order_key", "disposition");
                schema(node);
                // Analyze checks worker-produced IDs against the clean snapshot passed to the worker.
                // Commands that open a persisted orders dataset also check the SHA-256 format.
                String id = events == null || products != null
                        ? StrictJson.hash(node.get("event_id")) : StrictJson.text(node.get("event_id"));
                StrictJson.require(previous == null || CODEPOINT.compare(previous, id) < 0, "Lineage not uniquely sorted");
                previous = id;
                String invoice = identifier(node.get("invoice_id"));
                boolean service = switch (StrictJson.text(node.get("disposition"))) {
                    case "included_product" -> false;
                    case "excluded_service" -> true;
                    default -> throw new IOException("Invalid lineage disposition");
                };
                CleanOrderLine event = events == null ? null : events.get(id);
                if (events != null) {
                    StrictJson.require(event != null && event.invoice().equals(invoice) && event.service() == service,
                            "Lineage does not match a clean event");
                }
                lineage.put(id, new Lineage(invoice, service));
                Order order = byInvoice.get(invoice);
                if (order != null && event != null) {
                    StrictJson.require(Objects.equals(order.customer(), event.customer())
                            && order.kind().equals(event.kind()) && order.time().equals(event.time()), "Order/clean metadata mismatch");
                }
                if (order != null && order.customer() == null) knownMissingCustomerRows++;
                if (service) {
                    StrictJson.require(node.get("order_key").isNull(), "Service maps to order");
                    services++;
                    if (order == null) serviceOnlyRows++;
                } else {
                    equalText(node, "order_key", invoice);
                    StrictJson.require(order != null, "Lineage maps to missing order");
                    lines.merge(invoice, 1L, Math::addExact);
                    if (event != null) {
                        quantities.merge(invoice, event.quantity(), BigInteger::add);
                        amounts.computeIfAbsent(invoice, ignored -> new ExactDecimal.Sum()).add(event.price(), event.quantity());
                        stocks.computeIfAbsent(invoice, ignored -> new HashSet<>()).add(event.stock());
                    }
                }
            }
        }
        if (events != null) StrictJson.require(lineage.keySet().equals(events.keySet()), "Incomplete lineage");
        for (Order order : orders) {
            StrictJson.require(Objects.equals(lines.get(order.invoice()), order.lines()), "Order membership counts mismatch");
            if (events != null) StrictJson.require(order.quantity().equals(quantities.get(order.invoice()))
                    && stocks.get(order.invoice()).size() == order.stocks()
                    && amounts.get(order.invoice()).matches(ExactDecimal.parse(order.amount())), "Order/clean totals mismatch");
        }
        requireCount("excluded_service_rows", services);
        StrictJson.require(count(counts, "included_product_rows") == count(sourceCounts, "accepted_product_rows")
                && services == count(sourceCounts, "accepted_service_rows")
                && lineage.size() == count(sourceCounts, "accepted_rows"), "Source accepted accounting mismatch");
        long missingCustomerRows = count(sourceCounts, "missing_customer_rows");
        // A retained order fixes the customer for its invoice, including excluded service rows.
        long unknownCustomerRows = serviceOnlyRows + count(sourceCounts, "quarantined_rows");
        StrictJson.require(missingCustomerRows >= knownMissingCustomerRows
                && missingCustomerRows - knownMissingCustomerRows <= unknownCustomerRows,
                "Source missing customer count contradicts order lineage");
        readDuplicateGroups(copy, lineage, events);
        if (products != null) products.putAll(stocks);
    }

    private void readDuplicateGroups(Path copy, Map<String, Lineage> lineage, Map<String, CleanOrderLine> events)
            throws IOException {
        Map<CleanOrderLine, Long> occurrences = new HashMap<>();
        if (events != null) {
            for (var event : events.values()) occurrences.merge(event, 1L, Math::addExact);
            occurrences.values().removeIf(count -> count == 1);
        }
        long groups = 0;
        Set<String> duplicated = new HashSet<>();
        Map<String, Long> productDuplicateExcess = new HashMap<>();
        String previousGroup = null;
        try (var reader = ArtifactFiles.reader(copy.resolve("duplicates.jsonl"))) {
            for (JsonNode node; (node = nextRow(reader)) != null;) {
                StrictJson.keys(node, "schema", "schema_version", "event_ids"); schema(node);
                JsonNode ids = node.get("event_ids");
                StrictJson.require(ids.isArray() && ids.size() >= 2, "Invalid duplicate group");
                String first = StrictJson.text(ids.get(0));
                Lineage group = lineage.get(first);
                StrictJson.require(group != null, "Unknown duplicate event");
                CleanOrderLine content = events == null ? null : events.get(first);
                if (events != null) StrictJson.require(Objects.equals(occurrences.remove(content), (long) ids.size()),
                        "Duplicate group differs from clean events");
                String last = null;
                for (JsonNode item : ids) {
                    String id = StrictJson.text(item);
                    StrictJson.require((events == null || content.equals(events.get(id)))
                            && group.equals(lineage.get(id)) && duplicated.add(id)
                            && (last == null || CODEPOINT.compare(last, id) < 0), "Invalid duplicate membership");
                    last = id;
                }
                StrictJson.require(previousGroup == null || CODEPOINT.compare(previousGroup, first) < 0, "Duplicate groups not sorted");
                previousGroup = first; groups++;
                if (!group.service()) productDuplicateExcess.merge(group.invoice(), ids.size() - 1L, Math::addExact);
            }
        }
        StrictJson.require(occurrences.isEmpty(), "Missing clean duplicate groups");
        requireCount("duplicate_groups", groups);
        for (Order order : orders) {
            long maxStocks = order.lines() - productDuplicateExcess.getOrDefault(order.invoice(), 0L);
            StrictJson.require(order.stocks() <= maxStocks, "Order stock count contradicts product duplicate groups");
        }
    }

    static List<JsonNode> rows(Path path) throws IOException {
        List<JsonNode> result = new ArrayList<>();
        try (var reader = ArtifactFiles.reader(path)) {
            for (String line; (line = reader.readLine()) != null;) result.add(parseRow(line));
        }
        return result;
    }
    static JsonNode nextRow(BufferedReader reader) throws IOException {
        String line = reader.readLine();
        return line == null ? null : parseRow(line);
    }

    private static JsonNode parseRow(String line) throws IOException {
        JsonNode node = ROW_JSON.readTree(line);
        StrictJson.require(node != null, "Empty JSONL record");
        return node;
    }
    static void schema(JsonNode node) throws IOException { StrictJson.version(node); equalText(node, "schema", "repeatlab-orders"); }
    static void equalText(JsonNode node, String field, String expected) throws IOException {
        StrictJson.require(StrictJson.text(node.get(field)).equals(expected), "Invalid " + field);
    }
    static String identifier(JsonNode node) throws IOException {
        String value = StrictJson.text(node);
        StrictJson.require(!CleaningPolicy.missing(value), "Empty identifier");
        return value;
    }
    static long count(JsonNode node, String key) throws IOException { return StrictJson.integer(node.get(key), 0, Long.MAX_VALUE); }
    private void requireCount(String key, long expected) throws IOException { StrictJson.require(count(counts, key) == expected, "Count mismatch: " + key); }
}
