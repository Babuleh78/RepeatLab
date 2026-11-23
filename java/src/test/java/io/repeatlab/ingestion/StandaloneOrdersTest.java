package io.repeatlab.ingestion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class StandaloneOrdersTest {
    private static final String CLEAN_HASH = "0".repeat(64);
    private static final String AMOUNT = "1E+2147483648";
    private static final BigInteger QUANTITY = BigInteger.TEN.pow(1100);

    private record TestOrder(String invoice, String kind, String customer, BigInteger quantity, int stocks) {}

    @TempDir Path temp;

    @Test void standaloneReaderAcceptsCompletedOrdersWithoutCleanAndPreservesExactValues() throws Exception {
        Path source = exchange(true);
        OrdersDataset data = read(source);

        assertEquals(CLEAN_HASH, data.cleanHash);
        assertEquals(Json.hash(source.resolve("manifest.json")), data.manifestHash);
        assertEquals("ok", data.dataStatus);
        assertEquals(QUANTITY, data.orders.getFirst().quantity());
        assertEquals(AMOUNT, data.orders.getFirst().amount());
        assertEquals(2, data.counts.get("excluded_service_rows").intValue());
        assertEquals(1, data.counts.get("duplicate_groups").intValue());
    }

    @Test void emptyCompletedOrdersCanCarryUpstreamQuarantine() throws Exception {
        Path source = exchange(false);
        OrdersDataset data = read(source);

        assertTrue(data.orders.isEmpty());
        assertEquals("quarantined", data.dataStatus);
        assertEquals(1, data.sourceCounts.get("quarantined_rows").intValue());
    }

    @Test void tamperedUnhashedArtifactIsRejected() throws Exception {
        Path source = exchange(true);
        Files.writeString(source.resolve("orders.jsonl"), "");

        assertThrows(IOException.class, () -> read(source));
    }

    @ParameterizedTest
    @CsvSource({"C1,sale", "c1,sale", "I,cancellation"})
    void rejectsInvoicePrefixAndKindMismatch(String invoice, String kind) throws Exception {
        BigInteger quantity = kind.equals("sale") ? QUANTITY : QUANTITY.negate();
        Path source = exchange(new TestOrder(invoice, kind, "A", quantity, 1),
                List.of("S", "S"), List.of(3, 4), 0, 0);

        assertThrows(IOException.class, () -> read(source));
    }

    @ParameterizedTest
    @ValueSource(strings={"C1", "c1"})
    void acceptsBothCancellationPrefixes(String invoice) throws Exception {
        Path source = exchange(new TestOrder(invoice, "cancellation", "A", QUANTITY.negate(), 1),
                List.of("S", "S"), List.of(3, 4), 0, 0);

        assertEquals(QUANTITY.negate(), read(source).orders.getFirst().quantity());
    }

    @ParameterizedTest
    @CsvSource({"I,sale,1", "c1,cancellation,-1"})
    void rejectsQuantityMagnitudeBelowProductLineCount(String invoice, String kind, long quantity) throws Exception {
        Path source = exchange(new TestOrder(invoice, kind, "A", BigInteger.valueOf(quantity), 1),
                List.of("S", "S"), List.of(3, 4), 0, 0);

        assertThrows(IOException.class, () -> read(source));
    }

    @Test void rejectsMissingCustomerCountBelowKnownMissingProductAndServiceRows() throws Exception {
        Path source = exchange(new TestOrder("I", "sale", null, QUANTITY, 1),
                List.of("I", "S"), List.of(1, 2), 1, 2);

        assertThrows(IOException.class, () -> read(source));
    }

    @Test void rejectsMissingCustomerCountAboveUnknownServiceAndQuarantinedRows() throws Exception {
        Path source = exchange(new TestOrder("I", "sale", "A", QUANTITY, 1),
                List.of("I", "S"), List.of(1, 2), 1, 3);

        assertThrows(IOException.class, () -> read(source));
    }

    @ParameterizedTest
    @ValueSource(ints={3, 4, 5})
    void acceptsFeasibleMissingCustomerCounts(int missing) throws Exception {
        Path source = exchange(new TestOrder("I", "sale", null, QUANTITY, 1),
                List.of("I", "S"), List.of(1, 2), 1, missing);

        assertEquals(missing, read(source).sourceCounts.get("missing_customer_rows").intValue());
    }

    @Test void rejectsStockCodeCountAboveProductDuplicateBound() throws Exception {
        Path source = exchange(new TestOrder("I", "sale", "A", QUANTITY, 2),
                List.of("S", "S"), List.of(1, 2), 0, 0);

        assertThrows(IOException.class, () -> read(source));
    }

    @Test void serviceDuplicatesOnProductInvoiceDoNotReduceStockCodeBound() throws Exception {
        Path source = exchange(new TestOrder("I", "sale", "A", QUANTITY, 2),
                List.of("I", "I"), List.of(3, 4), 0, 0);

        assertEquals(2, read(source).orders.getFirst().stocks());
    }

    @ParameterizedTest
    @ValueSource(strings={"wrong-invoice", "wrong-order", "wrong-order-count", "service-with-key",
            "bad-event-id", "duplicate-event-id", "bad-disposition"})
    void recomputedHashDoesNotHideInconsistentLineage(String mutation) throws Exception {
        Path source = exchange(true);
        Path path = source.resolve("lineage.jsonl");
        List<String> lines = Files.readAllLines(path);
        ObjectNode second = (ObjectNode) StrictJson.parse(lines.get(1));
        ObjectNode third = (ObjectNode) StrictJson.parse(lines.get(2));
        switch (mutation) {
            case "wrong-invoice" -> second.put("invoice_id", "J");
            case "wrong-order" -> second.put("order_key", "J");
            case "wrong-order-count" -> {
                second.put("disposition", "excluded_service");
                second.putNull("order_key");
            }
            case "service-with-key" -> third.put("order_key", "I");
            case "bad-event-id" -> second.put("event_id", "1");
            case "duplicate-event-id" -> second.put("event_id", "1".repeat(64));
            case "bad-disposition" -> second.put("disposition", "other");
            default -> throw new AssertionError(mutation);
        }
        lines.set(1, Json.encode(second));
        lines.set(2, Json.encode(third));
        Files.writeString(path, String.join("\n", lines) + "\n");
        updateHash(source, path);

        assertThrows(IOException.class, () -> read(source));
    }

    @ParameterizedTest
    @ValueSource(strings={"missing-member", "repeated-member", "cross-invoice"})
    void recomputedHashDoesNotHideInconsistentDuplicateGroup(String mutation) throws Exception {
        Path source = exchange(true);
        Path path = source.resolve("duplicates.jsonl");
        List<String> ids = switch (mutation) {
            case "missing-member" -> List.of("3".repeat(64), "5".repeat(64));
            case "repeated-member" -> List.of("3".repeat(64), "3".repeat(64));
            case "cross-invoice" -> List.of("2".repeat(64), "3".repeat(64));
            default -> throw new AssertionError(mutation);
        };
        Files.writeString(path, Json.encode(row("event_ids", ids)) + "\n");
        updateHash(source, path);

        assertThrows(IOException.class, () -> read(source));
    }

    @ParameterizedTest
    @ValueSource(strings={"incomplete", "invalid-quality", "invalid-clean-hash", "invalid-config-hash",
            "invalid-clean-artifact", "unexpected-clean-artifact", "inconsistent-source-counts"})
    void malformedManifestMetadataIsRejected(String mutation) throws Exception {
        Path source = exchange(true);
        Path path = source.resolve("manifest.json");
        ObjectNode manifest = (ObjectNode) StrictJson.read(path);
        ObjectNode original = (ObjectNode) manifest.get("input");
        ObjectNode artifacts = (ObjectNode) original.get("clean_artifacts");
        switch (mutation) {
            case "incomplete" -> manifest.put("status", "writing");
            case "invalid-quality" -> manifest.put("data_status", "quarantined");
            case "invalid-clean-hash" -> original.put("clean_manifest_sha256", "garbage");
            case "invalid-config-hash" -> original.put("config_sha256", "garbage");
            case "invalid-clean-artifact" -> artifacts.put("clean_events.jsonl", "garbage");
            case "unexpected-clean-artifact" -> artifacts.put("extra", CLEAN_HASH);
            case "inconsistent-source-counts" -> {
                ObjectNode counts = (ObjectNode) manifest.get("source_clean_counts");
                counts.put("warning_rows", 5);
                manifest.put("data_status", "warnings");
                Path auditPath = source.resolve("audit.json");
                ObjectNode audit = (ObjectNode) StrictJson.read(auditPath);
                audit.set("source_clean_counts", counts);
                Files.writeString(auditPath, Json.encode(audit) + "\n");
                ((ObjectNode) manifest.get("artifacts")).put("audit.json", Json.hash(auditPath));
            }
            default -> throw new AssertionError(mutation);
        }
        Files.writeString(path, Json.encode(manifest) + "\n");

        assertThrows(IOException.class, () -> read(source));
    }

    private OrdersDataset read(Path source) throws IOException {
        return new OrdersDataset(source, temp.resolve("verified"));
    }

    private Path exchange(boolean populated) throws IOException {
        if (populated) return exchange(new TestOrder("I", "sale", "A", QUANTITY, 1),
                List.of("S", "S"), List.of(3, 4), 0, 0);
        return exchange(null, List.of(), List.of(), 1, 0);
    }

    private Path exchange(TestOrder order, List<String> serviceInvoices, List<Integer> duplicateMembers,
                          int quarantined, int missing) throws IOException {
        Path source = Files.createDirectory(temp.resolve("orders"));
        int productRows = order == null ? 0 : 2;
        int serviceRows = serviceInvoices.size();
        int accepted = productRows + serviceRows;
        if (order != null) {
            Json.write(source.resolve("orders.jsonl"), row("order_key", order.invoice(), "invoice_id", order.invoice(),
                    "customer_id", order.customer(), "event_time", "2020-01-01T00:00:00", "kind", order.kind(),
                    "line_count", productRows, "quantity", order.quantity(), "stock_code_count", order.stocks(),
                    "total_amount", order.kind().equals("sale") ? AMOUNT : "-" + AMOUNT));
            try (var lines = Files.newBufferedWriter(source.resolve("lineage.jsonl"))) {
                for (int index = 0; index < accepted; index++) {
                    boolean service = index >= productRows;
                    Json.line(lines, row("event_id", eventId(index + 1),
                            "invoice_id", service ? serviceInvoices.get(index - productRows) : order.invoice(),
                            "order_key", service ? null : order.invoice(),
                            "disposition", service ? "excluded_service" : "included_product"));
                }
            }
            Json.write(source.resolve("duplicates.jsonl"),
                    row("event_ids", duplicateMembers.stream().map(StandaloneOrdersTest::eventId).toList()));
        } else {
            for (String name : List.of("orders.jsonl", "lineage.jsonl", "duplicates.jsonl"))
                Files.writeString(source.resolve(name), "");
        }
        Map<String, Object> counts = Json.object("orders", order != null ? 1 : 0,
                "sale_orders", order != null && order.kind().equals("sale") ? 1 : 0,
                "cancellation_orders", order != null && order.kind().equals("cancellation") ? 1 : 0,
                "missing_customer_orders", order != null && order.customer() == null ? 1 : 0,
                "included_product_rows", productRows, "excluded_service_rows", serviceRows,
                "duplicate_groups", duplicateMembers.isEmpty() ? 0 : 1);
        Map<String, Object> sourceCounts = Json.object("data_rows", accepted + quarantined, "accepted_rows", accepted,
                "accepted_product_rows", productRows, "accepted_service_rows", serviceRows,
                "quarantined_rows", quarantined, "missing_customer_rows", missing, "header_rows", 1,
                "empty_rows", 0, "warning_rows", missing);
        Json.write(source.resolve("audit.json"), row("counts", counts, "source_clean_counts", sourceCounts));
        Map<String, Object> hashes = Json.object();
        for (String name : OrdersDataset.ARTIFACTS) hashes.put(name, Json.hash(source.resolve(name)));
        Json.write(source.resolve("manifest.json"), row("status", "completed", "policy_version", "orders-v1",
                "worker_protocol_version", 1, "data_status", quarantined > 0 ? "quarantined" : missing > 0 ? "warnings" : "ok",
                "counts", counts,
                "source_clean_counts", sourceCounts, "artifacts", hashes, "input", Json.object(
                        "clean_manifest_sha256", CLEAN_HASH, "config_sha256", CLEAN_HASH, "clean_artifacts",
                        Json.object("clean_events.jsonl", CLEAN_HASH, "dispositions.jsonl", CLEAN_HASH,
                                "audit.json", CLEAN_HASH))));
        return source;
    }

    private static String eventId(int number) {
        return Integer.toString(number).repeat(64);
    }

    private static Map<String, Object> row(Object... fields) {
        Map<String, Object> result = Json.object(fields);
        result.put("schema", "repeatlab-orders");
        result.put("schema_version", 1);
        return result;
    }

    private static void updateHash(Path source, Path artifact) throws IOException {
        Path path = source.resolve("manifest.json");
        ObjectNode manifest = (ObjectNode) StrictJson.read(path);
        ((ObjectNode) manifest.get("artifacts")).put(artifact.getFileName().toString(), Json.hash(artifact));
        Files.writeString(path, Json.encode(manifest) + "\n");
    }
}
