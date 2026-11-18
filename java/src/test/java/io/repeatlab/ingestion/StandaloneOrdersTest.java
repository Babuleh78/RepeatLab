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
import org.junit.jupiter.params.provider.ValueSource;

class StandaloneOrdersTest {
    private static final String CLEAN_HASH = "0".repeat(64);
    private static final String AMOUNT = "1E+2147483648";
    private static final BigInteger QUANTITY = BigInteger.TEN.pow(1100);

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
        Path source = Files.createDirectory(temp.resolve("orders"));
        if (populated) {
            Json.write(source.resolve("orders.jsonl"), row("order_key", "I", "invoice_id", "I",
                    "customer_id", "A", "event_time", "2020-01-01T00:00:00", "kind", "sale",
                    "line_count", 2, "quantity", QUANTITY, "stock_code_count", 1, "total_amount", AMOUNT));
            try (var lines = Files.newBufferedWriter(source.resolve("lineage.jsonl"))) {
                for (int number = 1; number <= 4; number++) {
                    boolean service = number >= 3;
                    Json.line(lines, row("event_id", Integer.toString(number).repeat(64), "invoice_id", service ? "S" : "I",
                            "order_key", service ? null : "I",
                            "disposition", service ? "excluded_service" : "included_product"));
                }
            }
            Json.write(source.resolve("duplicates.jsonl"), row("event_ids", List.of("3".repeat(64), "4".repeat(64))));
        } else {
            for (String name : List.of("orders.jsonl", "lineage.jsonl", "duplicates.jsonl"))
                Files.writeString(source.resolve(name), "");
        }
        Map<String, Object> counts = Json.object("orders", populated ? 1 : 0, "sale_orders", populated ? 1 : 0,
                "cancellation_orders", 0, "missing_customer_orders", 0, "included_product_rows", populated ? 2 : 0,
                "excluded_service_rows", populated ? 2 : 0, "duplicate_groups", populated ? 1 : 0);
        Map<String, Object> sourceCounts = Json.object("data_rows", populated ? 4 : 1, "accepted_rows", populated ? 4 : 0,
                "accepted_product_rows", populated ? 2 : 0, "accepted_service_rows", populated ? 2 : 0,
                "quarantined_rows", populated ? 0 : 1, "missing_customer_rows", 0, "header_rows", 1,
                "empty_rows", 0, "warning_rows", 0);
        Json.write(source.resolve("audit.json"), row("counts", counts, "source_clean_counts", sourceCounts));
        Map<String, Object> hashes = Json.object();
        for (String name : OrdersDataset.ARTIFACTS) hashes.put(name, Json.hash(source.resolve(name)));
        Json.write(source.resolve("manifest.json"), row("status", "completed", "policy_version", "orders-v1",
                "worker_protocol_version", 1, "data_status", populated ? "ok" : "quarantined", "counts", counts,
                "source_clean_counts", sourceCounts, "artifacts", hashes, "input", Json.object(
                        "clean_manifest_sha256", CLEAN_HASH, "config_sha256", CLEAN_HASH, "clean_artifacts",
                        Json.object("clean_events.jsonl", CLEAN_HASH, "dispositions.jsonl", CLEAN_HASH,
                                "audit.json", CLEAN_HASH))));
        return source;
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
