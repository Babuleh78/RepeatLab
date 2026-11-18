package io.repeatlab.ingestion;

import static org.junit.jupiter.api.Assertions.*;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class AnalyticsTest {
    @TempDir Path temp;

    private List<String> child(String mode) {
        String executable = System.getProperty("os.name").startsWith("Windows") ? "java.exe" : "java";
        return List.of(Path.of(System.getProperty("java.home"), "bin", executable).toString(), "-cp",
                System.getProperty("java.class.path"), AnalyticsChild.class.getName(), mode);
    }

    @Test void largeChildStreamsDoNotDeadlockAndNonzeroExitIsRetained() throws Exception {
        assertEquals(7, AnalyzeCommand.runProcess(child("flood"), temp));
        assertTrue(Files.size(temp.resolve("worker.stdout.log")) >= 1_000_000);
        assertTrue(Files.size(temp.resolve("worker.stderr.log")) >= 1_000_000);
    }

    @Test void interruptedWorkerIsStoppedAndIoFailureIsExplicit() throws Exception {
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread thread = new Thread(() -> {
            try { AnalyzeCommand.runProcess(child("sleep"), temp); }
            catch (Exception caught) { failure.set(caught); }
        });
        thread.start();
        long limit = System.nanoTime() + 5_000_000_000L;
        while (!Files.exists(temp.resolve("worker.stdout.log")) && System.nanoTime() < limit) Thread.sleep(10);
        thread.interrupt(); thread.join(5000);
        assertFalse(thread.isAlive());
        assertInstanceOf(InterruptedException.class, failure.get());
        Path blocked = Files.createDirectory(temp.resolve("blocked"));
        Files.createDirectory(blocked.resolve("worker.stdout.log"));
        assertThrows(java.io.IOException.class, () -> AnalyzeCommand.runProcess(child("flood"), blocked));
    }

    private OrdersDataset.Order order(String invoice, String amount) {
        return new OrdersDataset.Order(invoice, "A", LocalDateTime.parse("2020-01-01T00:00:00"),
                "sale", 1, BigInteger.ONE, 1, amount);
    }

    @Test void exactSqlAndSharedScaleAreNeverDefaultDecimalOrDouble() throws Exception {
        Path stage = Files.createDirectory(temp.resolve("sql"));
        var result = SqlAnalytics.run(List.of(order("I1", "0.335"), order("I2", "0.1000")), stage);
        assertEquals(3, result.scale());
        var monthly = StrictJson.read(stage.resolve("monthly_activity.jsonl"));
        assertEquals("0.435", monthly.get("total_amount").textValue());
        assertEquals(2, monthly.get("order_count").intValue());
        assertEquals(1, monthly.get("customer_count").intValue());
    }

    @ParameterizedTest
    @ValueSource(strings={"1E-39", "1E+38", "1E+2147483648", "1E-2147483649"})
    void numericBoundsFailBeforeRescalingOrPlainStringAllocation(String amount) {
        assertThrows(java.io.IOException.class, () -> SqlAnalytics.scale(List.of(order("I", amount))));
    }

    @Test void fitting38DigitsRemainExactAndGroupedOverflowFails() throws Exception {
        String maximum = "99999999999999999999999999999999999999";
        Path fitting = Files.createDirectory(temp.resolve("fit"));
        SqlAnalytics.run(List.of(order("I", maximum)), fitting);
        assertEquals(maximum, StrictJson.read(fitting.resolve("monthly_activity.jsonl")).get("total_amount").textValue());
        Path overflowing = Files.createDirectory(temp.resolve("overflow"));
        assertThrows(java.sql.SQLException.class, () -> SqlAnalytics.run(List.of(order("I", maximum), order("J", maximum)), overflowing));
        Path decimalOverflow = Files.createDirectory(temp.resolve("decimal-overflow"));
        assertThrows(java.sql.SQLException.class, () -> SqlAnalytics.run(List.of(order("I", maximum), order("J", "1")), decimalOverflow));
    }

    private Path[] exchange(String amount) throws Exception {
        Path folder = Files.createTempDirectory(temp, "exchange");
        Path clean = Files.createDirectory(folder.resolve("clean"));
        Path orders = Files.createDirectory(folder.resolve("orders"));
        Object sourceCounts = Json.object("data_rows", 1, "accepted_rows", 1, "accepted_product_rows", 1,
                "accepted_service_rows", 0, "quarantined_rows", 0, "missing_customer_rows", 0,
                "header_rows", 1, "empty_rows", 0, "warning_rows", 0);
        Json.write(clean.resolve("clean_events.jsonl"), Json.object("event_id", "e1", "invoice_id", "I", "customer_id", "A",
                "event_time", "2020-01-01T00:00:00", "kind", "sale", "quantity", new BigInteger("100000000000000000000"),
                "stock_code", "P", "unit_price", "0.01", "is_service", false));
        Json.write(clean.resolve("dispositions.jsonl"), Json.object());
        Json.write(clean.resolve("audit.json"), Json.object());
        Map<String, Object> cleanHashes = Json.object();
        for (String name : List.of("clean_events.jsonl", "dispositions.jsonl", "audit.json")) cleanHashes.put(name, Json.hash(clean.resolve(name)));
        Json.write(clean.resolve("manifest.json"), Json.object("schema", "repeatlab-clean", "schema_version", 1, "status", "completed",
                "data_status", "ok", "counts", sourceCounts, "artifacts", cleanHashes, "config_sha256", "0".repeat(64)));
        Map<String, Object> counts = Json.object("orders", 1, "sale_orders", 1, "cancellation_orders", 0,
                "missing_customer_orders", 0, "included_product_rows", 1, "excluded_service_rows", 0, "duplicate_groups", 0);
        Json.write(orders.resolve("orders.jsonl"), Json.object("schema", "repeatlab-orders", "schema_version", 1,
                "order_key", "I", "invoice_id", "I", "customer_id", "A", "event_time", "2020-01-01T00:00:00", "kind", "sale",
                "line_count", 1, "quantity", new BigInteger("100000000000000000000"), "stock_code_count", 1, "total_amount", amount));
        Json.write(orders.resolve("lineage.jsonl"), Json.object("schema", "repeatlab-orders", "schema_version", 1,
                "event_id", "e1", "invoice_id", "I", "order_key", "I", "disposition", "included_product"));
        Files.writeString(orders.resolve("duplicates.jsonl"), "");
        Json.write(orders.resolve("audit.json"), Json.object("schema", "repeatlab-orders", "schema_version", 1,
                "counts", counts, "source_clean_counts", sourceCounts));
        Map<String, Object> hashes = Json.object();
        for (String name : OrdersDataset.ARTIFACTS) hashes.put(name, Json.hash(orders.resolve(name)));
        Json.write(orders.resolve("manifest.json"), Json.object("schema", "repeatlab-orders", "schema_version", 1,
                "policy_version", "orders-v1", "worker_protocol_version", 1, "status", "completed", "data_status", "ok",
                "input", Json.object("clean_manifest_sha256", Json.hash(clean.resolve("manifest.json")), "clean_artifacts", cleanHashes,
                        "config_sha256", "0".repeat(64)), "counts", counts, "source_clean_counts", sourceCounts, "artifacts", hashes));
        return new Path[]{orders, clean, folder.resolve("verified")};
    }

    @Test void canonicalOrdersReaderDoesNotNarrowQuantityOrDecimalExponent() throws Exception {
        Path[] paths = exchange("1E+2147483648");
        OrdersDataset data = new OrdersDataset(paths[0], paths[1], paths[2]);
        assertEquals(new BigInteger("100000000000000000000"), data.orders.getFirst().quantity());
        assertEquals("1E+2147483648", data.orders.getFirst().amount());
        assertThrows(java.io.IOException.class, () -> SqlAnalytics.scale(data.orders));
    }

    @Test void quantityTokensMayExceedJacksonDefaultLength() throws Exception {
        Path path = temp.resolve("huge.jsonl");
        String integer = "1" + "0".repeat(1000);
        Files.writeString(path, "{\"quantity\":" + integer + "}\n");
        assertEquals(new BigInteger(integer), OrdersDataset.rows(path).getFirst().get("quantity").bigIntegerValue());
    }

    @ParameterizedTest
    @ValueSource(strings={"quantity", "key", "sign", "time", "lineage", "counts", "duplicate-key", "hash",
            "schema", "version", "extra", "stock", "customer", "money", "repeated-order", "repeated-lineage", "audit"})
    void strictReaderRejectsTamperedOrRehashedInvalidExchange(String mutation) throws Exception {
        Path[] paths = exchange("0.335");
        Path artifact = paths[0].resolve("orders.jsonl");
        ObjectNode node = (ObjectNode) StrictJson.read(artifact);
        switch (mutation) {
            case "quantity" -> node.put("quantity", true);
            case "key" -> node.put("order_key", "other");
            case "sign" -> node.put("total_amount", "-0.335");
            case "time" -> node.put("event_time", "2020-01-01T00:00:00.0000001");
            case "counts" -> node.put("line_count", 2);
            case "schema" -> node.put("schema", "other");
            case "version" -> node.put("schema_version", 2);
            case "extra" -> node.put("unexpected", true);
            case "stock" -> node.put("stock_code_count", 0);
            case "customer" -> node.put("customer_id", "\u00a0");
            case "money" -> node.put("total_amount", "NaN");
            case "repeated-lineage" -> {
                artifact = paths[0].resolve("lineage.jsonl");
                node = (ObjectNode) StrictJson.read(artifact);
            }
            case "audit" -> {
                artifact = paths[0].resolve("audit.json");
                node = (ObjectNode) StrictJson.read(artifact);
                ((ObjectNode) node.get("counts")).put("orders", 2);
            }
            case "lineage" -> {
                artifact = paths[0].resolve("lineage.jsonl");
                node = (ObjectNode) StrictJson.read(artifact);
                node.put("event_id", "unknown");
            }
            default -> { }
        }
        Files.writeString(artifact, Json.encode(node) + (mutation.equals("hash") ? " \n" : "\n"));
        if (mutation.startsWith("repeated-")) Files.writeString(artifact, (Json.encode(node) + "\n").repeat(2));
        if (mutation.equals("duplicate-key")) Files.writeString(artifact, Json.encode(node).replace("\"quantity\":", "\"quantity\":1,\"quantity\":"));
        if (!mutation.equals("hash")) {
            ObjectNode manifest = (ObjectNode) StrictJson.read(paths[0].resolve("manifest.json"));
            ((ObjectNode) manifest.get("artifacts")).put(artifact.getFileName().toString(), Json.hash(artifact));
            Files.writeString(paths[0].resolve("manifest.json"), Json.encode(manifest));
        }
        assertThrows(java.io.IOException.class, () -> new OrdersDataset(paths[0], paths[1], paths[2]));
    }

    @Test void unavailableWorkerFailsWithoutPublishingCompletionAndReservesOutput() throws Exception {
        Path input = Files.createDirectory(temp.resolve("clean"));
        for (String name : new String[]{"manifest.json", "audit.json", "clean_events.jsonl", "dispositions.jsonl"})
            Files.writeString(input.resolve(name), "{}");
        Path output = temp.resolve("result");
        try (var quiet = new PrintStream(new ByteArrayOutputStream())) {
            assertEquals(1, Main.run(new String[]{"analyze", "--input", input.toString(),
                    "--python", temp.resolve("absent-python").toString(), "--output", output.toString()}, quiet, quiet));
        }
        assertTrue(Files.exists(output.resolve("fatal.json")));
        assertFalse(Files.exists(output.resolve("manifest.json")));
    }
}
