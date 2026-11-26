package io.repeatlab.ingestion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DuplicateSensitivityAnalyticsTest {
    @TempDir Path temp;

    @Test void comparesEachObservedSegmentAndPreservesUnboundedSignedUnits() throws Exception {
        String large = "1" + "0".repeat(1100);
        String twice = new BigInteger(large).multiply(BigInteger.TWO).toString();
        String date = "0001-01-31T23:59:59.999999";
        Path facts = writeFacts(List.of(
                order("A", date, true, values(2, twice, "2.000"), values(1, large, "1.0")),
                order("B", date, true, values(1, large, "0.335"), values(1, large, "0.3350")),
                order("C1", date, false, values(2, "-" + twice, "-0.670"), values(1, "-" + large, "-0.335")),
                order("D", date, false, values(1, "2", "4"), values(1, "2", "4")),
                order("E", "9999-12-01T00:00:00", true, values(1, "1", "1"), values(1, "1", "1"))));
        Path clean = writeClean(List.of(event("A", date, "buyer", large, "1E-1100"), event("B", date, "buyer", large, "335E-1103"),
                event("C1", date, null, "-" + large, "335E-1103"), event("D", date, null, "2", "2"),
                event("A", date, "buyer", large, "1E-1100"), event("C1", date, null, "-" + large, "335E-1103"),
                event("E", "9999-12-01T00:00:00.000000", "buyer", "1", "1")));
        var result = runWithPolicy(facts, temp, clean);
        assertEquals(5, result.orders());
        assertEquals(7, result.productLines());
        assertEquals(5, result.collapsedLines());
        assertEquals(4, result.rows());
        assertFalse(result.engine().isBlank());
        assertEquals(1, result.sqlHashes().size());
        assertTrue(result.sqlHashes().get("duplicate_sensitivity.sql").matches("[0-9a-f]{64}"));
        var rows = OrdersDataset.rows(temp.resolve("duplicate_sensitivity.jsonl"));
        assertEquals(4, rows.size());
        assertSegment(rows.get(0), "0001-01", "cancellation", false,
                group(1, 2, "-" + twice, "-0.670"), group(1, 1, "-" + large, "-0.335"));
        assertSegment(rows.get(1), "0001-01", "sale", false, group(1, 1, "2", "4.000"), group(1, 1, "2", "4.000"));
        assertSegment(rows.get(2), "0001-01", "sale", true,
                group(2, 3, new BigInteger(twice).add(new BigInteger(large)).toString(), "2.335"),
                group(2, 2, twice, "1.335"));
        assertSegment(rows.get(3), "9999-12", "sale", true, group(1, 1, "1", "1.000"), group(1, 1, "1", "1.000"));
    }

    @Test void emptyFactsWriteEmptyReport() throws Exception {
        var service = event("S", "2010-01-01T00:00:00", "buyer", "1", "10");
        service.put("is_service", true);
        service.put("stock_code", "SERVICE");
        var result = runWithPolicy(writeFacts(List.of()), temp, writeClean(List.of(service)));
        assertEquals(0, result.orders());
        assertEquals(0, result.rows());
        assertEquals(0, Files.size(temp.resolve("duplicate_sensitivity.jsonl")));
    }

    @Test void aProductCannotBeSkippedAsAService() throws Exception {
        var product = event("I", "2010-01-01T00:00:00", "buyer", "1", "10");
        product.put("is_service", true);
        Path facts = writeFacts(List.of()), clean = writeClean(List.of(product));
        var failure = assertThrows(IOException.class, () -> runWithPolicy(facts, temp, clean));
        assertTrue(failure.getMessage().contains("Service flag"));
    }

    @Test void rejectsInconsistentScenarioFactsBeforeAggregation() throws Exception {
        String date = "2010-01-31T00:00:00";
        Path clean = writeClean(List.of(event("A", date, "buyer", "6", "1"), event("A", date, "buyer", "6", "1")));
        var valid = order("A", date, true, values(2, "12", "12"), values(1, "6", "6"));
        Path good = Files.createTempDirectory(temp, "valid");
        assertEquals(1, runWithPolicy(writeFacts(good, List.of(valid)), good, clean).orders());
        var bad = new ArrayList<Map<String, Object>>();
        bad.add(order("A", "2010-02-30T00:00:00", true, values(1, "1", "8"), values(1, "1", "8")));
        bad.add(order("A", date + ".0000001", true, values(1, "1", "8"), values(1, "1", "8")));
        bad.add(order("A", date, true, values(2, "-12", "8"), values(1, "6", "4")));
        bad.add(order("A", date, true, values(2, "12", "8"), values(2, "6", "4")));
        bad.add(order("A", date, true, values(2, "12", "8"), values(1, "12", "4")));
        bad.add(order("A", date, true, values(2, "12", "8"), values(1, "6", "8")));
        bad.add(order("A", date, true, values(2, "12", "8"), values(3, "6", "4")));
        bad.add(order("A", date, true, values(2, "1", "8"), values(1, "1", "4")));
        for (var row : bad) {
            Path stage = Files.createTempDirectory(temp, "invalid");
            assertThrows(IOException.class, () -> runWithPolicy(writeFacts(stage, List.of(row)), stage, clean));
        }
        Path stage = Files.createTempDirectory(temp, "twice");
        assertThrows(IOException.class, () -> runWithPolicy(writeFacts(stage, List.of(valid, valid)), stage, clean));
    }

    @Test void refusesGlobalScaleThatWouldRoundOrOverflowAndGroupTotalsBeyond38Digits() throws Exception {
        String date = "2010-01-01T00:00:00";
        var max = values(1, "1", "9".repeat(38));
        for (String amount : List.of("1E-39", "1E+38", "0.1")) {
            Path stage = Files.createTempDirectory(temp, "shape");
            var v = values(1, "1", amount);
            Path clean = writeClean(List.of(event("A", date, "buyer", "1", "9".repeat(38)), event("B", date, null, "1", amount)));
            Path facts = writeFacts(stage, List.of(order("A", date, true, max, max), order("B", date, false, v, v)));
            assertThrows(IOException.class, () -> runWithPolicy(facts, stage, clean));
        }
        var one = values(1, "1", "1");
        Path facts = writeFacts(List.of(order("A", date, true, max, max), order("B", date, true, one, one)));
        Path sameSegment = writeClean(List.of(event("A", date, "buyer", "1", "9".repeat(38)), event("B", date, "other", "1", "1")));
        assertThrows(SQLException.class, () -> runWithPolicy(facts, temp, sameSegment));
    }

    @Test void bindsWorkerOrdersAndSegmentMembershipToEveryCleanProductInvoice() throws Exception {
        String date = "2010-01-01T00:00:00";
        Path clean = writeClean(List.of(event("A", date, null, "1", "2"), event("B", date, "buyer", "3", "1.5"),
                event("B", date, "buyer", "1", "1.5")));
        var one = values(1, "1", "2");
        var two = values(2, "4", "6");
        var a = order("A", date, false, one, one);
        var b = order("B", date, true, two, two);
        // Equal grand totals can still assign product lines, units or sales to the wrong order or segment.
        var others = List.of(
                List.of(order("A", date, false, two, two), order("B", date, true, one, one)),
                List.of(order("A", date, true, one, one), b),
                List.of(a, order("B", date + ".000001", true, two, two)),
                List.of(a, order("X", date, true, two, two)),
                List.of(a, order("B", date, true, values(2, "5", "6"), values(2, "5", "6"))),
                List.of(a, order("B", date, true, values(2, "4", "5"), values(2, "4", "5"))),
                List.of(a, order("B", date, true, two, values(1, "3", "4.5"))),
                List.of(a));
        for (var facts : others) {
            Path stage = Files.createTempDirectory(temp, "replay");
            var failure = assertThrows(IOException.class,
                    () -> runWithPolicy(writeFacts(stage, facts), stage, clean));
            assertTrue(failure.getMessage().contains("clean product"));
        }
    }

    private static Map<String, Object> event(String key, String date, String customer, String units, String price) {
        return Json.object("invoice_id", key, "event_time", date, "kind", key.startsWith("C") ? "cancellation" : "sale",
                "is_service", false, "customer_id", customer, "quantity", new BigInteger(units), "stock_code", "P", "unit_price", price);
    }

    private static DuplicateSensitivityAnalytics.Result runWithPolicy(Path facts, Path stage, Path clean)
            throws IOException, SQLException {
        var config = StrictJson.parse(Json.encode(Json.object("policy_version", "clean-v1", "service_codes", List.of("SERVICE"))));
        return DuplicateSensitivityAnalytics.run(facts, stage, clean, CleaningPolicy.fromManifest(config));
    }

    private Path writeClean(List<Map<String, Object>> events) throws IOException {
        Path file = Files.createTempFile(temp, "events", ".jsonl");
        try (var writer = Files.newBufferedWriter(file)) {
            for (var event : events) Json.line(writer, event);
        }
        return file;
    }

    private Path writeFacts(List<Map<String, Object>> rows) throws IOException { return writeFacts(temp, rows); }

    private static Path writeFacts(Path dir, List<Map<String, Object>> rows) throws IOException {
        Path file = dir.resolve("facts.jsonl");
        try (var writer = Files.newBufferedWriter(file)) {
            for (var row : rows) Json.line(writer, row);
        }
        return file;
    }

    private static Map<String, Object> values(long lines, String units, String amount) {
        return Json.object("lines", lines, "units", units, "amount", amount);
    }

    private static Map<String, Object> order(String key, String date, boolean known, Object retained, Object collapsed) {
        return Json.object("order_key", key, "event_time", date, "kind", key.startsWith("C") ? "cancellation" : "sale",
                "customer_known", known, "retained", retained, "collapsed", collapsed);
    }

    private static Map<String, Object> group(long orders, long lines, String units, String amount) {
        return Json.object("order_count", orders, "product_line_count", lines, "units", units, "amount", amount);
    }

    private static void assertSegment(JsonNode row, String month, String kind, boolean known,
                                      Object retained, Object collapsed) throws IOException {
        var expected = Json.object("schema", "repeatlab-duplicate-sensitivity", "schema_version", 1,
                "month", month, "kind", kind, "customer_known", known, "retained", retained, "collapsed", collapsed);
        assertEquals(StrictJson.parse(Json.encode(expected)), row);
    }
}
