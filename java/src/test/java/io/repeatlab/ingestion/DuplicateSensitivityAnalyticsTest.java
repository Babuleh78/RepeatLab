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
        String large = "1" + "0".repeat(100);
        String twice = new BigInteger(large).multiply(BigInteger.TWO).toString();
        String date = "0001-01-31T23:59:59.999999";
        Path facts = writeFacts(List.of(
                order("A", date, true, values(2, twice, "2.000"), values(1, large, "1.0")),
                order("B", date, true, values(1, large, "0.335"), values(1, large, "0.3350")),
                order("C1", date, false, values(2, "-" + twice, "-0.670"), values(1, "-" + large, "-0.335")),
                order("D", date, false, values(1, "2", "4"), values(1, "2", "4")),
                order("E", "9999-12-01T00:00:00", true, values(1, "1", "1"), values(1, "1", "1"))));
        var result = DuplicateSensitivityAnalytics.run(facts, temp);
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
        var result = DuplicateSensitivityAnalytics.run(writeFacts(List.of()), temp);
        assertEquals(0, result.orders());
        assertEquals(0, result.rows());
        assertEquals(0, Files.size(temp.resolve("duplicate_sensitivity.jsonl")));
    }

    @Test void rejectsInconsistentScenarioFactsBeforeAggregation() throws Exception {
        String date = "2010-01-31T00:00:00";
        var valid = order("A", date, true, values(2, "12", "8"), values(1, "6", "4"));
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
            assertThrows(IOException.class, () -> DuplicateSensitivityAnalytics.run(writeFacts(stage, List.of(row)), stage));
        }
        Path stage = Files.createTempDirectory(temp, "twice");
        assertThrows(IOException.class, () -> DuplicateSensitivityAnalytics.run(writeFacts(stage, List.of(valid, valid)), stage));
    }

    @Test void refusesGlobalScaleThatWouldRoundOrOverflowAndGroupTotalsBeyond38Digits() throws Exception {
        String date = "2010-01-01T00:00:00";
        var max = values(1, "1", "9".repeat(38));
        for (String amount : List.of("1E-39", "1E+38", "0.1")) {
            Path stage = Files.createTempDirectory(temp, "shape");
            var v = values(1, "1", amount);
            Path facts = writeFacts(stage, List.of(order("A", date, true, max, max), order("B", date, false, v, v)));
            assertThrows(IOException.class, () -> DuplicateSensitivityAnalytics.run(facts, stage));
        }
        var one = values(1, "1", "1");
        Path facts = writeFacts(List.of(order("A", date, true, max, max), order("B", date, true, one, one)));
        assertThrows(SQLException.class, () -> DuplicateSensitivityAnalytics.run(facts, temp));
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
