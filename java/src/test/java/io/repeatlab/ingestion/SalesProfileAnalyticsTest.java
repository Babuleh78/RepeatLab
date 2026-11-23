package io.repeatlab.ingestion;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SalesProfileAnalyticsTest {
    private static final LocalDateTime START = LocalDateTime.parse("2020-01-01T00:00:00.123456");
    private static final long DAY = 86_400_000_000L;
    private static final long[] BOUNDS = {0, DAY, 7 * DAY, 30 * DAY, 90 * DAY};
    private static final String MAX_MONEY = "9".repeat(38);
    private static final String INTERVALS = "purchase_intervals.jsonl";
    private static final String CONCENTRATION = "sales_concentration.jsonl";

    @TempDir Path temp;

    @Test void profileMatchesHandCalculationAndKeepsTheExactQuotaAtTies() throws Exception {
        var orders = new ArrayList<>(List.of(
                sale("a1", "A", START, "5"),
                sale("a2", "A", START, "10"),
                sale("a3", "A", START.plusDays(3), "10"),
                sale("a4", "A", START.plusDays(8), "15"),
                sale("b", "B", START, "20"),
                sale("c", "C", START, "20"),
                sale("anonymous", null, START.plusDays(1), "100"),
                cancellation("return-a", "A", START.plusDays(4), "-1E+5000"),
                cancellation("return-d", "D", START.plusDays(2), "-1E-5000")));
        for (int i = 0; i < 22; i++) {
            orders.add(sale("small-" + i, "small-" + i, START, "1"));
        }

        var result = SalesProfileAnalytics.run(orders, temp);
        assertFalse(result.engine().isBlank());
        assertEquals(0, result.decimalScale());
        assertEquals(2, result.intervalRows());
        assertEquals(3, result.concentrationRows());
        assertEquals(Set.of("purchase_intervals.sql", "sales_concentration.sql"), result.sqlHashes().keySet());
        for (var hash : result.sqlHashes().entrySet()) {
            try (var input = SalesProfileAnalyticsTest.class.getResourceAsStream("/sql/" + hash.getKey())) {
                byte[] bytes = Objects.requireNonNull(input, hash.getKey()).readAllBytes();
                assertEquals(Json.hashText(new String(bytes, StandardCharsets.UTF_8)), hash.getValue());
            }
        }
        assertTrue(Files.notExists(temp.resolve("duckdb-temp")));
        var intervals = rows(temp, INTERVALS);
        assertEquals(2, intervals.size());
        assertInterval(intervals.get(0), "first", 1, 1, 3 * DAY, 3 * DAY, 0, 1, 0, 0, 0);
        assertInterval(intervals.get(1), "later", 1, 1, 5 * DAY, 5 * DAY, 0, 1, 0, 0, 0);
        var concentration = rows(temp, CONCENTRATION);
        assertEquals(3, concentration.size());
        assertConcentration(concentration.get(0), 1, 25, 1, "40", "102", "202");
        assertConcentration(concentration.get(1), 5, 25, 2, "60", "102", "202");
        assertConcentration(concentration.get(2), 10, 25, 3, "80", "102", "202");

        Collections.reverse(orders);
        Path reversed = Files.createDirectory(temp.resolve("reversed"));
        assertEquals(result, SalesProfileAnalytics.run(orders, reversed));
        assertArrayEquals(Files.readAllBytes(temp.resolve(INTERVALS)), Files.readAllBytes(reversed.resolve(INTERVALS)));
        assertArrayEquals(Files.readAllBytes(temp.resolve(CONCENTRATION)),
                Files.readAllBytes(reversed.resolve(CONCENTRATION)));
    }

    @Test void histogramIsHalfOpenAtEveryMicrosecondBoundary() throws Exception {
        var orders = new ArrayList<OrdersDataset.Order>();
        var time = START;
        orders.add(sale("start", "repeat", time, "1"));
        long[] gaps = {1, DAY - 1, DAY, 7 * DAY - 1, 7 * DAY, 30 * DAY - 1,
                30 * DAY, 90 * DAY - 1, 90 * DAY};
        for (int i = 0; i < gaps.length; i++) {
            time = time.plus(gaps[i], ChronoUnit.MICROS);
            orders.add(sale("later-" + i, "repeat", time, "1"));
        }
        orders.add(sale("second-start", "second", START, "1"));
        orders.add(sale("second-end", "second", START.plusDays(1), "1"));
        SalesProfileAnalytics.run(orders, temp);

        var intervals = rows(temp, INTERVALS);
        assertInterval(intervals.get(0), "first", 2, 2, 1L, DAY, 1, 1, 0, 0, 0);
        assertInterval(intervals.get(1), "later", 8, 1, DAY - 1, 90 * DAY, 1, 2, 2, 2, 1);
    }

    @Test void anonymousInvoicesAffectOnlyAllSalesAndKeepTheirScale() throws Exception {
        var result = SalesProfileAnalytics.run(List.of(
                sale("unknown-one", null, START, "1e-3"),
                sale("unknown-two", null, START.plusDays(1), "0.335"),
                cancellation("cancellation", "A", START.plusDays(2), "-1E+5000")), temp);

        assertEquals(3, result.decimalScale());
        assertEmptyIntervals();
        var concentration = rows(temp, CONCENTRATION);
        assertConcentration(concentration.get(0), 1, 0, 0, "0.000", "0.000", "0.336");
        assertConcentration(concentration.get(1), 5, 0, 0, "0.000", "0.000", "0.336");
        assertConcentration(concentration.get(2), 10, 0, 0, "0.000", "0.000", "0.336");
    }

    @Test void emptySalesKeepAllOutputRowsAndNullShares() throws Exception {
        SalesProfileAnalytics.run(List.of(), temp);

        assertEmptyIntervals();
        var concentration = rows(temp, CONCENTRATION);
        assertEquals(3, concentration.size());
        assertConcentration(concentration.get(0), 1, 0, 0, "0", "0", "0");
        assertConcentration(concentration.get(1), 5, 0, 0, "0", "0", "0");
        assertConcentration(concentration.get(2), 10, 0, 0, "0", "0", "0");
    }

    @Test void monetaryOutputsUseExactDecimalsAndDifferentDenominators() throws Exception {
        SalesProfileAnalytics.run(List.of(
                sale("a", "A", START, "0.335"),
                sale("b", "B", START, "0.6650"),
                sale("unknown", null, START, "1e-3")), temp);

        var concentration = rows(temp, CONCENTRATION);
        assertConcentration(concentration.get(0), 1, 2, 1, "0.665", "1.000", "1.001");
        assertConcentration(concentration.get(1), 5, 2, 1, "0.665", "1.000", "1.001");
        assertConcentration(concentration.get(2), 10, 2, 1, "0.665", "1.000", "1.001");
    }

    @Test void maximumExactWidthAndScaleRoundTripThroughDuckdb() throws Exception {
        SalesProfileAnalytics.run(List.of(sale("wide", "A", START, MAX_MONEY)), temp);
        assertConcentration(rows(temp, CONCENTRATION).getFirst(), 1, 1, 1, MAX_MONEY, MAX_MONEY, MAX_MONEY);

        Path smallStage = Files.createDirectory(temp.resolve("small"));
        SalesProfileAnalytics.run(List.of(sale("small", "B", START, "9e-38")), smallStage);
        String smallestScale = "0." + "0".repeat(37) + "9";
        assertConcentration(rows(smallStage, CONCENTRATION).getFirst(), 1, 1, 1,
                smallestScale, smallestScale, smallestScale);
    }

    @Test void rankingKeepsDifferencesAtTheThirtyEighthDecimalPlace() throws Exception {
        String lower = "0.2" + "0".repeat(36) + "1";
        String higher = "0.2" + "0".repeat(36) + "2";
        String total = "0.4" + "0".repeat(36) + "3";
        SalesProfileAnalytics.run(List.of(
                sale("first-in-ties", "A", START, lower),
                sale("higher", "B", START, higher)), temp);

        assertConcentration(rows(temp, CONCENTRATION).getFirst(), 1, 2, 1, higher, total, total);
    }

    @Test void unrepresentableAmountsFailExactPreflight() {
        var scale = assertThrows(IOException.class,
                () -> SalesProfileAnalytics.run(List.of(sale("tiny", null, START, "1e-39")), temp));
        assertTrue(scale.getMessage().contains("scale exceeds 38"));
        var width = assertThrows(IOException.class,
                () -> SalesProfileAnalytics.run(List.of(sale("huge", "A", START, "1e38")), temp));
        assertTrue(width.getMessage().contains("coefficient exceeds 38 digits"));
        var mixed = assertThrows(IOException.class, () -> SalesProfileAnalytics.run(List.of(
                sale("one", "A", START, "1"), sale("tiny", null, START, "1e-38")), temp));
        assertTrue(mixed.getMessage().contains("coefficient exceeds 38 digits"));
    }

    @Test void groupedAndAllSalesSumsFailOnExactOverflow() throws Exception {
        var wide = sale("wide", "A", START, MAX_MONEY);
        assertAggregateOverflow(List.of(wide, sale("same-buyer", "A", START.plusDays(1), "1")), temp);
        Path anotherStage = Files.createDirectory(temp.resolve("another"));
        assertAggregateOverflow(List.of(wide, sale("anonymous", null, START, "1")), anotherStage);
    }

    @Test void bothQueriesUseSalesFromEveryPreparedBatch() throws Exception {
        var orders = new ArrayList<OrdersDataset.Order>();
        for (int i = 0; i < 2050; i++) {
            orders.add(sale("sale-" + i, "buyer-" + i, START, "1"));
        }
        orders.add(sale("repeat", "buyer-2049", START.plusDays(1), "2"));
        SalesProfileAnalytics.run(orders, temp);

        assertInterval(rows(temp, INTERVALS).getFirst(), "first", 1, 1, DAY, DAY, 0, 1, 0, 0, 0);
        var concentration = rows(temp, CONCENTRATION);
        assertConcentration(concentration.get(0), 1, 2050, 21, "23", "2052", "2052");
        assertConcentration(concentration.get(1), 5, 2050, 103, "105", "2052", "2052");
        assertConcentration(concentration.get(2), 10, 2050, 205, "207", "2052", "2052");
    }

    private static OrdersDataset.Order sale(String invoice, String customer, LocalDateTime time, String amount) {
        return new OrdersDataset.Order(invoice, customer, time, "sale", 1, BigInteger.ONE, 1, amount);
    }

    private static OrdersDataset.Order cancellation(String invoice, String customer, LocalDateTime time, String amount) {
        return new OrdersDataset.Order(invoice, customer, time, "cancellation", 1, BigInteger.ONE.negate(), 1, amount);
    }

    private static List<JsonNode> rows(Path stage, String file) throws IOException {
        var result = new ArrayList<JsonNode>();
        for (String line : Files.readAllLines(stage.resolve(file))) {
            result.add(StrictJson.parse(line));
        }
        return result;
    }

    private void assertEmptyIntervals() throws IOException {
        var intervals = rows(temp, INTERVALS);
        assertEquals(2, intervals.size());
        assertInterval(intervals.get(0), "first", 0, 0, null, null, 0, 0, 0, 0, 0);
        assertInterval(intervals.get(1), "later", 0, 0, null, null, 0, 0, 0, 0, 0);
    }

    private static void assertInterval(JsonNode row, String segment, long count, long customers, Long min, Long max,
                                       long... bucketCounts) throws IOException {
        StrictJson.keys(row, "schema", "schema_version", "segment", "interval_count", "customer_count",
                "min_gap_microseconds", "max_gap_microseconds", "buckets");
        assertSchema(row);
        assertEquals(segment, StrictJson.text(row.get("segment")));
        assertCount(row.get("interval_count"), count);
        assertCount(row.get("customer_count"), customers);
        assertNullableCount(row.get("min_gap_microseconds"), min);
        assertNullableCount(row.get("max_gap_microseconds"), max);
        JsonNode buckets = row.get("buckets");
        assertTrue(buckets.isArray());
        assertEquals(5, buckets.size());
        for (int i = 0; i < BOUNDS.length; i++) {
            JsonNode bucket = buckets.get(i);
            StrictJson.keys(bucket, "lower_microseconds", "upper_microseconds", "interval_count");
            assertCount(bucket.get("lower_microseconds"), BOUNDS[i]);
            assertNullableCount(bucket.get("upper_microseconds"), i == BOUNDS.length - 1 ? null : BOUNDS[i + 1]);
            assertCount(bucket.get("interval_count"), bucketCounts[i]);
        }
    }

    private static void assertConcentration(JsonNode row, int percent, long eligible, long top, String topSales,
                                            String knownSales, String allSales) throws IOException {
        StrictJson.keys(row, "schema", "schema_version", "top_percent", "eligible_buyers", "top_buyers",
                "top_sales", "known_sales", "all_sales", "share_of_known_sales", "share_of_all_sales");
        assertSchema(row);
        assertCount(row.get("top_percent"), percent);
        assertCount(row.get("eligible_buyers"), eligible);
        assertCount(row.get("top_buyers"), top);
        assertEquals(topSales, StrictJson.text(row.get("top_sales")));
        assertEquals(knownSales, StrictJson.text(row.get("known_sales")));
        assertEquals(allSales, StrictJson.text(row.get("all_sales")));
        assertShare(row.get("share_of_known_sales"), topSales, knownSales);
        assertShare(row.get("share_of_all_sales"), topSales, allSales);
    }

    private static void assertSchema(JsonNode row) throws IOException {
        assertEquals("repeatlab-sales-profile", StrictJson.text(row.get("schema")));
        StrictJson.version(row);
    }

    private static void assertShare(JsonNode row, String numerator, String denominator) throws IOException {
        if (denominator.matches("0(?:\\.0+)?")) {
            assertTrue(row.isNull());
            return;
        }
        StrictJson.keys(row, "numerator", "denominator");
        assertEquals(numerator, StrictJson.text(row.get("numerator")));
        assertEquals(denominator, StrictJson.text(row.get("denominator")));
    }

    private static void assertCount(JsonNode node, long expected) throws IOException {
        assertEquals(expected, StrictJson.integer(node, 0, Long.MAX_VALUE));
    }

    private static void assertNullableCount(JsonNode node, Long expected) throws IOException {
        if (expected == null) {
            assertTrue(node.isNull());
        } else {
            assertCount(node, expected);
        }
    }

    private static void assertAggregateOverflow(List<OrdersDataset.Order> orders, Path stage) {
        var error = assertThrows(SQLException.class, () -> SalesProfileAnalytics.run(orders, stage));
        assertTrue(error.getMessage().contains("DECIMAL(38"));
        assertFalse(Files.exists(stage.resolve(CONCENTRATION)));
    }
}
