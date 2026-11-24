package io.repeatlab.ingestion;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ProductRepeatsAnalyticsTest {
    private static final LocalDateTime START = LocalDateTime.parse("2009-12-01T00:00:00");
    private static final String ARTIFACT = "product_repeats.jsonl";
    @TempDir Path temp;

    @Test void onlyEarlierPositivePurchasesByTheSameBuyerProvideProductHistory() throws Exception {
        var orders = new ArrayList<>(List.of(
                order("18", "A", START.plusDays(1), "sale", 2, 2),
                order("17", "A", START.plusNanos(1000), "sale", 4, 2),
                order("16", "A", START, "sale", 1, 1),
                order("15", "A", START, "sale", 1, 1),
                order("14", "B", START.minusDays(1), "sale", 1, 1),
                order("13", null, START.minusDays(2), "sale", 1, 1),
                order("C12", "A", START.minusDays(3), "cancellation", 1, 1)));
        var products = Map.of("15", Set.of("P"), "16", Set.of("P"), "17", Set.of("P", "Q"),
                "18", Set.of("X", "Y"), "14", Set.of("X"), "13", Set.of("X"), "C12", Set.of("Y"));

        var result = ProductRepeatsAnalytics.run(orders, products, temp);
        assertFalse(result.engine().isBlank());
        assertEquals(Set.of("product_repeats.sql"), result.sqlHashes().keySet());
        assertEquals(5, result.rows());
        var rows = OrdersDataset.rows(temp.resolve(ARTIFACT));
        assertEquals(5, rows.size());
        assertOrder(rows.get(0), "14", "B", "2009-11-30T00:00:00", false, 1, 0);
        assertOrder(rows.get(1), "15", "A", "2009-12-01T00:00:00", false, 1, 0);
        assertOrder(rows.get(2), "16", "A", "2009-12-01T00:00:00", false, 1, 0);
        assertOrder(rows.get(3), "17", "A", "2009-12-01T00:00:00.000001", true, 2, 1);
        assertOrder(rows.get(4), "18", "A", "2009-12-02T00:00:00", true, 2, 0);
        Collections.reverse(orders);
        Path reversed = Files.createDirectory(temp.resolve("reversed"));
        assertEquals(result, ProductRepeatsAnalytics.run(orders, products, reversed));
        assertArrayEquals(Files.readAllBytes(temp.resolve(ARTIFACT)), Files.readAllBytes(reversed.resolve(ARTIFACT)));
    }

    @Test void identifiersRemainLiteralThroughOrderingGroupingAndOutput() throws Exception {
        String buyer = "x\0大", other = "x\0д";
        String first = "a\0A", second = "a\0B", third = "\uE000", last = "🚲";
        var orders = List.of(order(last, buyer, START.plusSeconds(2), "sale", 1, 1),
                order(third, other, START.plusSeconds(1), "sale", 1, 1),
                order(second, buyer, START.plusSeconds(1), "sale", 4, 4),
                order(first, buyer, START, "sale", 2, 2));
        var products = Map.of(first, Set.of("P\0A", "é"), second, Set.of("P\0A", "P\0B", "e\u0301", "É"),
                third, Set.of("P\0A"), last, Set.of("P\0B"));

        ProductRepeatsAnalytics.run(orders, products, temp);
        var rows = OrdersDataset.rows(temp.resolve(ARTIFACT));
        assertEquals(4, rows.size());
        assertOrder(rows.get(0), first, buyer, "2009-12-01T00:00:00", false, 2, 0);
        assertOrder(rows.get(1), second, buyer, "2009-12-01T00:00:01", true, 4, 1);
        assertOrder(rows.get(2), third, other, "2009-12-01T00:00:01", false, 1, 0);
        assertOrder(rows.get(3), last, buyer, "2009-12-01T00:00:02", true, 1, 1);
    }

    @Test void aBasketLargerThanOneBatchKeepsItsWholeHistory() throws Exception {
        var codes = new HashSet<String>();
        for (int i = 0; i < 2050; i++) codes.add("product-" + i);
        var orders = List.of(order("1", "A", START, "sale", 2050, 2050),
                order("2", "A", START.plusDays(1), "sale", 2050, 2050));

        ProductRepeatsAnalytics.run(orders, Map.of("1", codes, "2", codes), temp);
        var rows = OrdersDataset.rows(temp.resolve(ARTIFACT));
        assertOrder(rows.get(0), "1", "A", "2009-12-01T00:00:00", false, 2050, 0);
        assertOrder(rows.get(1), "2", "A", "2009-12-02T00:00:00", true, 2050, 2050);
    }

    @Test void noIdentifiedSalesProduceAnEmptyArtifact() throws Exception {
        var orders = List.of(order("1", null, START, "sale", 1, 1),
                order("C2", "A", START, "cancellation", 1, 1));
        var result = ProductRepeatsAnalytics.run(orders, Map.of("1", Set.of("Q"), "C2", Set.of("Q")), temp);
        assertEquals(0, result.rows());
        assertEquals("", Files.readString(temp.resolve(ARTIFACT)));
        assertTrue(Files.notExists(temp.resolve("duckdb-temp")));
    }

    private static OrdersDataset.Order order(String key, String customer, LocalDateTime time, String kind,
                                            int lines, int distinct) {
        return new OrdersDataset.Order(key, customer, time, kind, lines,
                BigInteger.valueOf(kind.equals("sale") ? lines : -lines), distinct,
                kind.equals("sale") ? "1e5000" : "-1e5000");
    }

    private static void assertOrder(JsonNode row, String key, String customer, String time,
                                    boolean prior, long distinct, long repeated) throws Exception {
        var expected = Json.object("schema", "repeatlab-product-repeats", "schema_version", 1,
                "order_key", key, "customer_id", customer, "event_time", time, "has_prior_purchase", prior,
                "distinct_products", distinct, "repeated_products", repeated, "repeat_share",
                prior ? Json.object("numerator", repeated, "denominator", distinct) : null);
        assertEquals(StrictJson.parse(Json.encode(expected)), row);
    }
}
