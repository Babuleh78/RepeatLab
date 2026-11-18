package io.repeatlab.ingestion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CohortAnalyticsTest {
    @TempDir Path temp;

    @Test void repeatRatesAndCohortActivityMatchHandCalculation() throws Exception {
        var result = run(List.of(
                sale("a1", "A", "2020-01-10T00:00:00"),
                sale("a2", "A", "2020-01-10T00:00:00"),
                sale("a3", "A", "2020-02-09T00:00:00"),
                sale("a4", "A", "2020-02-10T00:00:00"),
                sale("a5", "A", "2020-04-02T00:00:00"),
                sale("c1", "C", "2020-01-10T00:00:00"),
                cancellation("c2", "C", "2020-02-15T00:00:00"),
                cancellation("d1", "D", "2020-02-15T00:00:00"),
                sale("b1", "B", "2020-03-25T00:00:00"),
                sale("b2", "B", "2020-03-26T00:00:00"),
                sale("unknown", null, "2020-01-11T00:00:00")),
                "2020-01-01T00:00:00", "2020-04-15T00:00:00");

        var rates = rows("repeat_rates.jsonl");
        assertEquals(3, rates.size());
        assertRate(rates.get(0), 30, 3, 2, 1, 0);
        assertRate(rates.get(1), 60, 3, 2, 1, 1);
        assertRate(rates.get(2), 90, 3, 2, 1, 1);

        var activity = rows("cohort_activity.jsonl");
        assertEquals(6, activity.size());
        assertActivity(activity.get(0), "2020-01", 0, "2020-01", 2, 2L);
        assertActivity(activity.get(1), "2020-01", 1, "2020-02", 2, 1L);
        assertActivity(activity.get(2), "2020-01", 2, "2020-03", 2, 0L);
        assertActivity(activity.get(3), "2020-01", 3, "2020-04", 2, null);
        assertActivity(activity.get(4), "2020-03", 0, "2020-03", 1, 1L);
        assertActivity(activity.get(5), "2020-03", 1, "2020-04", 1, null);

        assertEquals(3, result.repeatRows());
        assertEquals(6, result.activityRows());
        assertEquals(Set.of("repeat_rates.sql", "cohort_activity.sql"), result.sqlHashes().keySet());
        for (var hash : result.sqlHashes().entrySet()) {
            try (var input = CohortAnalyticsTest.class.getResourceAsStream("/sql/" + hash.getKey())) {
                byte[] bytes = java.util.Objects.requireNonNull(input, hash.getKey()).readAllBytes();
                assertEquals(Json.hashText(new String(bytes, StandardCharsets.UTF_8)), hash.getValue());
            }
        }
        assertTrue(Files.notExists(temp.resolve("duckdb-temp")));
    }

    @Test void repeatUsesExclusiveHorizonAndExactMicroseconds() throws Exception {
        String anchor = "2020-01-31T12:34:56.123456";
        run(List.of(
                sale("a1", "before", anchor),
                sale("a2", "before", "2020-03-01T12:34:56.123455"),
                sale("b1", "at", anchor),
                sale("b2", "at", "2020-03-01T12:34:56.123456"),
                sale("c1", "after", anchor),
                sale("c2", "after", "2020-03-01T12:34:56.123457"),
                sale("d1", "tie", anchor),
                sale("d2", "tie", anchor),
                sale("e1", "strictly-later", anchor),
                sale("e2", "strictly-later", "2020-01-31T12:34:56.123457")),
                anchor, "2020-05-01T00:00:00");

        var rates = rows("repeat_rates.jsonl");
        assertRate(rates.get(0), 30, 5, 5, 0, 2);
        assertRate(rates.get(1), 60, 5, 5, 0, 4);
        assertRate(rates.get(2), 90, 5, 5, 0, 4);
    }

    @Test void aHorizonEndingAtTheCutoffIsEligible() throws Exception {
        run(List.of(
                sale("a1", "complete", "2020-01-02T12:34:56.123456"),
                sale("a2", "complete", "2020-01-03T12:34:56.123456"),
                sale("b1", "incomplete", "2020-01-02T12:34:56.123457"),
                sale("b2", "incomplete", "2020-01-03T12:34:56.123456")),
                "2020-01-01T00:00:00", "2020-02-01T12:34:56.123456");

        var rates = rows("repeat_rates.jsonl");
        assertRate(rates.get(0), 30, 2, 1, 1, 1);
        assertRate(rates.get(1), 60, 2, 0, 2, 0);
        assertRate(rates.get(2), 90, 2, 0, 2, 0);
    }

    @Test void partialFirstMonthAndCutoffAtMonthStartKeepZeroDistinctFromUnknown() throws Exception {
        run(List.of(sale("a1", "A", "0001-01-02T00:00:00"),
                    sale("b1", "B", "0001-02-01T00:00:00")),
                "0001-01-02T00:00:00", "0001-03-01T00:00:00");

        var activity = rows("cohort_activity.jsonl");
        assertEquals(3, activity.size());
        assertActivity(activity.get(0), "0001-01", 0, "0001-01", 1, null);
        assertActivity(activity.get(1), "0001-01", 1, "0001-02", 1, 0L);
        assertActivity(activity.get(2), "0001-02", 0, "0001-02", 1, 1L);
    }

    @Test void calendarCanCrossTheLastSupportedInputYearWhenCheckingMaturity() throws Exception {
        run(List.of(sale("a1", "A", "9999-12-01T00:00:00"),
                    sale("a2", "A", "9999-12-10T00:00:00")),
                "9999-11-01T00:00:00", "9999-12-31T23:59:59.999999");

        var rates = rows("repeat_rates.jsonl");
        assertRate(rates.get(0), 30, 1, 1, 0, 1);
        assertRate(rates.get(1), 60, 1, 0, 1, 0);
        assertRate(rates.get(2), 90, 1, 0, 1, 0);
        var activity = rows("cohort_activity.jsonl");
        assertEquals(1, activity.size());
        assertActivity(activity.getFirst(), "9999-12", 0, "9999-12", 1, null);
    }

    @Test void emptyInputHasThreeNullRatesAndNoCohorts() throws Exception {
        run(List.of(), "2020-01-01T00:00:00", "2020-04-15T00:00:00");

        var rates = rows("repeat_rates.jsonl");
        assertEquals(3, rates.size());
        assertRate(rates.get(0), 30, 0, 0, 0, 0);
        assertRate(rates.get(1), 60, 0, 0, 0, 0);
        assertRate(rates.get(2), 90, 0, 0, 0, 0);
        assertEquals(List.of(), rows("cohort_activity.jsonl"));
    }

    @Test void customerCountsSurviveMultipleInsertBatches() throws Exception {
        String anchor = "2020-01-01T00:00:00";
        List<OrdersDataset.Order> orders = new ArrayList<>();
        for (int i = 0; i < 2050; i++) {
            orders.add(sale("base-" + i, "customer-" + i, anchor));
        }
        orders.add(sale("repeat", "customer-2049", "2020-01-02T00:00:00"));
        run(orders, anchor, "2020-02-01T00:00:00");
        assertRate(rows("repeat_rates.jsonl").getFirst(), 30, 2050, 2050, 0, 1);
        assertActivity(rows("cohort_activity.jsonl").getFirst(), "2020-01", 0, "2020-01", 2050, 2050L);
    }

    private CohortAnalytics.Result run(List<OrdersDataset.Order> orders, String from, String until) throws Exception {
        return CohortAnalytics.run(orders, LocalDateTime.parse(from), LocalDateTime.parse(until), temp);
    }

    private List<JsonNode> rows(String filename) throws IOException {
        List<JsonNode> rows = new ArrayList<>();
        for (String line : Files.readAllLines(temp.resolve(filename))) {
            rows.add(StrictJson.parse(line));
        }
        return rows;
    }

    private static void assertRate(JsonNode row, int horizon, long total, long eligible,
                                   long censored, long repeats) throws IOException {
        StrictJson.keys(row, "schema", "schema_version", "horizon_days", "total_customers",
                "eligible_customers", "censored_customers", "repeat_customers", "repeat_share");
        assertEquals("repeatlab-cohorts", row.get("schema").asText());
        StrictJson.version(row);
        assertCount(row, "horizon_days", horizon);
        assertCount(row, "total_customers", total);
        assertCount(row, "eligible_customers", eligible);
        assertCount(row, "censored_customers", censored);
        assertCount(row, "repeat_customers", repeats);
        if (eligible == 0) {
            assertTrue(row.get("repeat_share").isNull());
        } else {
            assertShare(row.get("repeat_share"), repeats, eligible);
        }
    }

    private static void assertActivity(JsonNode row, String cohort, int age, String month,
                                       long customers, Long active) throws IOException {
        StrictJson.keys(row, "schema", "schema_version", "cohort_month", "age_months", "activity_month",
                "cohort_customers", "status", "active_customers", "activity_share");
        assertEquals("repeatlab-cohorts", row.get("schema").asText());
        StrictJson.version(row);
        assertEquals(cohort, row.get("cohort_month").asText());
        assertCount(row, "age_months", age);
        assertEquals(month, row.get("activity_month").asText());
        assertCount(row, "cohort_customers", customers);
        if (active == null) {
            assertEquals("censored", row.get("status").asText());
            assertTrue(row.get("active_customers").isNull());
            assertTrue(row.get("activity_share").isNull());
        } else {
            assertEquals("complete", row.get("status").asText());
            assertCount(row, "active_customers", active);
            assertShare(row.get("activity_share"), active, customers);
        }
    }

    private static void assertShare(JsonNode share, long numerator, long denominator) throws IOException {
        StrictJson.keys(share, "numerator", "denominator");
        assertCount(share, "numerator", numerator);
        assertCount(share, "denominator", denominator);
    }

    private static void assertCount(JsonNode row, String field, long expected) throws IOException {
        assertEquals(expected, StrictJson.integer(row.get(field), 0, Long.MAX_VALUE), field);
    }

    private static OrdersDataset.Order sale(String invoice, String customer, String time) {
        return new OrdersDataset.Order(invoice, customer, LocalDateTime.parse(time), "sale",
                1, BigInteger.ONE, 1, "1E+3000");
    }

    private static OrdersDataset.Order cancellation(String invoice, String customer, String time) {
        return new OrdersDataset.Order(invoice, customer, LocalDateTime.parse(time), "cancellation",
                1, BigInteger.ONE.negate(), 1, "-1");
    }
}
