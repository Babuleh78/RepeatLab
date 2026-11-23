package io.repeatlab.ingestion;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/** Executes the cohort queries over observed sales without loading quantities or money. */
final class CohortAnalytics {
    record Result(String engine, Map<String, String> sqlHashes, long repeatRows, long activityRows) {}

    @FunctionalInterface
    private interface Row {
        Map<String, Object> encode(ResultSet row) throws SQLException;
    }

    private static final int INSERT_BATCH_SIZE = 1024;

    private CohortAnalytics() {}

    static Result run(List<OrdersDataset.Order> orders, LocalDateTime observedFrom,
                      LocalDateTime observedUntil, Path stage) throws IOException, SQLException {
        Path spill = Files.createDirectory(stage.resolve("duckdb-temp"));
        Properties config = new Properties();
        config.setProperty("memory_limit", "1GB");
        config.setProperty("threads", "1");
        config.setProperty("temp_directory", spill.toAbsolutePath().toString());
        Result result;
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:", config)) {
            String version;
            try (var query = connection.createStatement(); var rows = query.executeQuery("SELECT version()")) {
                if (!rows.next()) throw new SQLException("DuckDB version is unavailable");
                version = rows.getString(1);
            }
            loadSales(connection, orders);
            Map<String, String> sqlHashes = new LinkedHashMap<>();
            long repeatRows = writeRows(connection, stage, "repeat_rates", sqlHashes,
                    CohortAnalytics::repeatRow, observedUntil);
            long activityRows = writeRows(connection, stage, "cohort_activity", sqlHashes,
                    CohortAnalytics::activityRow, observedFrom, observedUntil);
            result = new Result(version, Map.copyOf(sqlHashes), repeatRows, activityRows);
        }
        AnalyzeCommand.removeTree(spill);
        return result;
    }

    private static void loadSales(Connection connection, List<OrdersDataset.Order> orders) throws SQLException {
        try (var query = connection.createStatement()) {
            query.execute("CREATE TABLE sales (customer_id VARCHAR NOT NULL, event_time TIMESTAMP NOT NULL)");
        }
        connection.setAutoCommit(false);
        try (var insert = connection.prepareStatement("INSERT INTO sales VALUES (?, CAST(? AS TIMESTAMP))")) {
            int pending = 0;
            for (var order : orders) {
                if (order.customer() == null || !order.kind().equals("sale")) continue;
                insert.setString(1, order.customer());
                insert.setString(2, order.time().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
                insert.addBatch();
                if (++pending == INSERT_BATCH_SIZE) {
                    insert.executeBatch();
                    pending = 0;
                }
            }
            if (pending > 0) insert.executeBatch();
        }
        connection.commit();
        connection.setAutoCommit(true);
    }

    private static long writeRows(Connection connection, Path stage, String name, Map<String, String> sqlHashes,
                                  Row encoder, LocalDateTime... bounds) throws IOException, SQLException {
        String sqlName = name + ".sql";
        String sql;
        try (var input = CohortAnalytics.class.getResourceAsStream("/sql/" + sqlName)) {
            if (input == null) throw new IOException("Missing packaged SQL: " + sqlName);
            sql = new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
        sqlHashes.put(sqlName, Json.hashText(sql));
        long count = 0;
        try (var query = connection.prepareStatement(sql)) {
            for (int i = 0; i < bounds.length; i++) {
                query.setString(i + 1, bounds[i].format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
            }
            try (var rows = query.executeQuery();
                 var writer = Files.newBufferedWriter(stage.resolve(name + ".jsonl"), StandardCharsets.UTF_8,
                         StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
                while (rows.next()) {
                    Json.line(writer, encoder.encode(rows));
                    count = Math.incrementExact(count);
                }
            }
        }
        return count;
    }

    private static Map<String, Object> repeatRow(ResultSet row) throws SQLException {
        long repeats = row.getLong("repeat_customers");
        long eligible = row.getLong("eligible_customers");
        return Json.object("schema", "repeatlab-cohorts", "schema_version", 1,
                "horizon_days", row.getInt("horizon_days"), "total_customers", row.getLong("total_customers"),
                "eligible_customers", eligible, "censored_customers", row.getLong("censored_customers"),
                "repeat_customers", repeats, "repeat_share", share(repeats, eligible));
    }

    private static Map<String, Object> activityRow(ResultSet row) throws SQLException {
        long active = row.getLong("active_customers");
        boolean observed = !row.wasNull();
        long cohort = row.getLong("cohort_customers");
        return Json.object("schema", "repeatlab-cohorts", "schema_version", 1,
                "cohort_month", row.getString("cohort_month"), "age_months", row.getLong("age_months"),
                "activity_month", row.getString("activity_month"), "cohort_customers", cohort,
                "status", row.getString("status"), "active_customers", observed ? active : null,
                "activity_share", observed ? share(active, cohort) : null);
    }

    private static Map<String, Object> share(long numerator, long denominator) {
        return denominator == 0 ? null : Json.object("numerator", numerator, "denominator", denominator);
    }
}
