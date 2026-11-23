package io.repeatlab.ingestion;

import java.io.IOException;
import java.io.Writer;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/** Executes both views over verified sales within the declared observation interval. */
final class SalesProfileAnalytics {
    record Result(String engine, int decimalScale, Map<String, String> sqlHashes,
                  long intervalRows, long concentrationRows) {}

    @FunctionalInterface
    private interface ArtifactWriter {
        long write(ResultSet rows, Writer writer) throws IOException, SQLException;
    }

    private static final String SCHEMA = "repeatlab-sales-profile";
    private static final int INSERT_BATCH_SIZE = 1024;

    private SalesProfileAnalytics() {}

    static Result run(List<OrdersDataset.Order> orders, Path stage) throws IOException, SQLException {
        List<OrdersDataset.Order> sales = orders.stream().filter(order -> order.kind().equals("sale")).toList();
        int scale = SqlAnalytics.scale(sales);
        Path spill = Files.createDirectory(stage.resolve("duckdb-temp"));
        Properties config = new Properties();
        config.setProperty("memory_limit", "1GB");
        config.setProperty("threads", "1");
        config.setProperty("temp_directory", spill.toAbsolutePath().toString());
        Result result;
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:", config)) {
            connection.setAutoCommit(false);
            String version;
            try (var query = connection.createStatement(); var rows = query.executeQuery("SELECT version()")) {
                if (!rows.next()) throw new SQLException("DuckDB version is unavailable");
                version = rows.getString(1);
            }
            loadSales(connection, sales, scale);
            Map<String, String> hashes = new LinkedHashMap<>();
            long intervals = writeArtifact(connection, stage, "purchase_intervals", hashes,
                    SalesProfileAnalytics::writeIntervals);
            long concentration = writeArtifact(connection, stage, "sales_concentration", hashes,
                    (rows, writer) -> writeConcentration(rows, writer, scale));
            connection.commit();
            result = new Result(version, scale, Map.copyOf(hashes), intervals, concentration);
        }
        AnalyzeCommand.removeTree(spill);
        return result;
    }

    private static void loadSales(Connection connection, List<OrdersDataset.Order> sales, int scale)
            throws SQLException {
        String amountType = "DECIMAL(38," + scale + ")";
        try (var query = connection.createStatement()) {
            query.execute("CREATE TABLE sales (customer_id VARCHAR, event_time TIMESTAMP NOT NULL, "
                    + "total_amount " + amountType + " NOT NULL)");
        }
        String sql = "INSERT INTO sales VALUES (?, CAST(? AS TIMESTAMP), CAST(? AS " + amountType + "))";
        try (var insert = connection.prepareStatement(sql)) {
            BigDecimal allSales = BigDecimal.ZERO;
            int pending = 0;
            for (var sale : sales) {
                BigDecimal amount = new BigDecimal(sale.amount()).setScale(scale);
                // Each grouped and ranked sum is bounded by this exact sum of positive amounts.
                allSales = allSales.add(amount);
                if (allSales.precision() > 38) throw new SQLException("SQL aggregate exceeds " + amountType);
                if (sale.customer() == null) insert.setNull(1, Types.VARCHAR);
                else insert.setString(1, sale.customer());
                insert.setString(2, sale.time().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
                insert.setString(3, amount.toPlainString());
                insert.addBatch();
                if (++pending == INSERT_BATCH_SIZE) {
                    insert.executeBatch();
                    pending = 0;
                }
            }
            if (pending > 0) insert.executeBatch();
        }
    }

    private static long writeArtifact(Connection connection, Path stage, String name, Map<String, String> hashes,
                                      ArtifactWriter encoder) throws IOException, SQLException {
        String sqlName = name + ".sql";
        String sql;
        try (var input = SalesProfileAnalytics.class.getResourceAsStream("/sql/" + sqlName)) {
            if (input == null) throw new IOException("Missing packaged SQL: " + sqlName);
            sql = new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
        hashes.put(sqlName, Json.hashText(sql));
        try (var query = connection.createStatement(); var rows = query.executeQuery(sql);
             var writer = Files.newBufferedWriter(stage.resolve(name + ".jsonl"), StandardCharsets.UTF_8,
                     StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            return encoder.write(rows, writer);
        }
    }

    private static long writeIntervals(ResultSet rows, Writer writer) throws IOException, SQLException {
        String segment = null;
        Map<String, Object> interval = null;
        List<Map<String, Object>> buckets = new ArrayList<>();
        long count = 0;
        while (rows.next()) {
            String nextSegment = rows.getString("segment");
            if (!nextSegment.equals(segment)) {
                if (interval != null) Json.line(writer, interval);
                segment = nextSegment;
                buckets = new ArrayList<>();
                interval = Json.object("schema", SCHEMA, "schema_version", 1,
                        "segment", segment, "interval_count", rows.getLong("interval_count"),
                        "customer_count", rows.getLong("customer_count"),
                        "min_gap_microseconds", nullableLong(rows, "min_gap_microseconds"),
                        "max_gap_microseconds", nullableLong(rows, "max_gap_microseconds"), "buckets", buckets);
                count++;
            }
            buckets.add(Json.object("lower_microseconds", rows.getLong("lower_microseconds"),
                    "upper_microseconds", nullableLong(rows, "upper_microseconds"),
                    "interval_count", rows.getLong("bucket_interval_count")));
        }
        if (interval != null) Json.line(writer, interval);
        return count;
    }

    private static Long nullableLong(ResultSet rows, String field) throws SQLException {
        long value = rows.getLong(field);
        return rows.wasNull() ? null : value;
    }

    private static long writeConcentration(ResultSet rows, Writer writer, int scale) throws IOException, SQLException {
        long count = 0;
        while (rows.next()) {
            String topSales = exactAmount(rows, "top_sales", scale).toPlainString();
            BigDecimal knownSales = exactAmount(rows, "known_sales", scale);
            BigDecimal allSales = exactAmount(rows, "all_sales", scale);
            Json.line(writer, Json.object("schema", SCHEMA, "schema_version", 1,
                    "top_percent", rows.getInt("top_percent"), "eligible_buyers", rows.getLong("eligible_buyers"),
                    "top_buyers", rows.getLong("top_buyers"), "top_sales", topSales,
                    "known_sales", knownSales.toPlainString(), "all_sales", allSales.toPlainString(),
                    "share_of_known_sales", share(topSales, knownSales),
                    "share_of_all_sales", share(topSales, allSales)));
            count++;
        }
        return count;
    }

    private static BigDecimal exactAmount(ResultSet rows, String field, int scale) throws SQLException {
        BigDecimal amount = rows.getBigDecimal(field);
        if (amount == null) return BigDecimal.ZERO.setScale(scale);
        try {
            amount = amount.setScale(scale, RoundingMode.UNNECESSARY);
        } catch (ArithmeticException invalid) {
            throw new SQLException("SQL aggregate scale is not exact for " + field, invalid);
        }
        if (amount.precision() > 38) throw new SQLException("SQL aggregate exceeds DECIMAL(38)");
        return amount;
    }

    private static Map<String, Object> share(String numerator, BigDecimal denominator) {
        return denominator.signum() == 0 ? null
                : Json.object("numerator", numerator, "denominator", denominator.toPlainString());
    }
}
