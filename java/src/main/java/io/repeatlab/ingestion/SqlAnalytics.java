package io.repeatlab.ingestion;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/** In-memory SQL execution with an explicit exact DECIMAL boundary. */
final class SqlAnalytics {
    record Result(int scale, String engine, Map<String, String> sqlHashes, long monthlyRows, long missingRows) {}
    record DecimalShape(BigInteger scale, int precision) {}
    private static final int INSERT_BATCH_SIZE = 1024;
    private SqlAnalytics() {}

    static DecimalShape shape(String text) throws IOException {
        StrictJson.require(OrdersDataset.DECIMAL.matcher(text).matches(), "Invalid exact amount");
        String[] parts = text.split("[eE]", 2);
        String coefficient = parts[0].replace("+", "").replace("-", "");
        int point = coefficient.indexOf('.');
        int fraction = point < 0 ? 0 : coefficient.length() - point - 1;
        String digits = coefficient.replace(".", "");
        int start = 0, end = digits.length();
        while (start < end && digits.charAt(start) == '0') start++;
        if (start == end) return new DecimalShape(BigInteger.ZERO, 1);
        while (end > start && digits.charAt(end - 1) == '0') end--;
        BigInteger exponent = parts.length == 1 ? BigInteger.ZERO : new BigInteger(parts[1]);
        return new DecimalShape(BigInteger.valueOf((long) fraction - (digits.length() - end)).subtract(exponent), end - start);
    }

    static int scale(List<OrdersDataset.Order> orders) throws IOException {
        BigInteger scale = BigInteger.ZERO;
        for (var order : orders) scale = scale.max(shape(order.amount()).scale());
        StrictJson.require(scale.compareTo(BigInteger.valueOf(38)) <= 0, "SQL exact scale exceeds 38");
        for (var order : orders) {
            DecimalShape shape = shape(order.amount());
            BigInteger width = BigInteger.valueOf(shape.precision()).add(scale).subtract(shape.scale());
            StrictJson.require(width.compareTo(BigInteger.valueOf(38)) <= 0, "SQL exact coefficient exceeds 38 digits");
        }
        return scale.intValueExact();
    }

    static Result run(List<OrdersDataset.Order> orders, Path stage) throws IOException, SQLException {
        int scale = scale(orders);
        Path spill = Files.createDirectory(stage.resolve("duckdb-temp"));
        Properties config = new Properties();
        config.setProperty("memory_limit", "1GB"); config.setProperty("threads", "1");
        config.setProperty("temp_directory", spill.toAbsolutePath().toString());
        Map<String, String> hashes = new LinkedHashMap<>();
        String version;
        long monthly, missing;
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:", config)) {
            try (var statement = connection.createStatement(); var rows = statement.executeQuery("SELECT version()")) {
                rows.next(); version = rows.getString(1);
            }
            try (var statement = connection.createStatement()) {
                statement.execute("CREATE TABLE orders (invoice_id VARCHAR, customer_id VARCHAR, event_time TIMESTAMP, kind VARCHAR, product_line_count BIGINT, total_amount DECIMAL(38," + scale + "))");
            }
            connection.setAutoCommit(false);
            try (var insert = connection.prepareStatement("INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?)")) {
                int pending = 0;
                for (var order : orders) {
                    insert.setString(1, order.invoice());
                    if (order.customer() == null) insert.setNull(2, Types.VARCHAR); else insert.setString(2, order.customer());
                    // A naive ISO string cast by DuckDB avoids JVM default-timezone conversion.
                    insert.setString(3, order.time().toString()); insert.setString(4, order.kind());
                    insert.setLong(5, order.lines());
                    insert.setBigDecimal(6, new BigDecimal(order.amount()).setScale(scale));
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
            monthly = query(connection, "monthly_activity", stage, scale, hashes);
            missing = query(connection, "missing_customer", stage, scale, hashes);
        }
        // Native connection is closed before its private spill directory is removed.
        AnalyzeCommand.removeTree(spill);
        return new Result(scale, version, hashes, monthly, missing);
    }

    private static long query(Connection connection, String name, Path stage, int scale, Map<String, String> hashes)
            throws IOException, SQLException {
        String script;
        try (var input = SqlAnalytics.class.getResourceAsStream("/sql/" + name + ".sql")) {
            if (input == null) throw new IOException("Missing packaged SQL: " + name);
            script = new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
        hashes.put(name + ".sql", Json.hashText(script));
        long count = 0;
        try (var statement = connection.createStatement(); var rows = statement.executeQuery(script);
             var writer = Files.newBufferedWriter(stage.resolve(name + ".jsonl"))) {
            while (rows.next()) {
                Map<String, Object> row = Json.object("schema", "repeatlab-analytics", "schema_version", 1,
                        "month", rows.getString("month"), "kind", rows.getString("kind"));
                String[] counts = name.equals("monthly_activity")
                        ? new String[]{"order_count", "customer_count", "product_line_count"}
                        : new String[]{"order_count", "missing_customer_order_count", "product_line_count", "missing_customer_line_count"};
                for (String field : counts) row.put(field, rows.getBigDecimal(field).longValueExact());
                String[] amounts = name.equals("monthly_activity")
                        ? new String[]{"total_amount", "identified_total_amount", "missing_customer_total_amount"}
                        : new String[]{"total_amount", "missing_customer_total_amount"};
                for (String field : amounts) row.put(field, exact(rows, field, scale));
                if (name.equals("missing_customer")) {
                    row.put("order_share", ratio(row.get("missing_customer_order_count"), row.get("order_count")));
                    row.put("line_share", ratio(row.get("missing_customer_line_count"), row.get("product_line_count")));
                    row.put("amount_share", ratio(row.get("missing_customer_total_amount"), row.get("total_amount")));
                }
                Json.line(writer, row); count++;
            }
        }
        return count;
    }
    private static String exact(ResultSet rows, String field, int scale) throws SQLException {
        BigDecimal amount = rows.getBigDecimal(field).setScale(scale);
        if (amount.precision() > 38) throw new SQLException("SQL aggregate exceeds DECIMAL(38)");
        return amount.toPlainString();
    }
    private static Object ratio(Object numerator, Object denominator) {
        return new BigDecimal(denominator.toString()).signum() == 0 ? null
                : Json.object("numerator", numerator, "denominator", denominator);
    }
}
