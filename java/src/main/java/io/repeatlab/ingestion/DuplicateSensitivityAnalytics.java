package io.repeatlab.ingestion;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.format.DateTimeParseException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

final class DuplicateSensitivityAnalytics {
    record Result(String engine, Map<String, String> sqlHashes, long rows, long orders,
                  long productLines, long collapsedLines) {}
    private record Counts(long orders, long retained, long collapsed, int scale) {}
    private record Values(long lines, BigInteger units, BigDecimal amount) {
        static Values read(JsonNode node, int sign) throws IOException {
            StrictJson.keys(node, "lines", "units", "amount");
            long lines = StrictJson.integer(node.get("lines"), 1, Long.MAX_VALUE);
            String unitsText = StrictJson.text(node.get("units"));
            StrictJson.require(INTEGER.matcher(unitsText).matches(), "Invalid units");
            BigInteger units = new BigInteger(unitsText);
            StrictJson.require(units.signum() == sign && units.abs().compareTo(BigInteger.valueOf(lines)) >= 0,
                    "Units contradict the order kind or line count");
            String text = StrictJson.text(node.get("amount"));
            StrictJson.require(OrdersDataset.DECIMAL.matcher(text).matches(), "Invalid exact amount");
            BigDecimal amount;
            try {
                amount = new BigDecimal(text).stripTrailingZeros();
            } catch (ArithmeticException | NumberFormatException invalid) {
                throw new IOException("Amount outside SQL DECIMAL(38) range", invalid);
            }
            StrictJson.require(amount.signum() == sign, "Amount contradicts the order kind");
            StrictJson.require(amount.scale() <= 38 && (long) amount.precision() - amount.scale() <= 38,
                    "Amount outside SQL DECIMAL(38) range");
            return new Values(lines, units, amount);
        }

        void bind(PreparedStatement insert, int start) throws SQLException {
            insert.setLong(start, lines);
            insert.setString(start + 1, units.toString());
            insert.setString(start + 2, amount.toPlainString());
        }
    }

    private static final String NAME = "duplicate_sensitivity";
    private static final Pattern INTEGER = Pattern.compile("-?[1-9][0-9]*");
    private static final int BATCH_SIZE = 1024;

    private DuplicateSensitivityAnalytics() {}

    static Result run(Path facts, Path stage) throws IOException, SQLException {
        Path spill = Files.createDirectory(stage.resolve("duckdb-temp"));
        try (Connection connection = DuckDbReports.connect(spill)) {
            String engine = DuckDbReports.version(connection);
            connection.setAutoCommit(false);
            Counts counts = loadFacts(connection, facts);
            String moneyType = "DECIMAL(38," + counts.scale() + ")";
            try (var statement = connection.createStatement()) {
                statement.execute("CREATE TEMP VIEW scenario_orders AS SELECT * REPLACE ("
                        + "CAST(retained_amount AS " + moneyType + ") AS retained_amount, "
                        + "CAST(collapsed_amount AS " + moneyType + ") AS collapsed_amount) FROM facts");
            }
            Map<String, String> hashes = new LinkedHashMap<>();
            long rows = DuckDbReports.writeRows(connection, stage, NAME, hashes, DuplicateSensitivityAnalytics::encode);
            connection.commit();
            return new Result(engine, Map.copyOf(hashes), rows, counts.orders(), counts.retained(), counts.collapsed());
        } finally {
            AnalyzeCommand.removeTree(spill);
        }
    }

    private static Counts loadFacts(Connection connection, Path facts) throws IOException, SQLException {
        // Amounts stay textual until all orders in both scenarios have determined the exact shared scale.
        try (var statement = connection.createStatement()) {
            statement.execute("CREATE TEMP TABLE facts (month VARCHAR, kind VARCHAR, customer_known BOOLEAN, "
                    + "retained_lines BIGINT, retained_units BIGNUM, retained_amount VARCHAR, "
                    + "collapsed_lines BIGINT, collapsed_units BIGNUM, collapsed_amount VARCHAR)");
        }
        int scale = 0, integerDigits = 0;
        long orders = 0, retainedLines = 0, collapsedLines = 0;
        String previous = null;
        try (var reader = Files.newBufferedReader(facts);
             var insert = connection.prepareStatement("INSERT INTO facts VALUES (?, ?, ?, ?, CAST(? AS BIGNUM), ?, "
                     + "?, CAST(? AS BIGNUM), ?)")) {
            for (String line; (line = reader.readLine()) != null;) {
                JsonNode row = StrictJson.parse(line);
                StrictJson.keys(row, "order_key", "event_time", "kind", "customer_known", "retained", "collapsed");
                String key = OrdersDataset.identifier(row.get("order_key"));
                StrictJson.require(previous == null || OrdersDataset.CODEPOINT.compare(previous, key) < 0,
                        "Order keys must be unique and sorted");
                previous = key;
                boolean cancellation = key.startsWith("C") || key.startsWith("c");
                String kind = StrictJson.text(row.get("kind"));
                StrictJson.require(kind.equals(cancellation ? "cancellation" : "sale"), "Invalid order kind");
                String timestamp = StrictJson.text(row.get("event_time"));
                try {
                    ObservedOrdersArguments.parseBound(timestamp);
                } catch (DateTimeParseException invalid) {
                    throw new IOException("Invalid order timestamp", invalid);
                }
                StrictJson.require(row.get("customer_known").isBoolean(), "Expected boolean customer_known");
                int sign = cancellation ? -1 : 1;
                Values retained = Values.read(row.get("retained"), sign);
                Values collapsed = Values.read(row.get("collapsed"), sign);
                checkCollapse(retained, collapsed);
                scale = Math.max(scale, Math.max(retained.amount().scale(), collapsed.amount().scale()));
                integerDigits = Math.max(integerDigits, Math.max(
                        retained.amount().precision() - retained.amount().scale(),
                        collapsed.amount().precision() - collapsed.amount().scale()));
                StrictJson.require(integerDigits + scale <= 38, "No common exact SQL DECIMAL(38) scale");
                orders = Math.incrementExact(orders);
                retainedLines = Math.addExact(retainedLines, retained.lines());
                collapsedLines = Math.addExact(collapsedLines, collapsed.lines());

                insert.setString(1, timestamp.substring(0, 7));
                insert.setString(2, kind);
                insert.setBoolean(3, row.get("customer_known").booleanValue());
                retained.bind(insert, 4);
                collapsed.bind(insert, 7);
                insert.addBatch();
                if (orders % BATCH_SIZE == 0) insert.executeBatch();
            }
            if (orders % BATCH_SIZE != 0) insert.executeBatch();
        }
        return new Counts(orders, retainedLines, collapsedLines, scale);
    }

    private static void checkCollapse(Values retained, Values collapsed) throws IOException {
        long removed = retained.lines() - collapsed.lines();
        StrictJson.require(removed >= 0, "Collapsed line count exceeds original");
        if (removed == 0) {
            StrictJson.require(retained.units().equals(collapsed.units())
                    && retained.amount().compareTo(collapsed.amount()) == 0, "Unchanged lines have different totals");
        } else {
            StrictJson.require(retained.units().abs().subtract(collapsed.units().abs()).compareTo(BigInteger.valueOf(removed)) >= 0
                    && retained.amount().abs().compareTo(collapsed.amount().abs()) > 0,
                    "Collapsed totals contradict removed product lines");
        }
    }

    private static Map<String, Object> encode(ResultSet row) throws SQLException {
        long orders = row.getLong("order_count");
        return Json.object("schema", "repeatlab-duplicate-sensitivity", "schema_version", 1,
                "month", row.getString("month"), "kind", row.getString("kind"), "customer_known", row.getBoolean("customer_known"),
                "retained", group(row, "retained", orders), "collapsed", group(row, "collapsed", orders));
    }

    private static Map<String, Object> group(ResultSet row, String scenario, long orders) throws SQLException {
        BigDecimal amount = row.getBigDecimal(scenario + "_amount");
        if (amount.precision() > 38) throw new SQLException("Segment total exceeds SQL DECIMAL(38) range");
        return Json.object("order_count", orders, "product_line_count", row.getBigDecimal(scenario + "_lines").longValueExact(),
                "units", row.getString(scenario + "_units"), "amount", amount.toPlainString());
    }
}
