package io.repeatlab.ingestion;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

final class ProductRepeatsAnalytics {
    record Result(String engine, Map<String, String> sqlHashes, long rows) {}

    private static final int INSERT_BATCH_SIZE = 1024;

    private ProductRepeatsAnalytics() {}

    static Result run(List<OrdersDataset.Order> orders, Map<String, Set<String>> products, Path stage)
            throws IOException, SQLException {
        Path spill = Files.createDirectory(stage.resolve("duckdb-temp"));
        Result result;
        try (Connection connection = DuckDbReports.connect(spill)) {
            String version = DuckDbReports.version(connection);
            loadProducts(connection, orders, products);
            Map<String, String> sqlHashes = new LinkedHashMap<>();
            long rows = DuckDbReports.writeRows(connection, stage, "product_repeats", sqlHashes,
                    ProductRepeatsAnalytics::encode);
            result = new Result(version, Map.copyOf(sqlHashes), rows);
        }
        AnalyzeCommand.removeTree(spill);
        return result;
    }

    private static void loadProducts(Connection connection, List<OrdersDataset.Order> orders,
                                     Map<String, Set<String>> products) throws SQLException {
        try (var query = connection.createStatement()) {
            query.execute("CREATE TABLE order_products (order_key VARCHAR NOT NULL, customer_id VARCHAR NOT NULL, "
                    + "event_time TIMESTAMP NOT NULL, stock_code VARCHAR NOT NULL)");
        }
        connection.setAutoCommit(false);
        try (var insert = connection.prepareStatement("INSERT INTO order_products VALUES (?, ?, CAST(? AS TIMESTAMP), ?)")) {
            int pending = 0;
            for (var order : orders) {
                if (order.customer() == null || !order.kind().equals("sale")) continue;
                String time = order.time().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
                for (String product : products.get(order.invoice())) {
                    insert.setString(1, order.invoice());
                    insert.setString(2, order.customer());
                    insert.setString(3, time);
                    insert.setString(4, product);
                    insert.addBatch();
                    if (++pending == INSERT_BATCH_SIZE) {
                        insert.executeBatch();
                        pending = 0;
                    }
                }
            }
            if (pending > 0) insert.executeBatch();
        }
        connection.commit();
        connection.setAutoCommit(true);
    }

    private static Map<String, Object> encode(ResultSet row) throws SQLException {
        boolean prior = row.getBoolean("has_prior_purchase");
        long distinct = row.getLong("distinct_products");
        long repeated = row.getLong("repeated_products");
        return Json.object("schema", "repeatlab-product-repeats", "schema_version", 1,
                "order_key", row.getString("order_key"), "customer_id", row.getString("customer_id"),
                "event_time", row.getString("event_time"), "has_prior_purchase", prior, "distinct_products", distinct,
                "repeated_products", repeated, "repeat_share",
                prior ? Json.object("numerator", repeated, "denominator", distinct) : null);
    }
}
