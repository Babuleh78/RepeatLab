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
import java.util.Map;
import java.util.Properties;

final class DuckDbReports {
    @FunctionalInterface
    interface Row {
        Map<String, Object> encode(ResultSet row) throws SQLException;
    }

    private DuckDbReports() {}

    static Connection connect(Path spill) throws SQLException {
        Properties config = new Properties();
        config.setProperty("memory_limit", "1GB");
        config.setProperty("threads", "1");
        config.setProperty("temp_directory", spill.toAbsolutePath().toString());
        return DriverManager.getConnection("jdbc:duckdb:", config);
    }

    static String version(Connection connection) throws SQLException {
        try (var query = connection.createStatement(); var rows = query.executeQuery("SELECT version()")) {
            if (!rows.next()) throw new SQLException("DuckDB version is unavailable");
            return rows.getString(1);
        }
    }

    static long writeRows(Connection connection, Path stage, String name, Map<String, String> sqlHashes,
                          Row encoder, LocalDateTime... bounds) throws IOException, SQLException {
        String sqlName = name + ".sql";
        String sql;
        try (var input = DuckDbReports.class.getResourceAsStream("/sql/" + sqlName)) {
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
}
