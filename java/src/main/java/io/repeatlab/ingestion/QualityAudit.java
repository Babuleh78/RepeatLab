package io.repeatlab.ingestion;

import java.io.IOException;
import java.io.Writer;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

final class QualityAudit {
    private final Writer issues;
    private final String sourceHash;
    private final Map<String, Long> counts = new TreeMap<>();
    private final Map<String, Long> missing = new TreeMap<>();
    private final Map<Integer, Map<String, Long>> bySheet = new TreeMap<>();
    private final Map<String, Object> duplicates = new HashMap<>();
    private final Map<String, Set<String>> keys = new LinkedHashMap<>();
    private long errors, warnings, rows;
    private LocalDateTime min, max;

    QualityAudit(Writer issues, String sourceHash) {
        this.issues = issues;
        this.sourceHash = sourceHash;
        for (String key : List.of("invoice", "customer_id", "stock_code")) keys.put(key, new HashSet<>());
        Profile.FIELDS.forEach(field -> missing.put(field, 0L));
    }

    boolean hasErrors() { return errors != 0; }
    String dataStatus() { return hasErrors() ? "errors" : warnings != 0 ? "warnings" : "ok"; }

    void row(int sheet, String name, int row, List<Cell> cells, Map<String, Integer> schema,
             boolean date1904) throws IOException {
        rows++;
        for (Cell cell : cells) {
            if (cell.isFormula()) issue("UNVERIFIED_FORMULA", "warning", sheet, name, row, cell,
                    "Formula retained without evaluation; cached value is unverified", null);
        }
        for (String field : Profile.FIELDS) {
            Cell cell = cells.get(schema.get(field));
            if (cell.empty()) {
                missing.merge(field, 1L, Long::sum);
                if (field.equals("customer_id")) issue("MISSING_CUSTOMER_ID", "warning", sheet, name, row,
                        cell, "No customer identifier; no substitute identifier assigned", null);
                else if (field.equals("invoice") || field.equals("stock_code"))
                    issue("MISSING_KEY_FIELD", "warning", sheet, name, row, cell, "Missing " + field, null);
            }
            if (keys.containsKey(field) && !cell.empty() && !cell.isFormula())
                keys.get(field).add(Json.hashText(Json.encode(cell.content())));
        }
        Cell quantityCell = cells.get(schema.get("quantity"));
        BigInteger quantity = null;
        if (!quantityCell.isFormula()) {
            try { quantity = decimal(quantityCell).toBigIntegerExact(); }
            catch (IllegalArgumentException | ArithmeticException e) {
                issue("INVALID_QUANTITY", "error", sheet, name, row, quantityCell,
                        "Expected a finite integral quantity", null);
            }
            if (quantity != null && quantity.signum() == 0)
                issue("ZERO_QUANTITY", "warning", sheet, name, row, quantityCell, "Quantity is zero", null);
        }
        Cell priceCell = cells.get(schema.get("unit_price"));
        if (!priceCell.isFormula()) {
            try {
                if (decimal(priceCell).signum() <= 0)
                    issue("NONPOSITIVE_PRICE", "warning", sheet, name, row, priceCell, "Price is not positive", null);
            } catch (IllegalArgumentException e) {
                issue("INVALID_PRICE", "error", sheet, name, row, priceCell, "Expected a finite decimal price", null);
            }
        }
        Cell invoice = cells.get(schema.get("invoice"));
        if (quantity != null && !invoice.empty() && !invoice.isFormula() && invoice.scalar()) {
            boolean prefix = invoice.value().toLowerCase(Locale.ROOT).startsWith("c");
            if (prefix != (quantity.signum() < 0))
                issue("CANCELLATION_MISMATCH", "warning", sheet, name, row, quantityCell,
                        "Cancellation prefix and quantity sign disagree", null);
        }
        Cell dateCell = cells.get(schema.get("invoice_date"));
        if (!dateCell.isFormula()) {
            try {
                LocalDateTime date = date(dateCell, date1904);
                if (min == null || date.isBefore(min)) min = date;
                if (max == null || date.isAfter(max)) max = date;
            } catch (IllegalArgumentException | java.time.DateTimeException | ArithmeticException e) {
                issue("INVALID_DATE", "error", sheet, name, row, dateCell,
                        "Expected an Excel serial or ISO local date/time without offset", null);
            }
        }
        // The canonical fields permit matching aliases and reordered headers across sheets.
        Map<String, Object> content = new TreeMap<>();
        for (String field : Profile.FIELDS) content.put(field, cells.get(schema.get(field)).content());
        Set<Integer> mapped = new HashSet<>(schema.values());
        List<Object> extra = new ArrayList<>();
        for (int i = 0; i < cells.size(); i++) if (!mapped.contains(i))
            extra.add(Json.object("column", cells.get(i).column(), "content", cells.get(i).content()));
        content.put("extra_columns", extra);
        String fingerprint = Json.hashText(Json.encode(content));
        Object first = duplicates.putIfAbsent(fingerprint,
                Json.object("sheet_index", sheet, "sheet_name", name, "row_number", row));
        if (first != null) issue("POSSIBLE_DUPLICATE", "warning", sheet, name, row, null,
                "Same cell types, states and contents as an earlier data row; both retained", first);
    }

    private static BigDecimal decimal(Cell cell) {
        if (cell.empty() || !cell.scalar() || cell.source_type().equals("d"))
            throw new IllegalArgumentException("Not a decimal cell");
        return new BigDecimal(cell.value());
    }

    private static LocalDateTime date(Cell cell, boolean date1904) {
        if (cell.empty() || !cell.scalar()) throw new IllegalArgumentException("Not a date cell");
        if (cell.source_type().equals("n")) {
            BigDecimal serial = new BigDecimal(cell.value());
            if (serial.signum() < 0 || serial.compareTo(new BigDecimal("2958466")) >= 0)
                throw new IllegalArgumentException("Serial outside supported calendar");
            long days = serial.setScale(0, RoundingMode.DOWN).longValueExact();
            return excelDate(serial, days, date1904);
        }
        String value = cell.value();
        if (value.length() == 10) return LocalDate.parse(value, DateTimeFormatter.ISO_LOCAL_DATE).atStartOfDay();
        return LocalDateTime.parse(value, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
    }

    private static LocalDateTime excelDate(BigDecimal serial, long days, boolean date1904) {
        if (!date1904 && days == 60) throw new IllegalArgumentException("Excel fictitious leap day");
        LocalDate epoch = date1904 ? LocalDate.of(1904, 1, 1) : LocalDate.of(1899, 12, 31);
        long adjusted = !date1904 && days > 60 ? days - 1 : days;
        long nanos = serial.subtract(BigDecimal.valueOf(days)).multiply(new BigDecimal("86400000000000"))
                .setScale(0, RoundingMode.HALF_UP).longValueExact();
        LocalDateTime result = epoch.plusDays(adjusted).atStartOfDay().plusNanos(nanos);
        if (result.getYear() > 9999) throw new IllegalArgumentException("Date after year 9999");
        return result;
    }

    private void issue(String code, String severity, int sheet, String name, int row, Cell cell,
                       String message, Object related) throws IOException {
        if (severity.equals("error")) errors++; else warnings++;
        counts.merge(code, 1L, Long::sum);
        bySheet.computeIfAbsent(sheet, ignored -> new TreeMap<>()).merge(code, 1L, Long::sum);
        Json.line(issues, Json.object("schema_version", 1, "source_sha256", sourceHash, "code", code,
                "severity", severity, "sheet_index", sheet, "sheet_name", name, "row_number", row,
                "column", cell == null ? null : cell.column(), "cell", cell, "message", message,
                "related_row", related));
    }

    Object summary(List<Map<String, Object>> sheets) {
        Map<String, Integer> cardinalities = new TreeMap<>();
        keys.forEach((field, values) -> cardinalities.put(field, values.size()));
        return Json.object("schema_version", 1, "profile", Profile.VERSION, "source_sha256", sourceHash,
                "data_rows", rows, "errors", errors, "warnings", warnings, "data_status", dataStatus(),
                "issue_counts", counts, "sheet_issue_counts", bySheet, "missing_fields", missing,
                "distinct_keys", cardinalities, "sheets", sheets,
                "date_range", Json.object("min", format(min), "max", format(max)));
    }

    private static String format(LocalDateTime time) {
        return time == null ? null : DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(time);
    }
}
