package io.repeatlab.ingestion;

import java.util.Map;

record Cell(String column, String reference, String source_type, String state, String value,
            String raw_value, String style, Map<String, String> formula, String cached_value) {
    static String columnName(int index) {
        StringBuilder name = new StringBuilder();
        for (int n = index + 1; n > 0; n = (n - 1) / 26) name.append((char) ('A' + (n - 1) % 26));
        return name.reverse().toString();
    }

    static Cell missing(int column, int row) {
        String name = columnName(column);
        return new Cell(name, name + row, null, "missing", null, null, null, null, null);
    }

    boolean isFormula() { return formula != null; }
    boolean empty() { return !isFormula() && (value == null || value.isEmpty()); }
    boolean scalar() { return source_type != null && !source_type.equals("b") && !source_type.equals("e"); }

    Object content() {
        return Json.object("source_type", source_type, "state", state, "value", value,
                "formula", formula, "cached_value", cached_value);
    }
}
