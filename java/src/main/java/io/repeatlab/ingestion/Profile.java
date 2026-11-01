package io.repeatlab.ingestion;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class Profile {
    static final String VERSION = "online-retail-v1";
    static final List<String> FIELDS = List.of("invoice", "stock_code", "description", "quantity",
            "invoice_date", "unit_price", "customer_id", "country");
    private static final Map<String, String> ALIASES = Map.ofEntries(
            Map.entry("Invoice", "invoice"), Map.entry("InvoiceNo", "invoice"),
            Map.entry("StockCode", "stock_code"), Map.entry("Description", "description"),
            Map.entry("Quantity", "quantity"), Map.entry("InvoiceDate", "invoice_date"),
            Map.entry("Price", "unit_price"), Map.entry("UnitPrice", "unit_price"),
            Map.entry("Customer ID", "customer_id"), Map.entry("CustomerID", "customer_id"),
            Map.entry("Country", "country"));

    private Profile() {}

    static Map<String, Integer> header(List<Cell> cells) throws IOException {
        Map<String, Integer> mapping = new LinkedHashMap<>();
        for (int column = 0; column < cells.size(); column++) {
            Cell cell = cells.get(column);
            if (cell.isFormula()) throw new IOException("Formula in header at " + cell.reference());
            String name = cell.value() == null ? null : ALIASES.get(cell.value());
            if (name != null && mapping.putIfAbsent(name, column) != null)
                throw new IOException("Ambiguous header for " + name);
        }
        if (!mapping.keySet().containsAll(FIELDS)) {
            List<String> missing = FIELDS.stream().filter(field -> !mapping.containsKey(field)).toList();
            throw new IOException("Missing required headers: " + String.join(", ", missing));
        }
        return mapping;
    }
}
