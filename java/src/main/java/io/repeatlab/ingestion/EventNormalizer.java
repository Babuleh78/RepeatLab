package io.repeatlab.ingestion;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

final class EventNormalizer {
    record Event(String invoice, String stock, String customer, BigInteger quantity,
                 BigDecimal price, ExactEventTime time, String kind,
                 Set<String> flags, Set<String> reasons) {
        Map<String, Object> encoded(RawDataset.Row row, RawDataset raw, CleaningPolicy policy) {
            return Json.object("schema", CleaningPolicy.SCHEMA, "schema_version", 1,
                    "event_id", row.eventId(raw.sourceHash), "invoice_id", invoice, "stock_code", stock,
                    "customer_id", customer, "quantity", quantity, "unit_price", price.toString(),
                    "event_time", time.formatted(), "kind", kind, "is_service", policy.service(stock),
                    "provenance", row.provenance(raw.sourceHash), "quality_flags", flags);
        }
    }

    private EventNormalizer() {}

    static Event normalize(RawDataset.Row row, String dateSystem) {
        Set<String> flags = new TreeSet<>(), reasons = new TreeSet<>();
        String invoice = id(row.field("invoice"), "INVOICE_ID", false, flags, reasons);
        String stock = id(row.field("stock_code"), "STOCK_CODE", false, flags, reasons);
        String customer = id(row.field("customer_id"), "CUSTOMER_ID", true, flags, reasons);
        BigInteger quantity = null;
        BigDecimal price = null;
        ExactEventTime time = null;
        String kind = null;
        try { quantity = decimal(row.field("quantity")).toBigIntegerExact(); }
        catch (IllegalArgumentException | ArithmeticException invalid) { reasons.add("INVALID_QUANTITY"); }
        try { price = decimal(row.field("unit_price")); }
        catch (IllegalArgumentException | ArithmeticException invalid) { reasons.add("INVALID_UNIT_PRICE"); }
        try { time = ExactEventTime.parse(row.field("invoice_date"), dateSystem); }
        catch (IllegalArgumentException | ArithmeticException | java.time.DateTimeException invalid) { reasons.add("INVALID_EVENT_TIME"); }
        if (quantity != null) {
            if (quantity.signum() == 0) reasons.add("ZERO_QUANTITY");
            else if (invoice != null) {
                boolean cancellation = invoice.startsWith("c") || invoice.startsWith("C");
                if (cancellation != (quantity.signum() < 0)) reasons.add("CANCELLATION_MISMATCH");
                else kind = cancellation ? "cancellation" : "sale";
            }
        }
        if (price != null && price.signum() <= 0) reasons.add("NONPOSITIVE_UNIT_PRICE");
        if (time != null && time.losesPrecision()) flags.add("TIME_PRECISION_FLOORED");
        for (String field : List.of("description", "country"))
            if (row.field(field).isFormula()) flags.add("NONCRITICAL_FORMULA");
        return new Event(invoice, stock, customer, quantity, price, time, kind, flags, reasons);
    }

    private static String id(Cell cell, String name, boolean optional, Set<String> flags, Set<String> reasons) {
        try {
            if (cell.isFormula()) throw new IllegalArgumentException("Untrusted formula");
            if (CleaningPolicy.missing(cell.value())) {
                if (optional) { flags.add("MISSING_CUSTOMER_ID"); return null; }
                throw new IllegalArgumentException("Missing identifier");
            }
            if (cell.source_type().equals("n")) {
                String result = new BigDecimal(cell.value()).toBigIntegerExact().toString();
                flags.add("NORMALIZED_" + name);
                return result;
            }
            if (!Set.of("s", "inlineStr", "str").contains(cell.source_type()))
                throw new IllegalArgumentException("Invalid identifier type");
            return cell.value();
        } catch (IllegalArgumentException | ArithmeticException invalid) {
            reasons.add("INVALID_" + name);
            return null;
        }
    }

    private static BigDecimal decimal(Cell cell) {
        if (cell.isFormula() || cell.empty() || !cell.scalar() || cell.source_type().equals("d"))
            throw new IllegalArgumentException("Invalid decimal cell");
        return new BigDecimal(cell.value());
    }
}
