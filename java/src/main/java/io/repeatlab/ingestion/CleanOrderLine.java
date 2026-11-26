package io.repeatlab.ingestion;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.math.BigInteger;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;

/** Business values used to reconcile an order or duplicate group against accepted source rows. */
record CleanOrderLine(String invoice, String customer, LocalDateTime time, String kind,
                      String stock, BigInteger quantity, ExactDecimal price, boolean service) {
    static CleanOrderLine read(JsonNode node) throws IOException {
        String invoice = OrdersDataset.identifier(node.get("invoice_id"));
        String kind = StrictJson.text(node.get("kind"));
        boolean cancellation = invoice.startsWith("C") || invoice.startsWith("c");
        StrictJson.require(kind.equals(cancellation ? "cancellation" : "sale"), "Invalid clean order kind");
        JsonNode quantity = node.path("quantity");
        StrictJson.require(quantity.isIntegralNumber(), "Invalid clean quantity");
        BigInteger units = quantity.bigIntegerValue();
        StrictJson.require(units.signum() == (cancellation ? -1 : 1), "Invalid clean quantity sign");
        ExactDecimal price = ExactDecimal.parse(StrictJson.text(node.get("unit_price")));
        StrictJson.require(price.coefficient().signum() > 0, "Invalid clean unit price");
        StrictJson.require(node.path("is_service").isBoolean(), "Expected boolean is_service");
        return new CleanOrderLine(invoice, StrictJson.nullableText(node.get("customer_id")),
                timestamp(StrictJson.text(node.get("event_time"))), kind, OrdersDataset.identifier(node.get("stock_code")),
                units, price, node.get("is_service").booleanValue());
    }

    static LocalDateTime timestamp(String value) throws IOException {
        try {
            return ObservedOrdersArguments.parseBound(value);
        } catch (DateTimeParseException invalid) {
            throw new IOException("Invalid order timestamp", invalid);
        }
    }
}
