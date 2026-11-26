package io.repeatlab.ingestion;

import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

final class InvoiceState {
    boolean hasProduct;
    boolean invalid, customerSeen, kindSeen, timeSeen;
    String customer, kind;
    ExactEventTime time;
    final Set<String> conflicts = new TreeSet<>();

    void add(EventNormalizer.Event event, CleaningPolicy policy) {
        if (!hasProduct && event.stock() != null && !policy.service(event.stock())) hasProduct = true;
        if (!event.reasons().isEmpty()) invalid = true;
        if (!event.reasons().contains("INVALID_CUSTOMER_ID")) {
            if (customerSeen && !Objects.equals(customer, event.customer()))
                conflicts.add("INVOICE_CONFLICT_CUSTOMER_ID");
            customer = event.customer();
            customerSeen = true;
        }
        if (event.kind() != null) {
            if (kindSeen && !kind.equals(event.kind())) conflicts.add("INVOICE_CONFLICT_KIND");
            kind = event.kind();
            kindSeen = true;
        }
        if (event.time() != null) {
            if (timeSeen && time.compareTo(event.time()) != 0)
                conflicts.add("INVOICE_CONFLICT_EVENT_TIME");
            time = event.time();
            timeSeen = true;
        }
    }
}
