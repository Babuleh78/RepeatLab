package io.repeatlab.ingestion;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Compare both worker scenarios with product rows in the private clean snapshot. */
final class SensitivityOrderMembership {
    private final CleanOrderLine first;
    private final Totals retained = new Totals();
    private final Totals collapsed = new Totals();

    private SensitivityOrderMembership(CleanOrderLine first) { this.first = first; }

    static Map<String, SensitivityOrderMembership> read(Path cleanEvents) throws IOException {
        Map<String, SensitivityOrderMembership> invoices = new HashMap<>();
        Set<CleanOrderLine> distinct = new HashSet<>();
        try (var reader = Files.newBufferedReader(cleanEvents)) {
            for (JsonNode node; (node = OrdersDataset.nextRow(reader)) != null;) {
                if (node.path("is_service").isBoolean() && node.get("is_service").booleanValue()) continue;
                CleanOrderLine event = CleanOrderLine.read(node);
                var invoice = invoices.computeIfAbsent(event.invoice(), ignored -> new SensitivityOrderMembership(event));
                StrictJson.require(invoice.first.time().equals(event.time()) && invoice.first.kind().equals(event.kind())
                        && Objects.equals(invoice.first.customer(), event.customer()), "Inconsistent clean product invoice");
                invoice.retained.add(event);
                if (distinct.add(event)) invoice.collapsed.add(event);
            }
        }
        return invoices;
    }

    boolean matches(LocalDateTime time, String kind, boolean known, DuplicateSensitivityAnalytics.Values before,
                    DuplicateSensitivityAnalytics.Values after) {
        return first.time().equals(time) && first.kind().equals(kind) && (first.customer() != null) == known
                && retained.matches(before) && collapsed.matches(after);
    }

    private static final class Totals {
        private long lines;
        private BigInteger units = BigInteger.ZERO;
        private final ExactDecimal.Sum amount = new ExactDecimal.Sum();

        void add(CleanOrderLine event) {
            lines = Math.incrementExact(lines);
            units = units.add(event.quantity());
            amount.add(event.price(), event.quantity());
        }

        boolean matches(DuplicateSensitivityAnalytics.Values fact) {
            BigDecimal decimal = fact.amount();
            return lines == fact.lines() && units.equals(fact.units())
                    && amount.matches(new ExactDecimal(decimal.unscaledValue(), BigInteger.valueOf(-(long) decimal.scale())));
        }
    }
}
