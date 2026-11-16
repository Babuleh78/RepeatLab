package io.repeatlab.ingestion;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

final class CleanWriter {
    static final List<String> ARTIFACTS = List.of("clean_events.jsonl", "dispositions.jsonl", "audit.json");
    private final Map<String, Invoice> invoices = new HashMap<>();
    private final Map<String, Long> counts = new TreeMap<>();
    private final Map<String, Long> reasonCounts = new TreeMap<>();

    private static final class Invoice {
        boolean invalid, customerSeen, kindSeen, timeSeen;
        String customer, kind;
        ExactEventTime time;
        final Set<String> conflicts = new TreeSet<>();

        void add(EventNormalizer.Event event) {
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
                if (timeSeen && time.seconds().compareTo(event.time().seconds()) != 0)
                    conflicts.add("INVOICE_CONFLICT_EVENT_TIME");
                time = event.time();
                timeSeen = true;
            }
        }
    }

    CleanWriter(RawDataset raw, CleaningPolicy policy, Path stage) throws IOException {
        for (String name : List.of("data_rows", "accepted_rows", "accepted_product_rows", "accepted_service_rows",
                "quarantined_rows", "missing_customer_rows", "header_rows", "empty_rows", "warning_rows")) counts.put(name, 0L);
        counts.put("header_rows", raw.headerRows);
        counts.put("empty_rows", raw.emptyRows);
        raw.rows(row -> {
            var event = EventNormalizer.normalize(row, raw.dateSystem);
            if (event.invoice() != null) invoices.computeIfAbsent(event.invoice(), ignored -> new Invoice()).add(event);
        });
        try (var events = Files.newBufferedWriter(stage.resolve("clean_events.jsonl"));
             var dispositions = Files.newBufferedWriter(stage.resolve("dispositions.jsonl"))) {
            raw.rows(row -> {
                var event = EventNormalizer.normalize(row, raw.dateSystem);
                Set<String> reasons = new TreeSet<>(event.reasons());
                Invoice invoice = event.invoice() == null ? null : invoices.get(event.invoice());
                if (invoice != null) {
                    if (invoice.invalid) reasons.add("INVOICE_INVALID");
                    reasons.addAll(invoice.conflicts);
                }
                String status;
                increment("data_rows");
                if (event.flags().contains("MISSING_CUSTOMER_ID")) increment("missing_customer_rows");
                if (!event.flags().isEmpty()) increment("warning_rows");
                if (!reasons.isEmpty()) {
                    status = "quarantined";
                    increment("quarantined_rows");
                    for (String reason : reasons) reasonCounts.merge(reason, 1L, Long::sum);
                } else {
                    status = policy.service(event.stock()) ? "accepted_service" : "accepted_product";
                    increment("accepted_rows");
                    increment(status + "_rows");
                    Json.line(events, event.encoded(row, raw, policy));
                }
                Json.line(dispositions, Json.object("schema", CleaningPolicy.SCHEMA, "schema_version", 1,
                        "event_id", row.eventId(raw.sourceHash), "invoice_id", event.invoice(),
                        "provenance", row.provenance(raw.sourceHash), "status", status,
                        "reasons", reasons, "quality_flags", event.flags()));
            });
        }
        StrictJson.require(counts.get("data_rows") == raw.dataRows, "Incomplete row coverage");
        Json.write(stage.resolve("audit.json"), Json.object("schema", CleaningPolicy.SCHEMA, "schema_version", 1,
                "counts", counts, "reason_counts", reasonCounts, "coverage_complete", true));
    }

    private void increment(String name) { counts.merge(name, 1L, Long::sum); }
    Map<String, Long> counts() { return counts; }
    boolean quarantined() { return counts.get("quarantined_rows") > 0; }
    String dataStatus() { return quarantined() ? "quarantined" : counts.get("warning_rows") > 0 ? "warnings" : "ok"; }
}
