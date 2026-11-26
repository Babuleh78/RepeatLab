package io.repeatlab.ingestion;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

final class SnapshotCommand {
    private static final Set<String> REQUIRED =
            Set.of("--input", "--config", "--output", "--snapshot-time", "--observed-until");
    private static final String ROWS = "snapshots.jsonl";
    private SnapshotCommand() {}

    private record Window(LocalDateTime time, LocalDateTime until, LocalDateTime end,
                          int lookbackDays, int horizonDays) {
        boolean complete() { return !until.isBefore(end); }
        LocalDateTime availableEnd() { return complete() ? end : until; }
    }

    static int run(String[] args, PrintStream out, PrintStream err) {
        Map<String, String> options = new HashMap<>();
        for (int i = 1; i < args.length; i += 2) {
            String key = args[i];
            if (i + 1 == args.length || args[i + 1].isBlank()
                    || !(REQUIRED.contains(key) || key.equals("--lookback-days") || key.equals("--horizon-days"))
                    || options.putIfAbsent(key, args[i + 1]) != null) return 64;
        }
        if (!options.keySet().containsAll(REQUIRED)) return 64;
        Path input, config, output;
        Window window;
        try {
            input = Path.of(options.get("--input"));
            config = Path.of(options.get("--config"));
            output = Path.of(options.get("--output"));
            var time = ObservedOrdersArguments.parseBound(options.get("--snapshot-time"));
            var until = ObservedOrdersArguments.parseBound(options.get("--observed-until"));
            int lookback = Integer.parseInt(options.getOrDefault("--lookback-days", "180"));
            int horizon = Integer.parseInt(options.getOrDefault("--horizon-days", "30"));
            if (lookback <= 0 || horizon <= 0 || until.isBefore(time)) return 64;
            var end = time.plusDays(horizon);
            if (end.getYear() > 9999) return 64;
            window = new Window(time, until, end, lookback, horizon);
        } catch (IllegalArgumentException | DateTimeException invalid) { return 64; }

        TransactionalOutput transaction = null;
        Instant started = Instant.now();
        try {
            output = ObservedOrdersArguments.resolveOutput(output);
            input = ObservedOrdersArguments.resolveSource(input, output, "raw");
            config = ObservedOrdersArguments.resolveSource(config, output, "config");
            transaction = new TransactionalOutput(output);
            Path stage = transaction.staging();
            CleaningPolicy policy = CleaningPolicy.read(config);
            RawDataset raw = new RawDataset(input, stage);
            var ledger = new Ledger(window, policy);
            try {
                raw.rows(row -> ledger.add(row, raw.dateSystem));
            } finally { raw.releaseSnapshot(); }
            long customers = ledger.write(stage.resolve(ROWS));
            var counts = Json.object("customers", customers, "undated_rows", ledger.undated,
                    "dated_rows_without_invoice", ledger.noInvoice,
                    "quarantined_history_invoices", ledger.invalidHistory,
                    "quarantined_target_invoices", ledger.invalidTarget);
            boolean withErrors = ledger.undated > 0 || ledger.noInvoice > 0
                    || ledger.invalidHistory > 0 || ledger.invalidTarget > 0;
            Map<String, String> hashes = Map.of(ROWS, Json.hash(stage.resolve(ROWS)));
            Object manifest = Json.object("schema", "repeatlab-snapshot", "schema_version", 1,
                    "policy_version", "event-time-v1", "status", "completed", "input", raw.provenance(),
                    "config_sha256", policy.hash(), "snapshot_time", format(window.time),
                    "observed_until", format(window.until), "label_end", format(window.end),
                    "lookback_days", window.lookbackDays, "horizon_days", window.horizonDays,
                    "counts", counts, "data_status", withErrors ? "quarantined" : "ok", "artifacts", hashes);
            transaction.publish(List.of(ROWS), hashes, manifest, Json.object("schema_version", 1,
                    "status", "execution_finished", "started_at", started.toString(), "finished_at", Instant.now().toString()));
            out.println("Completed: " + customers + " customers");
            return withErrors ? 2 : 0;
        } catch (Exception failure) {
            Object fatal = Json.object("schema_version", 1, "status", "failed", "code", "SNAPSHOT_FAILED",
                    "error_type", failure.getClass().getSimpleName(), "message", failure.getMessage());
            try {
                err.println(Json.encode(fatal));
                if (transaction != null) transaction.failed(fatal);
            } catch (IOException diagnostic) { err.println("SNAPSHOT_FAILED: unable to persist diagnostics"); }
            return 1;
        }
    }

    private static String format(LocalDateTime time) {
        return DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(time);
    }

    private static final class Ledger {
        final Window window;
        final EventNormalizer normalizer = new EventNormalizer();
        final CleaningPolicy policy;
        final ExactEventTime start, cutoff, until;
        final Map<String, InvoiceState> history = new HashMap<>(), target = new HashMap<>();
        long undated, noInvoice, invalidHistory, invalidTarget;

        Ledger(Window window, CleaningPolicy policy) {
            this.window = window;
            this.policy = policy;
            start = ExactEventTime.of(window.time.minusDays(window.lookbackDays));
            cutoff = ExactEventTime.of(window.time);
            until = ExactEventTime.of(window.availableEnd());
        }

        void add(RawDataset.Row row, String dateSystem) throws IOException {
            ExactEventTime time = EventNormalizer.readTime(row, dateSystem);
            if (time == null) { undated++; return; }
            if (time.compareTo(until) >= 0) return;
            var event = normalizer.normalize(row, time);
            if (event.invoice() == null) { noInvoice++; return; }
            var ledger = time.compareTo(cutoff) < 0 ? history : target;
            ledger.computeIfAbsent(event.invoice(), ignored -> new InvoiceState()).add(event, policy);
        }

        long write(Path destination) throws IOException {
            var buyers = new TreeMap<String, Long>(OrdersDataset.CODEPOINT);
            for (InvoiceState invoice : history.values()) {
                if (invoice.invalid || !invoice.conflicts.isEmpty()) { invalidHistory++; continue; }
                if (purchase(invoice) && invoice.time.compareTo(start) >= 0)
                    buyers.merge(invoice.customer, 1L, Long::sum);
            }
            var targetBuyers = new HashSet<String>();
            for (var entry : target.entrySet()) {
                InvoiceState invoice = entry.getValue();
                // A matching invoice on opposite sides of t necessarily has two distinct event times.
                if (history.containsKey(entry.getKey()) || invoice.invalid || !invoice.conflicts.isEmpty()) {
                    invalidTarget++;
                } else if (purchase(invoice)) {
                    targetBuyers.add(invoice.customer);
                }
            }
            try (var writer = Files.newBufferedWriter(destination)) {
                for (var buyer : buyers.entrySet()) {
                    Integer label = window.complete() ? Integer.valueOf(targetBuyers.contains(buyer.getKey()) ? 1 : 0) : null;
                    Json.line(writer, Json.object("customer_id", buyer.getKey(), "snapshot_time", format(window.time),
                            "label_end", format(window.end), "history_order_count", buyer.getValue(),
                            "label", label, "status", window.complete() ? "complete" : "censored"));
                }
            }
            return buyers.size();
        }

        private static boolean purchase(InvoiceState invoice) {
            return invoice.hasProduct && "sale".equals(invoice.kind) && invoice.customer != null;
        }
    }
}
