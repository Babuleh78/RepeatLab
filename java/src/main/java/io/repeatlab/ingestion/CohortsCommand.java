package io.repeatlab.ingestion;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

final class CohortsCommand {
    private static final String SCHEMA = "repeatlab-cohorts";
    private static final Set<String> OPTIONS =
            Set.of("--input", "--output", "--observed-from", "--observed-until");
    private static final List<String> ARTIFACTS =
            List.of("repeat_rates.jsonl", "cohort_activity.jsonl", "audit.json");
    private static final Pattern BOUND_PATTERN =
            Pattern.compile("[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\\.[0-9]{1,6})?");
    private static final DateTimeFormatter BOUND_FORMAT = new DateTimeFormatterBuilder()
            .appendPattern("uuuu-MM-dd'T'HH:mm:ss")
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 6, true)
            .toFormatter(Locale.ROOT);

    private record Arguments(Path input, Path output, LocalDateTime from, LocalDateTime until) {}

    private CohortsCommand() {}

    static int run(String[] args, PrintStream out, PrintStream err) {
        Arguments arguments = parse(args);
        if (arguments == null) return 64;

        Instant started = Instant.now();
        String stageName = "output_reservation";
        TransactionalOutput transaction = null;
        try {
            Path input = arguments.input();
            Path output = arguments.output().toAbsolutePath();
            Path outputParent = output.getParent();
            if (outputParent != null) {
                Path realParent = outputParent.toRealPath();
                // Invalid input paths still go through orders validation and its diagnostics.
                if (Files.isDirectory(input)) {
                    input = input.toRealPath();
                    if (realParent.startsWith(input))
                        throw new IOException("Output directory is inside the input orders directory");
                }
                output = realParent.resolve(output.getFileName());
            }
            transaction = new TransactionalOutput(output);
            Path stage = transaction.staging();

            stageName = "orders_validation";
            Path snapshot = stage.resolve("verified-orders");
            OrdersDataset dataset = new OrdersDataset(input, snapshot);
            AnalyzeCommand.removeTree(snapshot);

            stageName = "observation_validation";
            for (OrdersDataset.Order order : dataset.orders) {
                if (order.time().isBefore(arguments.from()) || !order.time().isBefore(arguments.until()))
                    throw new IOException("Order timestamp is outside the declared observation interval");
            }

            stageName = "sql";
            CohortAnalytics.Result sql = CohortAnalytics.run(
                    dataset.orders, arguments.from(), arguments.until(), stage);

            stageName = "publication";
            Map<String, Object> observation = Json.object(
                    "observed_from", BOUND_FORMAT.format(arguments.from()),
                    "observed_until", BOUND_FORMAT.format(arguments.until()));
            Json.write(stage.resolve("audit.json"), Json.object(
                    "schema", SCHEMA, "schema_version", 1, "observation", observation,
                    "orders_counts", dataset.counts, "source_clean_counts", dataset.sourceCounts,
                    "data_status", dataset.dataStatus, "repeat_rates_rows", sql.repeatRows(),
                    "cohort_activity_rows", sql.activityRows()));
            Map<String, String> hashes = new LinkedHashMap<>();
            for (String artifact : ARTIFACTS) hashes.put(artifact, Json.hash(stage.resolve(artifact)));
            Object manifest = Json.object(
                    "schema", SCHEMA, "schema_version", 1, "policy_version", "cohorts-v1",
                    "status", "completed", "data_status", dataset.dataStatus, "observation", observation,
                    "input", Json.object("orders_manifest_sha256", dataset.manifestHash,
                            "clean_manifest_sha256", dataset.cleanHash),
                    "engine", Json.object("name", "DuckDB", "version", sql.engine()),
                    "sql_sha256", sql.sqlHashes(), "artifacts", hashes);
            Object run = Json.object("schema_version", 1, "status", "execution_finished",
                    "started_at", started.toString(), "finished_at", Instant.now().toString(),
                    "java_version", System.getProperty("java.version"));
            transaction.publish(ARTIFACTS, hashes, manifest, run);
            out.println("Completed: " + dataset.dataStatus);
            return dataset.dataStatus.equals("quarantined") ? 2 : 0;
        } catch (Exception | LinkageError failure) {
            Object fatal = Json.object("schema_version", 1, "status", "failed", "code", "COHORTS_FAILED",
                    "stage", stageName, "error_type", failure.getClass().getSimpleName(),
                    "message", failure.getMessage(), "started_at", started.toString(),
                    "finished_at", Instant.now().toString());
            try {
                err.println(Json.encode(fatal));
                if (transaction != null) transaction.failed(fatal);
            } catch (IOException diagnosticFailure) {
                err.println("COHORTS_FAILED: unable to persist diagnostics");
            }
            return 1;
        }
    }

    private static Arguments parse(String[] args) {
        if (args.length != 9) return null;
        Map<String, String> parameters = new HashMap<>();
        for (int i = 1; i < args.length; i += 2) {
            if (!OPTIONS.contains(args[i]) || args[i + 1].isBlank()
                    || parameters.putIfAbsent(args[i], args[i + 1]) != null) return null;
        }
        try {
            LocalDateTime from = parseBound(parameters.get("--observed-from"));
            LocalDateTime until = parseBound(parameters.get("--observed-until"));
            if (!from.isBefore(until)) return null;
            return new Arguments(Path.of(parameters.get("--input")), Path.of(parameters.get("--output")),
                    from, until);
        } catch (InvalidPathException | DateTimeParseException invalid) {
            return null;
        }
    }

    private static LocalDateTime parseBound(String value) {
        if (!BOUND_PATTERN.matcher(value).matches())
            throw new DateTimeParseException("Expected local timestamp with seconds and up to six fractional digits",
                    value, 0);
        LocalDateTime time = LocalDateTime.parse(value);
        if (time.getYear() < 1) throw new DateTimeParseException("Year must be in 0001–9999", value, 0);
        return time;
    }
}
