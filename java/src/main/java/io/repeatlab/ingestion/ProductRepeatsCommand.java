package io.repeatlab.ingestion;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

final class ProductRepeatsCommand {
    private static final String SCHEMA = "repeatlab-product-repeats";
    private static final List<String> ARTIFACTS = List.of("product_repeats.jsonl", "audit.json");

    private ProductRepeatsCommand() {}

    static int run(String[] args, PrintStream out, PrintStream err) {
        ObservedOrdersArguments arguments = ObservedOrdersArguments.parseWithClean(args);
        if (arguments == null) return 64;

        Instant started = Instant.now();
        String stageName = "output_reservation";
        TransactionalOutput transaction = null;
        try {
            arguments = arguments.resolvePaths();
            transaction = new TransactionalOutput(arguments.output());
            Path stage = transaction.staging();

            stageName = "orders_validation";
            Map<String, Set<String>> products = new HashMap<>();
            OrdersDataset dataset = readInputs(arguments, stage, products);

            stageName = "observation_validation";
            arguments.requireCoverage(dataset.orders);

            stageName = "sql";
            ProductRepeatsAnalytics.Result sql = ProductRepeatsAnalytics.run(dataset.orders, products, stage);

            stageName = "publication";
            Map<String, Object> observation = arguments.observation();
            Json.write(stage.resolve("audit.json"), Json.object(
                    "schema", SCHEMA, "schema_version", 1, "observation", observation,
                    "orders_counts", dataset.counts, "source_clean_counts", dataset.sourceCounts,
                    "data_status", dataset.dataStatus, "product_repeats_rows", sql.rows()));
            Map<String, String> hashes = new LinkedHashMap<>();
            for (String artifact : ARTIFACTS) hashes.put(artifact, Json.hash(stage.resolve(artifact)));
            Object manifest = Json.object(
                    "schema", SCHEMA, "schema_version", 1, "policy_version", "product-repeats-v1",
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
            Object fatal = Json.object("schema_version", 1, "status", "failed", "code", "PRODUCT_REPEATS_FAILED",
                    "stage", stageName, "error_type", failure.getClass().getSimpleName(),
                    "message", failure.getMessage(), "started_at", started.toString(),
                    "finished_at", Instant.now().toString());
            try {
                err.println(Json.encode(fatal));
                if (transaction != null) transaction.failed(fatal);
            } catch (IOException diagnosticFailure) {
                err.println("PRODUCT_REPEATS_FAILED: unable to persist diagnostics");
            }
            return 1;
        }
    }

    private static OrdersDataset readInputs(ObservedOrdersArguments arguments, Path stage,
                                            Map<String, Set<String>> products) throws IOException {
        Path snapshots = Files.createDirectory(stage.resolve("inputs"));
        try {
            Path clean = Files.createDirectory(snapshots.resolve("clean"));
            for (String name : OrdersDataset.CLEAN_FILES)
                Files.copy(arguments.clean().resolve(name), clean.resolve(name));
            return new OrdersDataset(arguments.input(), clean, snapshots.resolve("orders"), products);
        } finally {
            AnalyzeCommand.removeTree(snapshots);
        }
    }
}
