package io.repeatlab.ingestion;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

final class DuplicateSensitivityCommand {
    private static final String SCHEMA = "repeatlab-duplicate-sensitivity";
    private static final List<String> ARTIFACTS = List.of("duplicate_sensitivity.jsonl", "audit.json");
    private static final Set<String> OPTIONS = Set.of("--input", "--output", "--python");

    private DuplicateSensitivityCommand() {}

    static int run(String[] args, PrintStream out, PrintStream err) {
        if (args.length != 7) return 64;
        Map<String, String> options = new HashMap<>();
        for (int i = 1; i < args.length; i += 2) {
            if (!OPTIONS.contains(args[i]) || args[i + 1].isBlank()
                    || options.putIfAbsent(args[i], args[i + 1]) != null) return 64;
        }
        Path input, output;
        try {
            input = Path.of(options.get("--input"));
            output = Path.of(options.get("--output"));
            Path.of(options.get("--python"));
        } catch (InvalidPathException invalid) { return 64; }

        Instant started = Instant.now();
        String stageName = "output_reservation";
        TransactionalOutput transaction = null;
        try {
            output = ObservedOrdersArguments.resolveOutput(output);
            input = ObservedOrdersArguments.resolveSource(input, output, "clean");
            transaction = new TransactionalOutput(output);
            Path stage = transaction.staging();
            stageName = "clean_validation";
            Path work = Files.createDirectory(stage.resolve("work"));
            CleanSnapshot snapshot;
            DuplicateSensitivityAnalytics.Result sql;
            int code;
            try {
                Path clean = Files.createDirectory(work.resolve("clean"));
                snapshot = CleanSnapshot.copy(input, clean);
                Path facts = work.resolve("facts.jsonl");

                stageName = "worker";
                code = AnalyzeCommand.runProcess(List.of(options.get("--python"), "-I", "-m",
                        "repeatlab.duplicate_sensitivity", "--input", clean.toString(),
                        "--output", facts.toString()), output);
                StrictJson.require(code == (snapshot.dataStatus().equals("quarantined") ? 2 : 0),
                        "Sensitivity worker exit/quality mismatch: " + code);
                snapshot.verifyHashes(clean);
                StrictJson.require(Files.isRegularFile(facts, LinkOption.NOFOLLOW_LINKS),
                        "Worker did not produce an order facts file");
                verifyWorkerReceipt(output.resolve("worker.stdout.log"), snapshot.manifestHash(), facts);

                stageName = "sql";
                sql = DuplicateSensitivityAnalytics.run(facts, stage, clean.resolve("clean_events.jsonl"));
                StrictJson.require(sql.productLines() == OrdersDataset.count(snapshot.counts(), "accepted_product_rows"),
                        "Retained product rows differ from the clean audit");
            } finally {
                AnalyzeCommand.removeTree(work);
            }

            stageName = "publication";
            Json.write(stage.resolve("audit.json"), Json.object(
                    "schema", SCHEMA, "schema_version", 1, "data_status", snapshot.dataStatus(),
                    "source_clean_counts", snapshot.counts(), "duplicate_sensitivity_rows", sql.rows(),
                    "orders", sql.orders(), "retained_product_rows", sql.productLines(),
                    "collapsed_product_rows", sql.collapsedLines()));
            Map<String, String> hashes = new LinkedHashMap<>();
            for (String name : ARTIFACTS) hashes.put(name, Json.hash(stage.resolve(name)));
            Object manifest = Json.object("schema", SCHEMA, "schema_version", 1,
                    "policy_version", "duplicate-sensitivity-v1", "status", "completed",
                    "data_status", snapshot.dataStatus(), "input", Json.object(
                            "clean_manifest_sha256", snapshot.manifestHash(), "config_sha256", snapshot.configHash(),
                            "clean_artifacts", snapshot.artifacts()),
                    "engine", Json.object("name", "DuckDB", "version", sql.engine()),
                    "sql_sha256", sql.sqlHashes(), "artifacts", hashes);
            Object run = Json.object("schema_version", 1, "status", "execution_finished",
                    "started_at", started.toString(), "finished_at", Instant.now().toString(),
                    "java_version", System.getProperty("java.version"), "worker_exit_code", code);
            transaction.publish(ARTIFACTS, hashes, manifest, run);
            out.println("Completed: " + snapshot.dataStatus());
            return code;
        } catch (Exception | LinkageError failure) {
            Object fatal = Json.object("schema_version", 1, "status", "failed", "code", "DUPLICATE_SENSITIVITY_FAILED",
                    "stage", stageName, "error_type", failure.getClass().getSimpleName(), "message", failure.getMessage(),
                    "started_at", started.toString(), "finished_at", Instant.now().toString());
            try {
                err.println(Json.encode(fatal));
                if (transaction != null) transaction.failed(fatal);
            } catch (IOException diagnosticFailure) { err.println("DUPLICATE_SENSITIVITY_FAILED: unable to persist diagnostics"); }
            if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
            return 1;
        }
    }

    private static void verifyWorkerReceipt(Path receipt, String cleanHash, Path facts) throws IOException {
        StrictJson.require(Files.size(receipt) <= 512, "Invalid worker receipt size");
        JsonNode result = StrictJson.read(receipt);
        StrictJson.keys(result, "protocol_version", "clean_manifest_sha256", "facts_sha256");
        StrictJson.integer(result.get("protocol_version"), 1, 1);
        StrictJson.require(StrictJson.hash(result.get("clean_manifest_sha256")).equals(cleanHash),
                "Worker receipt points to a different clean input");
        StrictJson.require(StrictJson.hash(result.get("facts_sha256")).equals(Json.hash(facts)),
                "Order facts differ from the worker receipt");
    }

    private record CleanSnapshot(String manifestHash, String configHash, JsonNode artifacts,
                                 JsonNode counts, String dataStatus) {
        static CleanSnapshot copy(Path input, Path clean) throws IOException {
            for (String name : OrdersDataset.CLEAN_FILES) Files.copy(input.resolve(name), clean.resolve(name));
            Path manifestPath = clean.resolve("manifest.json");
            JsonNode manifest = StrictJson.read(manifestPath);
            StrictJson.version(manifest);
            OrdersDataset.equalText(manifest, "schema", CleaningPolicy.SCHEMA);
            OrdersDataset.equalText(manifest, "policy_version", CleaningPolicy.VERSION);
            OrdersDataset.equalText(manifest, "status", "completed");

            JsonNode config = manifest.get("config");
            StrictJson.keys(config, "policy_version", "service_codes");
            OrdersDataset.equalText(config, "policy_version", CleaningPolicy.VERSION);
            StrictJson.require(config.get("service_codes").isArray(), "service_codes must be an array");
            List<String> codes = new ArrayList<>();
            for (JsonNode value : config.get("service_codes")) codes.add(StrictJson.text(value));
            String configHash = StrictJson.hash(manifest.get("config_sha256"));
            StrictJson.require(new CleaningPolicy(codes).hash().equals(configHash), "Clean config checksum mismatch");

            JsonNode counts = manifest.get("counts");
            JsonNode audit = StrictJson.read(clean.resolve("audit.json"));
            StrictJson.require(counts != null && counts.equals(audit.get("counts")), "Clean audit counts mismatch");
            long quarantined = OrdersDataset.count(counts, "quarantined_rows");
            long warnings = OrdersDataset.count(counts, "warning_rows");
            String dataStatus = quarantined > 0 ? "quarantined" : warnings > 0 ? "warnings" : "ok";
            OrdersDataset.equalText(manifest, "data_status", dataStatus);
            JsonNode artifacts = manifest.get("artifacts");
            StrictJson.keys(artifacts, CleanWriter.ARTIFACTS.toArray(String[]::new));
            CleanSnapshot snapshot = new CleanSnapshot(Json.hash(manifestPath), configHash, artifacts, counts, dataStatus);
            snapshot.verifyHashes(clean);
            return snapshot;
        }

        void verifyHashes(Path clean) throws IOException {
            StrictJson.require(Json.hash(clean.resolve("manifest.json")).equals(manifestHash), "Clean manifest changed");
            for (String name : CleanWriter.ARTIFACTS)
                StrictJson.require(Json.hash(clean.resolve(name)).equals(StrictJson.hash(artifacts.get(name))),
                        "Clean artifact checksum mismatch: " + name);
        }
    }
}
