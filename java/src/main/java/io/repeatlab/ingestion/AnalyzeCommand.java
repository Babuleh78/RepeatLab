package io.repeatlab.ingestion;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

final class AnalyzeCommand {
    private AnalyzeCommand() {}
    static int run(String[] args, PrintStream out, PrintStream err) {
        Map<String, String> parameters = new HashMap<>();
        if (args.length != 7) return 64;
        for (int i = 1; i < args.length; i += 2)
            if (!Set.of("--input", "--output", "--python").contains(args[i]) || args[i + 1].isBlank()
                    || parameters.putIfAbsent(args[i], args[i + 1]) != null) return 64;
        Path input, output;
        try { input = Path.of(parameters.get("--input")); output = Path.of(parameters.get("--output")); }
        catch (InvalidPathException failure) { return 64; }
        boolean reserved = false;
        String stageName = "snapshot";
        Instant started = Instant.now();
        try {
            Files.createDirectory(output); reserved = true;
            Path stage = Files.createDirectory(output.resolve(".partial"));
            Path clean = Files.createDirectory(stage.resolve("clean"));
            Map<String, String> snapshotHashes = new LinkedHashMap<>();
            for (String name : OrdersDataset.CLEAN_FILES) {
                Files.copy(input.resolve(name), clean.resolve(name));
                snapshotHashes.put(name, Json.hash(clean.resolve(name)));
            }
            stageName = "worker";
            int code = worker(parameters.get("--python"), clean, output.resolve("orders"), output);
            StrictJson.require(code == 0 || code == 2, "Orders worker exited " + code);
            for (var entry : snapshotHashes.entrySet())
                StrictJson.require(Json.hash(clean.resolve(entry.getKey())).equals(entry.getValue()), "Worker changed clean snapshot");
            stageName = "orders_validation";
            OrdersDataset dataset = new OrdersDataset(output.resolve("orders"), clean, stage.resolve("verified-orders"));
            StrictJson.require(code == (dataset.dataStatus.equals("quarantined") ? 2 : 0), "Worker exit/quality mismatch");
            stageName = "sql";
            SqlAnalytics.Result sql = SqlAnalytics.run(dataset.orders, stage);
            Json.write(stage.resolve("audit.json"), Json.object("schema", "repeatlab-analytics", "schema_version", 1,
                    "orders_counts", dataset.counts, "source_clean_counts", dataset.sourceCounts,
                    "monthly_activity_rows", sql.monthlyRows(), "missing_customer_rows", sql.missingRows(),
                    "decimal_scale", sql.scale(), "data_status", dataset.dataStatus));
            stageName = "publication";
            Map<String, String> artifacts = new LinkedHashMap<>();
            artifacts.put("orders/manifest.json", dataset.manifestHash);
            StrictJson.require(Json.hash(output.resolve("orders/manifest.json")).equals(dataset.manifestHash), "Orders manifest changed");
            for (String name : OrdersDataset.ARTIFACTS)
                StrictJson.require(Json.hash(output.resolve("orders").resolve(name)).equals(dataset.manifest.get("artifacts").get(name).textValue()), "Orders artifact changed");
            for (String name : List.of("monthly_activity.jsonl", "missing_customer.jsonl", "audit.json")) {
                artifacts.put(name, Json.hash(stage.resolve(name)));
                Files.move(stage.resolve(name), output.resolve(name));
            }
            Object manifest = Json.object("schema", "repeatlab-analytics", "schema_version", 1,
                    "policy_version", "analytics-v1", "status", "completed", "data_status", dataset.dataStatus,
                    "input", Json.object("orders_manifest_sha256", dataset.manifestHash, "clean_manifest_sha256", dataset.cleanHash),
                    "engine", Json.object("name", "DuckDB", "version", sql.engine()), "decimal_scale", sql.scale(),
                    "sql_sha256", sql.sqlHashes(), "artifacts", artifacts);
            removeTree(stage);
            Json.write(output.resolve("run.json"), Json.object("schema_version", 1, "status", "execution_finished",
                    "started_at", started.toString(), "finished_at", Instant.now().toString(),
                    "java_version", System.getProperty("java.version"), "worker_exit_code", code));
            Path marker = output.resolve(".manifest.partial");
            Json.write(marker, manifest);
            Files.move(marker, output.resolve("manifest.json"), StandardCopyOption.ATOMIC_MOVE);
            out.println("Completed: " + dataset.dataStatus);
            return code;
        } catch (Exception | LinkageError failure) {
            Object fatal = Json.object("schema_version", 1, "status", "failed", "code", "ANALYTICS_FAILED",
                    "stage", stageName, "error_type", failure.getClass().getSimpleName(), "message", failure.getMessage(),
                    "started_at", started.toString(), "finished_at", Instant.now().toString());
            try {
                err.println(Json.encode(fatal));
                if (reserved) Json.write(output.resolve("fatal.json"), fatal);
            } catch (IOException diagnosticFailure) { err.println("ANALYTICS_FAILED: unable to persist diagnostics"); }
            if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
            return 1;
        }
    }

    static int worker(String python, Path clean, Path orders, Path output) throws IOException, InterruptedException {
        return runProcess(List.of(python, "-m", "repeatlab.orders", "--input", clean.toAbsolutePath().toString(),
                "--output", orders.toAbsolutePath().toString()), output);
    }

    static int runProcess(List<String> command, Path output) throws IOException, InterruptedException {
        // Direct arguments and file redirection keep diagnostic streams out of the data protocol and avoid pipe deadlock.
        Process process = new ProcessBuilder(command)
                .redirectOutput(output.resolve("worker.stdout.log").toFile())
                .redirectError(output.resolve("worker.stderr.log").toFile()).start();
        try { return process.waitFor(); }
        catch (InterruptedException interrupted) {
            process.destroyForcibly();
            try { process.waitFor(); } catch (InterruptedException again) { interrupted.addSuppressed(again); }
            throw interrupted;
        }
    }

    static void removeTree(Path root) throws IOException {
        // Only private stage paths created by this invocation are passed here; no symbolic links are followed.
        try (var paths = Files.walk(root)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) Files.delete(path);
        }
    }
}
