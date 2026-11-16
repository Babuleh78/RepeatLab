package io.repeatlab.ingestion;

import java.io.PrintStream;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

final class CleanCommand {
    private CleanCommand() {}

    static int run(String[] args, PrintStream out, PrintStream err) {
        Map<String, String> parameters = new HashMap<>();
        if (args.length != 7) return 64;
        for (int i = 1; i < args.length; i += 2) {
            if (!Set.of("--input", "--config", "--output").contains(args[i]) || args[i + 1].isBlank()
                    || parameters.putIfAbsent(args[i], args[i + 1]) != null) return 64;
        }
        Path input, config, output;
        try {
            input = Path.of(parameters.get("--input"));
            config = Path.of(parameters.get("--config"));
            output = Path.of(parameters.get("--output"));
        } catch (InvalidPathException invalid) { return 64; }
        TransactionalOutput transaction = null;
        Instant started = Instant.now();
        try {
            transaction = new TransactionalOutput(output);
            CleaningPolicy policy = CleaningPolicy.read(config);
            RawDataset raw = new RawDataset(input, transaction.staging());
            CleanWriter writer = new CleanWriter(raw, policy, transaction.staging());
            raw.releaseSnapshot();
            Map<String, String> hashes = new LinkedHashMap<>();
            for (String name : CleanWriter.ARTIFACTS) hashes.put(name, Json.hash(transaction.staging().resolve(name)));
            Object manifest = Json.object("schema", CleaningPolicy.SCHEMA, "schema_version", 1,
                    "tool_version", Main.VERSION, "policy_version", CleaningPolicy.VERSION,
                    "status", "completed", "data_status", writer.dataStatus(), "input", raw.provenance(),
                    "config", policy.canonical(), "config_sha256", policy.hash(), "counts", writer.counts(), "artifacts", hashes);
            Object run = Json.object("schema_version", 1, "status", "execution_finished", "started_at", started.toString(),
                    "finished_at", Instant.now().toString(), "java_version", System.getProperty("java.version"));
            transaction.publish(CleanWriter.ARTIFACTS, hashes, manifest, run);
            out.println("Completed: " + writer.dataStatus());
            return writer.quarantined() ? 2 : 0;
        } catch (Exception failure) {
            Object fatal = Json.object("schema_version", 1, "status", "failed", "code", "CLEANING_FAILED",
                    "error_type", failure.getClass().getSimpleName(), "message", failure.getMessage(),
                    "started_at", started.toString(), "finished_at", Instant.now().toString());
            try {
                err.println(Json.encode(fatal));
                if (transaction != null) transaction.failed(fatal);
            } catch (Exception diagnosticFailure) {
                err.println("CLEANING_FAILED: unable to persist diagnostics: " + diagnosticFailure.getClass().getSimpleName());
            }
            return 1;
        }
    }
}
