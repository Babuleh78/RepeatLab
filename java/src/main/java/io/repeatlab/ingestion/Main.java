package io.repeatlab.ingestion;

import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

public final class Main {
    static final String VERSION = "0.1.0";
    private static final String HELP = """
            RepeatLab data CLI
            Usage: java -jar repeatlab-data.jar ingest --input <xlsx> --output <new-directory>
                   java -jar repeatlab-data.jar clean --input <ingest-dir> --config <json> --output <new-directory>
                   java -jar repeatlab-data.jar --help
            Exit codes: 0 completed; 2 completed with data errors; 1 file/structure/I/O failure;
                        64 invalid invocation. Warnings alone return 0.
            A completed manifest is required for consumption of ingest or clean datasets.
            """;

    private Main() {}

    public static void main(String[] args) { System.exit(run(args, System.out, System.err)); }

    public static int run(String[] args, PrintStream out, PrintStream err) {
        if (args.length == 1 && args[0].equals("--help")) { out.print(HELP); return 0; }
        if (args.length > 0 && args[0].equals("clean")) {
            int result = CleanCommand.run(args, out, err);
            if (result == 64) err.print(HELP);
            return result;
        }
        if (args.length != 5 || !args[0].equals("ingest")) { err.print(HELP); return 64; }
        Map<String, String> parameters = new LinkedHashMap<>();
        for (int i = 1; i < args.length; i += 2) {
            if ((!args[i].equals("--input") && !args[i].equals("--output")) || args[i + 1].isBlank()
                    || parameters.putIfAbsent(args[i], args[i + 1]) != null) { err.print(HELP); return 64; }
        }
        if (parameters.size() != 2) { err.print(HELP); return 64; }
        Path input, output;
        try { input = Path.of(parameters.get("--input")); output = Path.of(parameters.get("--output")); }
        catch (InvalidPathException invalid) { err.print(HELP); return 64; }
        TransactionalOutput transaction = null;
        Instant started = Instant.now();
        try {
            transaction = new TransactionalOutput(output);
            Path stage = transaction.staging();
            Path snapshot = stage.resolve("source.xlsx");
            Files.copy(input, snapshot);
            String sourceHash = Json.hash(snapshot);
            WorkbookReader workbook;
            QualityAudit audit;
            try (var raw = Files.newBufferedWriter(stage.resolve("raw_transactions.jsonl"), StandardCharsets.UTF_8);
                 var issues = Files.newBufferedWriter(stage.resolve("issues.jsonl"), StandardCharsets.UTF_8)) {
                audit = new QualityAudit(issues, sourceHash);
                workbook = new WorkbookReader(raw, audit, sourceHash);
                workbook.read(snapshot);
            }
            Files.delete(snapshot);
            Json.write(stage.resolve("audit.json"), audit.summary(workbook.sheets()));
            Map<String, String> hashes = new LinkedHashMap<>();
            for (String artifact : TransactionalOutput.ARTIFACTS) hashes.put(artifact, Json.hash(stage.resolve(artifact)));
            Object stableParameters = Json.object("command", "ingest", "profile", Profile.VERSION);
            Object manifest = Json.object("schema_version", 1, "tool_version", VERSION, "profile", Profile.VERSION,
                    "status", "completed", "data_status", audit.dataStatus(), "date_system", workbook.dateSystem(),
                    "source", Json.object("basename", input.getFileName().toString(), "sha256", sourceHash),
                    "parameters", stableParameters, "parameters_sha256", Json.hashText(Json.encode(stableParameters)),
                    "sheets", workbook.sheets(), "artifacts", hashes);
            Object run = Json.object("schema_version", 1, "status", "execution_finished", "started_at", started.toString(),
                    "finished_at", Instant.now().toString(), "java_version", System.getProperty("java.version"));
            transaction.publish(hashes, manifest, run);
            out.println("Completed: " + audit.dataStatus());
            return audit.hasErrors() ? 2 : 0;
        } catch (Exception failure) {
            Map<String, Object> fatal = Json.object("schema_version", 1, "status", "failed", "code", "INGESTION_FAILED",
                    "error_type", failure.getClass().getSimpleName(), "message", failure.getMessage(),
                    "started_at", started.toString(), "finished_at", Instant.now().toString());
            if (failure instanceof WorkbookFailure workbookFailure) {
                fatal.put("sheet_index", workbookFailure.sheetIndex);
                fatal.put("sheet_name", workbookFailure.sheetName);
                fatal.put("row_number", workbookFailure.rowNumber);
            }
            try {
                err.println(Json.encode(fatal));
                if (transaction != null) transaction.failed(fatal);
            } catch (Exception diagnosticFailure) {
                err.println("INGESTION_FAILED: unable to persist diagnostics: " + diagnosticFailure.getClass().getSimpleName());
            }
            return 1;
        }
    }
}
