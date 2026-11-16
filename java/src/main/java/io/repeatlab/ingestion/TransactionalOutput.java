package io.repeatlab.ingestion;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Map;

final class TransactionalOutput {
    static final List<String> ARTIFACTS = List.of("raw_transactions.jsonl", "issues.jsonl", "audit.json");
    private final Path output;
    private final Path staging;

    TransactionalOutput(Path output) throws IOException {
        // Reserving the directory is atomic and never replaces an existing path, including a symlink.
        this.output = Files.createDirectory(output);
        staging = Files.createDirectory(output.resolve(".partial"));
    }

    Path staging() { return staging; }

    void publish(Map<String, String> hashes, Object manifest, Object run) throws IOException {
        publish(ARTIFACTS, hashes, manifest, run);
    }

    void publish(List<String> artifacts, Map<String, String> hashes, Object manifest, Object run) throws IOException {
        for (String name : artifacts) {
            if (!Json.hash(staging.resolve(name)).equals(hashes.get(name)))
                throw new IOException("Artifact checksum mismatch: " + name);
            Files.move(staging.resolve(name), output.resolve(name));
        }
        Json.write(output.resolve("run.json"), run);
        Json.write(staging.resolve("manifest.json"), manifest);
        // This reserved output has no existing manifest. No fallback to a non-atomic completion marker.
        Files.move(staging.resolve("manifest.json"), output.resolve("manifest.json"), StandardCopyOption.ATOMIC_MOVE);
    }

    void failed(Object fatal) throws IOException { Json.write(output.resolve("fatal.json"), fatal); }
}
