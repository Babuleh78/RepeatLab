package io.repeatlab.ingestion;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

final class TransactionalOutput {
    static final List<String> ARTIFACTS = List.of("raw_transactions.jsonl", "issues.jsonl", "audit.json");
    private final Path output;
    private final Path staging;
    private final List<Path> published = new ArrayList<>();

    TransactionalOutput(Path output) throws IOException {
        // Reserving the directory is atomic and never replaces an existing path, including a symlink.
        this.output = privateDirectory(output);
        staging = Files.createDirectory(output.resolve(".partial"));
    }

    Path staging() { return staging; }

    static Path privateDirectory(Path path) throws IOException {
        Path parent = path.toAbsolutePath().getParent();
        if (parent != null && Files.getFileStore(parent).supportsFileAttributeView(PosixFileAttributeView.class))
            return Files.createDirectory(path, PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
        return Files.createDirectory(path);
    }

    void publish(Map<String, String> hashes, Object manifest, Object run) throws IOException {
        publish(ARTIFACTS, hashes, manifest, run);
    }

    void publish(List<String> artifacts, Map<String, String> hashes, Object manifest, Object run) throws IOException {
        for (String name : artifacts) {
            if (!Json.hash(staging.resolve(name)).equals(hashes.get(name)))
                throw new IOException("Artifact checksum mismatch: " + name);
            published.add(Files.move(staging.resolve(name), output.resolve(name)));
        }
        Json.write(staging.resolve("run.json"), run);
        published.add(Files.move(staging.resolve("run.json"), output.resolve("run.json")));
        Path marker = output.resolve(".manifest.partial");
        Json.write(staging.resolve("manifest.json"), manifest);
        published.add(Files.move(staging.resolve("manifest.json"), marker));
        removeTree(staging);
        // This reserved output has no existing manifest. No fallback to a non-atomic completion marker.
        Files.move(marker, output.resolve("manifest.json"), StandardCopyOption.ATOMIC_MOVE);
        published.clear();
    }

    void failed(Object fatal) throws IOException {
        Cleanup cleanup = new Cleanup();
        try {
            for (Path path : published) cleanup.delete(path);
            if (Files.exists(staging, java.nio.file.LinkOption.NOFOLLOW_LINKS)) cleanup.tree(staging);
            cleanup.check();
        } finally {
            Json.write(output.resolve("fatal.json"), fatal);
        }
    }

    static void removeTree(Path root) throws IOException {
        Cleanup cleanup = new Cleanup();
        cleanup.tree(root);
        cleanup.check();
    }

    // A locked file must not prevent removal of other data from this run.
    private static final class Cleanup extends SimpleFileVisitor<Path> {
        private IOException firstFailure;

        void check() throws IOException { if (firstFailure != null) throw firstFailure; }

        void record(IOException failure) { if (firstFailure == null) firstFailure = failure; }

        void delete(Path path) {
            try { Files.deleteIfExists(path); }
            catch (IOException failure) { record(failure); }
        }

        void tree(Path root) {
            try { Files.walkFileTree(root, this); } // Default traversal does not follow symbolic links.
            catch (IOException failure) { record(failure); }
        }

        @Override public FileVisitResult visitFile(Path file, BasicFileAttributes attributes) {
            delete(file);
            return FileVisitResult.CONTINUE;
        }

        @Override public FileVisitResult visitFileFailed(Path file, IOException failure) {
            record(failure);
            return FileVisitResult.CONTINUE;
        }

        @Override public FileVisitResult postVisitDirectory(Path directory, IOException failure) {
            if (failure != null) record(failure);
            delete(directory);
            return FileVisitResult.CONTINUE;
        }
    }
}
