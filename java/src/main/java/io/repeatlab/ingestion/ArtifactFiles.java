package io.repeatlab.ingestion;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;

final class ArtifactFiles {
    private ArtifactFiles() {}

    static InputStream open(Path path) throws IOException {
        var attributes = Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        if (!attributes.isRegularFile()) throw new DiagnosticFailure("Expected a regular artifact file");
        return Files.newInputStream(path, LinkOption.NOFOLLOW_LINKS);
    }

    static BufferedReader reader(Path path) throws IOException {
        return new BufferedReader(new InputStreamReader(open(path), StandardCharsets.UTF_8.newDecoder()));
    }

    static void copy(Path source, Path target) throws IOException {
        try (var input = open(source)) {
            Files.copy(input, target);
        }
    }
}
