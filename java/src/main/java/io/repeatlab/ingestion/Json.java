package io.repeatlab.ingestion;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;

final class Json {
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);

    private Json() {}

    static Map<String, Object> object(Object... pairs) {
        Map<String, Object> result = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) result.put((String) pairs[i], pairs[i + 1]);
        return result;
    }

    static String encode(Object value) throws IOException { return MAPPER.writeValueAsString(value); }

    static void line(Writer writer, Object value) throws IOException {
        writer.write(encode(value));
        writer.write('\n');
    }

    static void write(Path path, Object value) throws IOException {
        Files.writeString(path, encode(value) + "\n", StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW);
    }

    static String hash(Path path) throws IOException {
        MessageDigest digest = digest();
        try (var input = Files.newInputStream(path)) {
            byte[] buffer = new byte[65536];
            for (int length; (length = input.read(buffer)) != -1;) digest.update(buffer, 0, length);
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    static String hashText(String value) {
        return HexFormat.of().formatHex(digest().digest(value.getBytes(StandardCharsets.UTF_8)));
    }

    private static MessageDigest digest() {
        try { return MessageDigest.getInstance("SHA-256"); }
        catch (NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); }
    }
}
