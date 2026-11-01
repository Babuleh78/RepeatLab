package io.repeatlab.ingestion;

import static org.junit.jupiter.api.Assertions.*;
import static io.repeatlab.ingestion.WorkbookFixture.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class IngestionTest {
    @TempDir Path temp;
    private static final ObjectMapper JSON = new ObjectMapper();

    private int ingest(Path source, Path target) throws Exception {
        return invoke("ingest", "--input", source.toString(), "--output", target.toString());
    }

    private int invoke(String... args) throws Exception {
        // Reflection allows the contract to fail as an assertion before the CLI exists.
        try {
            Class<?> main = Class.forName("io.repeatlab.ingestion.Main");
            try (PrintStream quiet = new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8)) {
                return (Integer) main.getMethod("run", String[].class, PrintStream.class, PrintStream.class)
                        .invoke(null, args, quiet, quiet);
            }
        } catch (ClassNotFoundException missing) {
            fail("The ingestion CLI has not been implemented", missing);
            return -1;
        }
    }

    private JsonNode json(Path path) throws Exception { return JSON.readTree(path.toFile()); }
    private List<JsonNode> lines(Path path) throws Exception {
        return Files.readAllLines(path).stream().map(line -> {
            try { return JSON.readTree(line); } catch (Exception e) { throw new IllegalArgumentException(e); }
        }).toList();
    }

    @Test void preservesExactLexemesAllSheetsAndSparsePhysicalRows() throws Exception {
        Path source = write(temp.resolve("source.xlsx"), false, header() + data(3) + "<row r=\"5\"/>", header() + data(2), "");
        byte[] before = Files.readAllBytes(source);
        Path out = temp.resolve("out");
        assertEquals(0, ingest(source, out));
        List<JsonNode> raw = lines(out.resolve("raw_transactions.jsonl"));
        assertEquals(5, raw.size());
        JsonNode row = raw.get(1);
        assertEquals(3, row.path("row_number").asInt());
        assertEquals("data", row.path("kind").asText());
        assertEquals("0.1000", row.path("cells").get(5).path("value").asText());
        assertEquals("0.1000", row.path("cells").get(5).path("raw_value").asText());
        assertEquals("00042", row.path("cells").get(6).path("value").asText());
        assertEquals(" product\u00a0 ", row.path("cells").get(2).path("value").asText());
        assertEquals("missing", row.path("cells").get(8).path("state").asText());
        assertEquals("empty", raw.get(2).path("kind").asText());
        JsonNode manifest = json(out.resolve("manifest.json"));
        assertEquals(3, manifest.path("sheets").size());
        assertEquals("hidden", manifest.path("sheets").get(1).path("state").asText());
        assertEquals(2, manifest.path("sheets").get(0).path("absent_rows").asInt());
        assertEquals(0, manifest.path("sheets").get(2).path("physical_rows").asInt());
        assertArrayEquals(before, Files.readAllBytes(source));
        assertEquals("1900-01-01T12:00:00", json(out.resolve("audit.json")).path("date_range").path("min").asText());
        assertTrue(lines(out.resolve("issues.jsonl")).stream().anyMatch(n -> n.path("code").asText().equals("POSSIBLE_DUPLICATE")));
    }

    @Test void hashesArtifactsAndReproducesStableOutputs() throws Exception {
        Path source = write(temp.resolve("source.xlsx"), false, header() + data(2));
        Path first = temp.resolve("first"), second = temp.resolve("second");
        assertEquals(0, ingest(source, first));
        assertEquals(0, ingest(source, second));
        for (String name : List.of("raw_transactions.jsonl", "issues.jsonl", "audit.json", "manifest.json"))
            assertArrayEquals(Files.readAllBytes(first.resolve(name)), Files.readAllBytes(second.resolve(name)));
        JsonNode manifest = json(first.resolve("manifest.json"));
        assertEquals(hash(source), manifest.path("source").path("sha256").asText());
        for (String name : List.of("raw_transactions.jsonl", "issues.jsonl", "audit.json"))
            assertEquals(hash(first.resolve(name)), manifest.path("artifacts").path(name).asText());
        assertEquals("completed", manifest.path("status").asText());
        Path changed = write(temp.resolve("changed.xlsx"), false, header() + data(2).replace("0.1000", "0.1001"));
        Path third = temp.resolve("third");
        assertEquals(0, ingest(changed, third));
        assertNotEquals(manifest.path("source").path("sha256"), json(third.resolve("manifest.json")).path("source").path("sha256"));
    }

    @Test void keepsRowErrorsAndDoesNotTrustFormulaCaches() throws Exception {
        String bad = data(2).replace("<v>2</v>", "<v>abc</v>").replace("<v>1.5</v>", "<v>60</v>")
                .replace("<v>0.1000</v>", "<f>1/0</f><v>-99</v>");
        Path source = write(temp.resolve("bad.xlsx"), false, header() + bad + data(3));
        Path out = temp.resolve("out");
        assertEquals(2, ingest(source, out));
        assertEquals(3, lines(out.resolve("raw_transactions.jsonl")).size());
        List<JsonNode> issues = lines(out.resolve("issues.jsonl"));
        assertTrue(issues.stream().anyMatch(n -> n.path("code").asText().equals("INVALID_QUANTITY") && n.path("severity").asText().equals("error")));
        assertTrue(issues.stream().anyMatch(n -> n.path("code").asText().equals("INVALID_DATE")));
        assertTrue(issues.stream().anyMatch(n -> n.path("code").asText().equals("UNVERIFIED_FORMULA")));
        assertFalse(issues.stream().anyMatch(n -> n.path("code").asText().equals("NONPOSITIVE_PRICE")));
        JsonNode cell = lines(out.resolve("raw_transactions.jsonl")).get(1).path("cells").get(5);
        assertEquals("1/0", cell.path("formula").path("text").asText());
        assertEquals("-99", cell.path("cached_value").asText());
        assertEquals("errors", json(out.resolve("manifest.json")).path("data_status").asText());
    }

    @Test void distinguishesBlankEmptyStringAndMissing() throws Exception {
        String cells = data(2).replace(text("C2", " product\u00a0 "), "<c r=\"C2\"/>")
                .replace(text("G2", "00042"), text("G2", ""));
        Path source = write(temp.resolve("blank.xlsx"), true, header() + cells);
        Path out = temp.resolve("out");
        assertEquals(0, ingest(source, out));
        JsonNode row = lines(out.resolve("raw_transactions.jsonl")).get(1);
        assertEquals("blank", row.path("cells").get(2).path("state").asText());
        assertEquals("empty_string", row.path("cells").get(6).path("state").asText());
        assertEquals("missing", row.path("cells").get(8).path("state").asText());
        assertEquals("1904-01-02T12:00:00", json(out.resolve("audit.json")).path("date_range").path("min").asText());
    }

    @Test void rejectsStructuralFailuresWithoutCompletedManifest() throws Exception {
        Path corrupt = Files.writeString(temp.resolve("corrupt.xlsx"), "not a zip");
        Path badSchema = write(temp.resolve("schema.xlsx"), false, header().replace("Quantity", "Wrong"));
        Path collision = write(temp.resolve("collision.xlsx"), false, header().replace("Extra", "Invoice"));
        int index = 0;
        for (Path source : List.of(corrupt, badSchema, collision)) {
            Path out = temp.resolve("failure" + index++);
            assertEquals(1, ingest(source, out));
            assertFalse(Files.exists(out.resolve("manifest.json")));
            assertEquals("failed", json(out.resolve("fatal.json")).path("status").asText());
        }
    }

    @Test void auditsSignsMissingKeysAndStrictTypesWithoutCleaning() throws Exception {
        String suspicious = data(2).replace(text("A2", "X1"), text("A2", "C1"))
                .replace("<v>2</v>", "<v>0</v>").replace("<v>0.1000</v>", "<v>-0.20</v>")
                .replace(text("G2", "00042"), "");
        String bad = data(3).replace("<v>2</v>", "<v>1.5</v>")
                .replace("<v>0.1000</v>", "<v>NaN</v>")
                .replace(number("E3", "1.5"), text("E3", "2024-02-30T10:00:00"));
        Path source = write(temp.resolve("types.xlsx"), false, header() + suspicious + bad + data(4));
        Path out = temp.resolve("out");
        assertEquals(2, ingest(source, out));
        List<JsonNode> issues = lines(out.resolve("issues.jsonl"));
        for (String code : List.of("ZERO_QUANTITY", "NONPOSITIVE_PRICE", "CANCELLATION_MISMATCH", "MISSING_CUSTOMER_ID"))
            assertTrue(issues.stream().anyMatch(n -> n.path("code").asText().equals(code) && n.path("severity").asText().equals("warning")), code);
        for (String code : List.of("INVALID_QUANTITY", "INVALID_PRICE", "INVALID_DATE"))
            assertTrue(issues.stream().anyMatch(n -> n.path("code").asText().equals(code) && n.path("severity").asText().equals("error")), code);
        assertEquals(4, lines(out.resolve("raw_transactions.jsonl")).size());
    }

    @Test void acceptsAliasesAndRejectsTimezoneAssumptions() throws Exception {
        String aliases = header().replace("InvoiceNo", "Invoice").replace("UnitPrice", "Price")
                .replace("CustomerID", "Customer ID");
        String row = data(2).replace(number("E2", "1.5"), text("E2", "2024-02-29T13:14:15"));
        Path source = write(temp.resolve("iso.xlsx"), false, aliases + row);
        Path out = temp.resolve("out");
        assertEquals(0, ingest(source, out));
        assertEquals("2024-02-29T13:14:15", json(out.resolve("audit.json")).path("date_range").path("max").asText());
        source = write(temp.resolve("offset.xlsx"), false, aliases + row.replace("13:14:15", "13:14:15Z"));
        assertEquals(2, ingest(source, temp.resolve("offset")));
    }

    @Test void preservesSharedRichTextAndBlankHeaderGaps() throws Exception {
        String sparseHeader = header().replace(text("I1", "Extra"), "<c r=\"I1\"/>" + text("K1", "extra"));
        String row = data(2).replace(text("G2", "00042"), "<c r=\"G2\" t=\"s\"><v>0</v></c>");
        Path source = write(temp.resolve("shared.xlsx"), false, sparseHeader + row);
        Path out = temp.resolve("out");
        assertEquals(0, ingest(source, out));
        JsonNode record = lines(out.resolve("raw_transactions.jsonl")).get(1);
        assertEquals(" 00042\u00a0 ", record.path("cells").get(6).path("value").asText());
        assertEquals("0", record.path("cells").get(6).path("raw_value").asText());
        assertEquals("s", record.path("cells").get(6).path("source_type").asText());
        assertEquals(11, record.path("cells").size());
    }

    @Test void refusesExistingOutputAndInvalidInvocations() throws Exception {
        Path source = write(temp.resolve("source.xlsx"), false, header() + data(2));
        Path out = Files.createDirectory(temp.resolve("existing"));
        Files.writeString(out.resolve("sentinel"), "keep");
        assertEquals(1, ingest(source, out));
        assertEquals(List.of("sentinel"), Files.list(out).map(p -> p.getFileName().toString()).toList());
        assertEquals("keep", Files.readString(out.resolve("sentinel")));
        assertEquals(64, invoke());
        assertEquals(64, invoke("ingest", "--input", source.toString()));
        assertEquals(64, invoke("ingest", "--input", source.toString(), "--input", source.toString(), "--output", "x"));
        assertEquals(0, invoke("--help"));
        Path fileParent = Files.writeString(temp.resolve("parent-file"), "keep");
        assertEquals(1, ingest(source, fileParent.resolve("out")));
        assertEquals("keep", Files.readString(fileParent));
    }

    @Test void failedPublicationNeverOverwritesFilesOrPublishesManifest() throws Exception {
        Path out = temp.resolve("publication");
        TransactionalOutput transaction = new TransactionalOutput(out);
        java.util.Map<String, String> hashes = new java.util.LinkedHashMap<>();
        for (String name : List.of("raw_transactions.jsonl", "issues.jsonl", "audit.json")) {
            Path artifact = Files.writeString(transaction.staging().resolve(name), "{}\n");
            hashes.put(name, hash(artifact));
        }
        Files.writeString(out.resolve("issues.jsonl"), "existing");
        assertThrows(java.io.IOException.class,
                () -> transaction.publish(hashes, java.util.Map.of("status", "completed"), java.util.Map.of()));
        assertEquals("existing", Files.readString(out.resolve("issues.jsonl")));
        assertFalse(Files.exists(out.resolve("manifest.json")));
    }

    @Test void failureAfterFirstSheetHasStructuredOriginAndNoCompletion() throws Exception {
        Path source = write(temp.resolve("later.xlsx"), false, header() + data(2), header().replace("Quantity", "wrong"));
        Path out = temp.resolve("out");
        assertEquals(1, ingest(source, out));
        assertFalse(Files.exists(out.resolve("manifest.json")));
        JsonNode fatal = json(out.resolve("fatal.json"));
        assertEquals("Sheet2", fatal.path("sheet_name").asText());
        assertEquals(1, fatal.path("row_number").asInt());
        assertEquals("failed", fatal.path("status").asText());
    }

    @Test void excludesPhoneticAnnotationsFromSharedAndInlineIdentifiers() throws Exception {
        String shared = data(2).replace(text("G2", "00042"), "<c r=\"G2\" t=\"s\"><v>1</v></c>");
        String inline = data(3).replace(text("G3", "00042"),
                "<c r=\"G3\" t=\"inlineStr\"><is><t>BASE</t><rPh sb=\"0\" eb=\"4\"><t>READING</t></rPh></is></c>");
        Path source = write(temp.resolve("phonetic.xlsx"), false, header() + shared + inline);
        Path out = temp.resolve("out");
        assertEquals(0, ingest(source, out));
        List<JsonNode> raw = lines(out.resolve("raw_transactions.jsonl"));
        assertAll(
                () -> assertEquals("BASE", raw.get(1).path("cells").get(6).path("value").asText()),
                () -> assertEquals("BASE", raw.get(2).path("cells").get(6).path("value").asText()),
                () -> assertEquals("1", raw.get(1).path("cells").get(6).path("raw_value").asText()));
    }

    @ParameterizedTest
    @ValueSource(strings = {"inlineStr", "d"})
    void acceptsYear9999ForTextAndDateCells(String type) throws Exception {
        String value = "9999-12-31T23:59:59";
        String cell = type.equals("inlineStr") ? text("E2", value)
                : "<c r=\"E2\" t=\"d\"><v>" + value + "</v></c>";
        Path source = write(temp.resolve("last-year.xlsx"), false,
                header() + data(2).replace(number("E2", "1.5"), cell));
        Path out = temp.resolve("out");
        assertEquals(0, ingest(source, out));
        assertEquals(value, json(out.resolve("audit.json")).path("date_range").path("max").asText());
    }

    @ParameterizedTest
    @ValueSource(strings = {"inlineStr", "d"})
    void reportsYear10000WithoutLosingTextOrDateCells(String type) throws Exception {
        String value = "+10000-01-01T00:00:00";
        String cell = type.equals("inlineStr") ? text("E2", value)
                : "<c r=\"E2\" t=\"d\"><v>" + value + "</v></c>";
        Path source = write(temp.resolve("later-year.xlsx"), false,
                header() + data(2).replace(number("E2", "1.5"), cell) + data(3));
        Path out = temp.resolve("out");
        assertEquals(2, ingest(source, out));
        List<JsonNode> raw = lines(out.resolve("raw_transactions.jsonl"));
        assertEquals(3, raw.size());
        assertEquals(value, raw.get(1).path("cells").get(4).path("value").asText());
        assertEquals(type, raw.get(1).path("cells").get(4).path("source_type").asText());
        List<JsonNode> issues = lines(out.resolve("issues.jsonl"));
        assertTrue(issues.stream().anyMatch(n -> n.path("code").asText().equals("INVALID_DATE")
                && n.path("severity").asText().equals("error") && n.path("row_number").asInt() == 2
                && n.path("column").asText().equals("E")));
        assertEquals("1900-01-01T12:00:00", json(out.resolve("audit.json")).path("date_range").path("max").asText());
        assertEquals("errors", json(out.resolve("manifest.json")).path("data_status").asText());
    }

    private static String hash(Path path) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(path)));
    }
}
