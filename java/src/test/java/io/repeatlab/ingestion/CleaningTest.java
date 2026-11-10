package io.repeatlab.ingestion;

import static org.junit.jupiter.api.Assertions.*;
import static io.repeatlab.ingestion.WorkbookFixture.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class CleaningTest {
    @TempDir Path temp;
    private static final ObjectMapper JSON = new ObjectMapper();

    private int invoke(String... args) throws Exception {
        try (PrintStream quiet = new PrintStream(new ByteArrayOutputStream())) {
            return Main.run(args, quiet, quiet);
        }
    }

    private Path clean(String configText, int expected, String... sheets) throws Exception {
        Path folder = Files.createTempDirectory(temp, "case");
        Path source = write(folder.resolve("source.xlsx"), false, sheets);
        Path raw = folder.resolve("raw");
        int ingestExit = invoke("ingest", "--input", source.toString(), "--output", raw.toString());
        assertTrue(ingestExit == 0 || ingestExit == 2);
        Path config = folder.resolve("config.json");
        Files.writeString(config, configText);
        Path target = folder.resolve("clean");
        assertEquals(expected, invoke("clean", "--input", raw.toString(), "--config", config.toString(),
                "--output", target.toString()));
        return target;
    }

    private static final String CONFIG = "{\"policy_version\":\"clean-v1\",\"service_codes\":[\"SERVICE\"]}";

    @Test void laterBadServiceQuarantinesWholeInvoice() throws Exception {
        String bad = data(2).replace("0007", "SERVICE").replace("0.1000", "invalid");
        Path target = clean(CONFIG, 2, header() + data(2), header() + bad + data(3).replace("X1", "OK"));
        assertEquals(1, Files.readAllLines(target.resolve("clean_events.jsonl")).size());
        var rows = Files.readAllLines(target.resolve("dispositions.jsonl"));
        assertEquals(3, rows.size());
        assertEquals("quarantined", JSON.readTree(rows.get(0)).path("status").textValue());
        assertTrue(JSON.readTree(rows.get(0)).path("reasons").toString().contains("INVOICE_INVALID"));
        assertTrue(JSON.readTree(rows.get(1)).path("reasons").toString().contains("INVALID_UNIT_PRICE"));
    }

    @Test void serviceConflictsRemainVisibleEvenWhenAnotherFieldIsInvalid() throws Exception {
        String service = data(3).replace("0007", "SERVICE").replace("00042", "OTHER")
                .replace("1.5", "2.5").replace("0.1000", "invalid");
        Path target = clean(CONFIG, 2, header() + data(2) + service);
        assertEquals("", Files.readString(target.resolve("clean_events.jsonl")));
        String reasons = JSON.readTree(Files.readAllLines(target.resolve("dispositions.jsonl")).get(0)).path("reasons").toString();
        assertTrue(reasons.contains("INVOICE_CONFLICT_CUSTOMER_ID"));
        assertTrue(reasons.contains("INVOICE_CONFLICT_EVENT_TIME"));
    }

    @Test void timeFloorsWithoutCrossingMidnightAndChecksExactConflicts() throws Exception {
        String base = data(2).replace(number("E2", "1.5"), text("E2", "2020-01-01T23:59:59.9999996"));
        Path target = clean(CONFIG, 0, header() + base);
        JsonNode event = JSON.readTree(Files.readString(target.resolve("clean_events.jsonl")));
        assertEquals("2020-01-01T23:59:59.999999", event.path("event_time").textValue());
        String other = data(3).replace(number("E3", "1.5"), text("E3", "2020-01-01T23:59:59.9999997"));
        Path conflict = clean(CONFIG, 2, header() + base + other);
        assertEquals("", Files.readString(conflict.resolve("clean_events.jsonl")));
        Path serial = clean(CONFIG, 0, header() + data(2).replace("1.5", "1.00000000001"));
        assertEquals("1900-01-01T00:00:00", JSON.readTree(Files.readString(serial.resolve("clean_events.jsonl"))).path("event_time").textValue());
    }

    @Test void numericIdsNormalizeButLiteralIdsAndWhitespaceFollowPolicy() throws Exception {
        String numeric = data(2).replace(text("A2", "X1"), number("A2", "4.2E1"))
                .replace(text("G2", "00042"), text("G2", "\u00a0"));
        Path target = clean(CONFIG, 0, header() + numeric);
        JsonNode event = JSON.readTree(Files.readString(target.resolve("clean_events.jsonl")));
        assertEquals("42", event.path("invoice_id").textValue());
        assertTrue(event.path("customer_id").isNull());
    }

    @Test void strictPolicyRejectsDuplicateKeysAndUnknownFields() throws Exception {
        for (String config : new String[] {
                "{\"policy_version\":\"clean-v1\",\"service_codes\":[],\"extra\":1}",
                "{\"policy_version\":\"clean-v1\",\"service_codes\":[],\"service_codes\":[]}"}) {
            Path target = clean(config, 1, header() + data(2));
            assertFalse(Files.exists(target.resolve("manifest.json")));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"zero", "price", "sign", "fractional_id", "formula", "serial60", "year0", "year10000"})
    void invalidNecessaryFieldCannotProducePartialInvoice(String problem) throws Exception {
        String bad = data(3);
        bad = switch (problem) {
            case "zero" -> bad.replace(number("D3", "2"), number("D3", "0"));
            case "price" -> bad.replace("0.1000", "0");
            case "sign" -> bad.replace(number("D3", "2"), number("D3", "-2"));
            case "fractional_id" -> bad.replace(text("G3", "00042"), number("G3", "42.1"));
            case "formula" -> bad.replace(number("F3", "0.1000"), "<c r=\"F3\"><f>1/10</f><v>0.1</v></c>");
            case "serial60" -> bad.replace("1.5", "60.5");
            case "year0" -> bad.replace(number("E3", "1.5"), text("E3", "0000-01-01"));
            default -> bad.replace(number("E3", "1.5"), text("E3", "+10000-01-01"));
        };
        Path target = clean(CONFIG, 2, header() + data(2) + bad);
        assertEquals("", Files.readString(target.resolve("clean_events.jsonl")));
        assertEquals(2, Files.readAllLines(target.resolve("dispositions.jsonl")).size());
    }

    @Test void equivalentExactTimesAndNoncriticalFormulaAreAccepted() throws Exception {
        String first = data(2).replace(number("E2", "1.5"), text("E2", "2020-01-01T12:00:00.1"));
        String second = data(3).replace(number("E3", "1.5"), text("E3", "2020-01-01T12:00:00.1000"))
                .replace(text("H3", "Testland"), "<c r=\"H3\"><f>1</f><v>1</v></c>");
        assertEquals(2, Files.readAllLines(clean(CONFIG, 0, header() + first + second).resolve("clean_events.jsonl")).size());
        String iso = data(3).replace(number("E3", "1.5"), text("E3", "1900-01-01T12:00:00"));
        assertEquals(2, Files.readAllLines(clean(CONFIG, 0, header() + data(2) + iso).resolve("clean_events.jsonl")).size());
    }

    @Test void missingInvoiceIsIsolatedAndCancellationIsRetained() throws Exception {
        String missing = data(2).replace(text("A2", "X1"), text("A2", " "));
        String cancel = data(3).replace("X1", "c1").replace(number("D3", "2"), number("D3", "-2"));
        Path target = clean(CONFIG, 2, header() + missing + cancel);
        JsonNode event = JSON.readTree(Files.readString(target.resolve("clean_events.jsonl")));
        assertEquals("cancellation", event.path("kind").textValue());
        assertEquals(-2, event.path("quantity").intValue());
    }

    @Test void emptySheetsCompleteWithZeroCoverage() throws Exception {
        Path target = clean(CONFIG, 0, "", header());
        assertEquals("", Files.readString(target.resolve("clean_events.jsonl")));
        assertEquals("", Files.readString(target.resolve("dispositions.jsonl")));
        JsonNode audit = JSON.readTree(target.resolve("audit.json").toFile());
        assertEquals(0, audit.path("counts").path("data_rows").intValue());
        assertEquals(1, audit.path("counts").path("header_rows").intValue());
    }

    @Test void completedOutputReleasesSnapshotWithoutChangingRaw() throws Exception {
        Path target = clean(CONFIG, 0, header() + data(2));
        assertFalse(Files.exists(target.resolve(".partial/input")));
        assertTrue(Files.isRegularFile(target.getParent().resolve("raw/raw_transactions.jsonl")));
        assertTrue(Files.isRegularFile(target.getParent().resolve("raw/manifest.json")));
    }

    @ParameterizedTest
    @ValueSource(strings = {"source", "row", "state", "schema", "count", "path", "duplicate", "json_key"})
    void rehashedMalformedRawCannotBecomeCompletedClean(String problem) throws Exception {
        Path source = write(temp.resolve("source.xlsx"), false, header() + data(2));
        Path raw = temp.resolve("raw");
        assertEquals(0, invoke("ingest", "--input", source.toString(), "--output", raw.toString()));
        Path rawFile = raw.resolve("raw_transactions.jsonl");
        var rows = Files.readAllLines(rawFile);
        ObjectNode row = (ObjectNode) JSON.readTree(rows.get(1));
        ObjectNode manifest = (ObjectNode) JSON.readTree(raw.resolve("manifest.json").toFile());
        switch (problem) {
            case "source" -> row.put("source_sha256", "f".repeat(64));
            case "row" -> row.put("row_number", 1);
            case "state" -> ((ObjectNode) row.path("cells").get(0)).put("state", "blank");
            case "schema" -> ((ObjectNode) manifest.path("sheets").get(0).path("schema")).put("invoice", "Z");
            case "count" -> ((ObjectNode) manifest.path("sheets").get(0)).put("data_rows", 99);
            case "path" -> ((ObjectNode) manifest.path("artifacts")).put("../other.jsonl", "f".repeat(64));
            default -> { }
        }
        rows.set(1, row.toString());
        if (problem.equals("duplicate")) rows.add(rows.get(1));
        if (problem.equals("json_key")) rows.set(1, rows.get(1).replace("\"schema_version\":1", "\"schema_version\":1,\"schema_version\":1"));
        Files.writeString(rawFile, String.join("\n", rows) + "\n");
        ((ObjectNode) manifest.path("artifacts")).put("raw_transactions.jsonl", Json.hash(rawFile));
        Files.writeString(raw.resolve("manifest.json"), manifest.toString());
        Path config = temp.resolve("config.json");
        Files.writeString(config, CONFIG);
        Path target = temp.resolve("clean");
        assertEquals(1, invoke("clean", "--input", raw.toString(), "--config", config.toString(), "--output", target.toString()));
        assertFalse(Files.exists(target.resolve("manifest.json")));
    }

    @Test void configurationOrderDoesNotChangeArtifactsAndOutputNeverOverwrites() throws Exception {
        Path source = write(temp.resolve("source.xlsx"), false, header() + data(2));
        Path raw = temp.resolve("raw");
        assertEquals(0, invoke("ingest", "--input", source.toString(), "--output", raw.toString()));
        Path config = temp.resolve("config.json");
        Files.writeString(config, "{\"policy_version\":\"clean-v1\",\"service_codes\":[\"😀\",\"\ue000\",\"0007\"]}");
        Path first = temp.resolve("first"), second = temp.resolve("second");
        assertEquals(0, invoke("clean", "--input", raw.toString(), "--config", config.toString(), "--output", first.toString()));
        Files.writeString(config, "{\"service_codes\":[\"0007\",\"\ue000\",\"😀\",\"0007\"],\"policy_version\":\"clean-v1\"}");
        assertEquals(0, invoke("clean", "--input", raw.toString(), "--config", config.toString(), "--output", second.toString()));
        for (String name : new String[]{"clean_events.jsonl", "dispositions.jsonl", "audit.json", "manifest.json"})
            assertArrayEquals(Files.readAllBytes(first.resolve(name)), Files.readAllBytes(second.resolve(name)));
        byte[] before = Files.readAllBytes(first.resolve("manifest.json"));
        assertEquals(1, invoke("clean", "--input", raw.toString(), "--config", config.toString(), "--output", first.toString()));
        assertArrayEquals(before, Files.readAllBytes(first.resolve("manifest.json")));
    }

    @Test void excel1904UsesItsOwnEpoch() throws Exception {
        Path source = write(temp.resolve("source.xlsx"), true, header() + data(2).replace("1.5", "0"));
        Path raw = temp.resolve("raw");
        assertEquals(0, invoke("ingest", "--input", source.toString(), "--output", raw.toString()));
        Path config = temp.resolve("config.json");
        Files.writeString(config, CONFIG);
        Path target = temp.resolve("clean");
        assertEquals(0, invoke("clean", "--input", raw.toString(), "--config", config.toString(), "--output", target.toString()));
        assertEquals("1904-01-01T00:00:00", JSON.readTree(Files.readString(target.resolve("clean_events.jsonl"))).path("event_time").textValue());
    }

    @Test void cleanPreservesExactMoneyAndDuplicateOrigins() throws Exception {
        Path source = write(temp.resolve("source.xlsx"), false, header() + data(2) + data(3));
        Path raw = temp.resolve("raw");
        assertEquals(0, invoke("ingest", "--input", source.toString(), "--output", raw.toString()));
        Path config = temp.resolve("config.json");
        Files.writeString(config, "{\"policy_version\":\"clean-v1\",\"service_codes\":[]}");
        Path clean = temp.resolve("clean");
        assertEquals(0, invoke("clean", "--input", raw.toString(), "--config", config.toString(),
                "--output", clean.toString()));
        var lines = Files.readAllLines(clean.resolve("clean_events.jsonl"));
        assertEquals(2, lines.size());
        JsonNode first = JSON.readTree(lines.get(0)), second = JSON.readTree(lines.get(1));
        assertEquals("0.1000", first.path("unit_price").textValue());
        assertEquals("00042", first.path("customer_id").textValue());
        assertEquals("1900-01-01T12:00:00", first.path("event_time").textValue());
        assertNotEquals(first.path("event_id"), second.path("event_id"));
        assertEquals(2, Files.readAllLines(clean.resolve("dispositions.jsonl")).size());
    }
}
