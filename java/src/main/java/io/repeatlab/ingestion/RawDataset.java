package io.repeatlab.ingestion;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

final class RawDataset {
    private final Path snapshot;
    private final JsonNode manifest;
    private final String manifestHash;
    final String sourceHash;
    final String dateSystem;
    private final List<SheetState> sheets = new ArrayList<>();
    long dataRows, headerRows, emptyRows;

    private static final class SheetState {
        final JsonNode metadata;
        final int index;
        final String name;
        int lastRow, width;
        long physical, data, empty, absent, headers;
        Map<String, Integer> mapping;

        SheetState(JsonNode metadata, int index) throws IOException {
            this.metadata = metadata;
            this.index = index;
            StrictJson.keys(metadata, "index", "name", "state", "physical_rows", "data_rows",
                    "empty_rows", "absent_rows", "header_rows", "schema");
            StrictJson.integer(metadata.get("index"), index, index);
            name = StrictJson.text(metadata.get("name"));
            StrictJson.require(!name.isEmpty(), "Empty sheet name");
            StrictJson.require(Set.of("visible", "hidden", "veryHidden").contains(StrictJson.text(metadata.get("state"))), "Invalid sheet visibility");
            for (String field : List.of("physical_rows", "data_rows", "empty_rows", "absent_rows", "header_rows"))
                StrictJson.integer(metadata.get(field), 0, 1048576);
            StrictJson.require(metadata.get("schema").isObject(), "Invalid sheet schema");
        }

        Map<String, Object> derived() {
            Map<String, String> schema = new LinkedHashMap<>();
            if (mapping != null) mapping.forEach((name, column) -> schema.put(name, Cell.columnName(column)));
            return Json.object("index", index, "name", name, "state", metadata.get("state").textValue(),
                    "physical_rows", physical, "data_rows", data, "empty_rows", empty,
                    "absent_rows", absent, "header_rows", headers, "schema", schema);
        }
    }

    record Row(int sheetIndex, String sheetName, int rowNumber, List<Cell> cells, Map<String, Integer> mapping) {
        Cell field(String name) { return cells.get(mapping.get(name)); }
        Map<String, Object> provenance(String source) {
            return Json.object("source_sha256", source, "sheet_index", sheetIndex,
                    "sheet_name", sheetName, "row_number", rowNumber);
        }
        String eventId(String source) { return Json.hashText(source + ":" + sheetIndex + ":" + rowNumber); }
    }

    @FunctionalInterface interface RowConsumer { void accept(Row row) throws IOException; }

    RawDataset(Path input, Path stage) throws IOException {
        snapshot = Files.createDirectory(stage.resolve("input"));
        Files.copy(input.resolve("manifest.json"), snapshot.resolve("manifest.json"));
        manifestHash = Json.hash(snapshot.resolve("manifest.json"));
        manifest = StrictJson.read(snapshot.resolve("manifest.json"));
        StrictJson.keys(manifest, "schema_version", "tool_version", "profile", "status", "data_status",
                "date_system", "source", "parameters", "parameters_sha256", "sheets", "artifacts");
        StrictJson.version(manifest);
        StrictJson.require(Main.VERSION.equals(StrictJson.text(manifest.get("tool_version"))), "Unsupported ingest tool version");
        StrictJson.require(Profile.VERSION.equals(StrictJson.text(manifest.get("profile"))), "Unsupported ingest profile");
        StrictJson.require("completed".equals(StrictJson.text(manifest.get("status"))), "Input is not completed");
        StrictJson.require(Set.of("ok", "warnings", "errors").contains(StrictJson.text(manifest.get("data_status"))), "Invalid data status");
        dateSystem = StrictJson.text(manifest.get("date_system"));
        StrictJson.require(Set.of("1900", "1904").contains(dateSystem), "Invalid date system");
        StrictJson.keys(manifest.get("source"), "basename", "sha256");
        String basename = StrictJson.text(manifest.get("source").get("basename"));
        StrictJson.require(!basename.isEmpty() && !basename.contains("/") && !basename.contains("\\"), "Invalid source basename");
        sourceHash = StrictJson.hash(manifest.get("source").get("sha256"));
        Object parameters = Json.object("command", "ingest", "profile", Profile.VERSION);
        StrictJson.require(StrictJson.parse(Json.encode(parameters)).equals(manifest.get("parameters")), "Unsupported ingest parameters");
        StrictJson.require(Json.hashText(Json.encode(parameters)).equals(StrictJson.hash(manifest.get("parameters_sha256"))), "Parameter checksum mismatch");
        StrictJson.keys(manifest.get("artifacts"), "raw_transactions.jsonl", "issues.jsonl", "audit.json");
        for (String name : TransactionalOutput.ARTIFACTS) {
            String expected = StrictJson.hash(manifest.get("artifacts").get(name));
            Files.copy(input.resolve(name), snapshot.resolve(name));
            StrictJson.require(expected.equals(Json.hash(snapshot.resolve(name))), "Input checksum mismatch: " + name);
        }
        StrictJson.require(manifest.get("sheets").isArray() && !manifest.get("sheets").isEmpty(), "Missing sheet metadata");
        Set<String> names = new HashSet<>();
        for (JsonNode sheet : manifest.get("sheets")) {
            SheetState state = new SheetState(sheet, sheets.size() + 1);
            StrictJson.require(names.add(state.name), "Duplicate sheet name");
            sheets.add(state);
        }
        validate(stage);
    }

    private void validate(Path stage) throws IOException {
        Path expectedIssues = stage.resolve("verified_issues.jsonl");
        QualityAudit audit;
        try (var writer = Files.newBufferedWriter(expectedIssues);
             var rows = Files.newBufferedReader(snapshot.resolve("raw_transactions.jsonl"))) {
            audit = new QualityAudit(writer, sourceHash);
            int previousSheet = 1;
            for (String line; (line = rows.readLine()) != null;) {
                JsonNode node = StrictJson.parse(line);
                StrictJson.keys(node, "schema_version", "source_sha256", "sheet_index", "sheet_name", "row_number", "kind", "cells");
                StrictJson.version(node);
                StrictJson.require(sourceHash.equals(StrictJson.hash(node.get("source_sha256"))), "Wrong source provenance");
                int index = (int) StrictJson.integer(node.get("sheet_index"), previousSheet, sheets.size());
                previousSheet = index;
                SheetState sheet = sheets.get(index - 1);
                StrictJson.require(sheet.name.equals(StrictJson.text(node.get("sheet_name"))), "Wrong sheet provenance");
                int number = (int) StrictJson.integer(node.get("row_number"), sheet.lastRow + 1L, 1048576);
                List<Cell> cells = cells(node.get("cells"), number);
                StrictJson.require(cells.size() >= sheet.width, "Row shorter than header");
                boolean empty = cells.stream().allMatch(Cell::empty);
                String kind = empty ? "empty" : sheet.mapping == null ? "header" : "data";
                StrictJson.require(kind.equals(StrictJson.text(node.get("kind"))), "Invalid raw row kind");
                sheet.absent += number - sheet.lastRow - 1;
                sheet.lastRow = number;
                sheet.physical++;
                if (empty) { sheet.empty++; emptyRows++; }
                else if (sheet.mapping == null) {
                    sheet.mapping = Profile.header(cells);
                    sheet.width = cells.size();
                    sheet.headers++;
                    headerRows++;
                } else {
                    sheet.data++;
                    dataRows++;
                    audit.row(index, sheet.name, number, cells, sheet.mapping, dateSystem.equals("1904"));
                }
            }
        }
        List<Map<String, Object>> derived = sheets.stream().map(SheetState::derived).toList();
        StrictJson.require(StrictJson.parse(Json.encode(derived)).equals(manifest.get("sheets")), "Sheet metadata does not match raw");
        StrictJson.require(StrictJson.parse(Json.encode(audit.summary(derived))).equals(StrictJson.read(snapshot.resolve("audit.json"))), "Audit does not match raw");
        StrictJson.require(audit.dataStatus().equals(manifest.get("data_status").textValue()), "Manifest quality does not match raw");
        try (var expected = Files.newBufferedReader(expectedIssues);
             var actual = Files.newBufferedReader(snapshot.resolve("issues.jsonl"))) {
            while (true) {
                String left = expected.readLine(), right = actual.readLine();
                if (left == null || right == null) {
                    StrictJson.require(left == null && right == null, "Issue count mismatch");
                    break;
                }
                StrictJson.require(StrictJson.parse(left).equals(StrictJson.parse(right)), "Issues do not match raw");
            }
        }
        Files.delete(expectedIssues);
    }

    void rows(RowConsumer consumer) throws IOException {
        try (var input = Files.newBufferedReader(snapshot.resolve("raw_transactions.jsonl"))) {
            for (String line; (line = input.readLine()) != null;) {
                JsonNode node = StrictJson.parse(line);
                if (!node.get("kind").textValue().equals("data")) continue;
                SheetState sheet = sheets.get(node.get("sheet_index").intValue() - 1);
                int number = node.get("row_number").intValue();
                consumer.accept(new Row(sheet.index, sheet.name, number, cells(node.get("cells"), number), sheet.mapping));
            }
        }
    }

    Map<String, Object> provenance() {
        return Json.object("manifest_sha256", manifestHash, "source_sha256", sourceHash,
                "date_system", dateSystem, "artifacts", manifest.get("artifacts"));
    }

    void releaseSnapshot() throws IOException {
        for (String name : List.of("raw_transactions.jsonl", "issues.jsonl", "audit.json", "manifest.json"))
            Files.delete(snapshot.resolve(name));
        Files.delete(snapshot);
    }

    private static List<Cell> cells(JsonNode nodes, int row) throws IOException {
        StrictJson.require(nodes != null && nodes.isArray() && nodes.size() <= 16384, "Invalid cells");
        List<Cell> result = new ArrayList<>(nodes.size());
        for (JsonNode node : nodes) {
            StrictJson.keys(node, "column", "reference", "source_type", "state", "value", "raw_value", "style", "formula", "cached_value");
            String column = Cell.columnName(result.size());
            StrictJson.require(column.equals(StrictJson.text(node.get("column")))
                    && (column + row).equals(StrictJson.text(node.get("reference"))), "Invalid cell coordinate");
            String type = StrictJson.nullableText(node.get("source_type"));
            String state = StrictJson.text(node.get("state"));
            String value = StrictJson.nullableText(node.get("value"));
            String raw = StrictJson.nullableText(node.get("raw_value"));
            String style = StrictJson.nullableText(node.get("style"));
            String cached = StrictJson.nullableText(node.get("cached_value"));
            Map<String, String> formula = null;
            if (!node.get("formula").isNull()) {
                StrictJson.require(node.get("formula").isObject() && node.get("formula").has("text"), "Invalid formula");
                formula = new HashMap<>();
                var fields = node.get("formula").fieldNames();
                while (fields.hasNext()) { String field = fields.next(); formula.put(field, StrictJson.text(node.get("formula").get(field))); }
            }
            if (state.equals("missing")) {
                StrictJson.require(type == null && value == null && raw == null && style == null && formula == null && cached == null, "Invalid missing cell");
            } else {
                StrictJson.require(type != null && Set.of("n", "s", "inlineStr", "str", "b", "e", "d").contains(type), "Invalid cell type");
                String expected = formula != null ? "formula" : value == null ? "blank" : value.isEmpty() ? "empty_string" : "value";
                StrictJson.require(expected.equals(state), "Invalid cell state");
                StrictJson.require(formula == null ? cached == null : value == null, "Invalid formula value");
                String resolved = formula == null ? value : cached;
                if (!type.equals("s") && !type.equals("inlineStr"))
                    StrictJson.require(Objects.equals(raw, resolved), "Raw scalar value mismatch");
                if (type.equals("s")) {
                    StrictJson.require((raw == null) == (resolved == null), "Missing shared string index");
                    if (raw != null) {
                        try { StrictJson.require(Integer.parseInt(raw) >= 0, "Invalid shared string index"); }
                        catch (NumberFormatException failure) { throw new IOException("Invalid shared string index", failure); }
                    }
                }
            }
            result.add(new Cell(column, column + row, type, state, value, raw, style, formula, cached));
        }
        return result;
    }
}
