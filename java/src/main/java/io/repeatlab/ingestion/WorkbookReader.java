package io.repeatlab.ingestion;

import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.openxml4j.opc.PackageAccess;
import org.apache.poi.xssf.eventusermodel.XSSFReader;
import org.apache.poi.xssf.model.SharedStrings;

final class WorkbookReader {
    private static final String NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main";
    private static final String REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships";
    private final Writer raw;
    private final QualityAudit audit;
    private final String sourceHash;
    private final List<Map<String, Object>> sheets = new ArrayList<>();
    private boolean date1904;

    WorkbookReader(Writer raw, QualityAudit audit, String sourceHash) {
        this.raw = raw;
        this.audit = audit;
        this.sourceHash = sourceHash;
    }

    List<Map<String, Object>> sheets() { return sheets; }
    String dateSystem() { return date1904 ? "1904" : "1900"; }

    void read(Path source) throws Exception {
        try (OPCPackage pkg = OPCPackage.open(source.toFile(), PackageAccess.READ)) {
            XSSFReader reader = new XSSFReader(pkg);
            reader.setUseReadOnlySharedStringsTable(true);
            SharedStrings strings = reader.getSharedStringsTable();
            List<Sheet> descriptors;
            try (InputStream input = reader.getWorkbookData()) { descriptors = workbook(input); }
            if (descriptors.isEmpty()) throw new IOException("Workbook has no sheets");
            for (int index = 0; index < descriptors.size(); index++) {
                Sheet sheet = descriptors.get(index);
                try (InputStream input = reader.getSheet(sheet.relationship())) {
                    readSheet(input, strings, index + 1, sheet);
                }
            }
        }
    }

    private record Sheet(String name, String state, String relationship) {}

    private List<Sheet> workbook(InputStream input) throws Exception {
        XMLStreamReader xml = xml(input);
        List<Sheet> result = new ArrayList<>();
        try {
            while (xml.hasNext()) {
                int event = xml.next();
                rejectDtd(event);
                if (event != XMLStreamConstants.START_ELEMENT) continue;
                if (xml.getLocalName().equals("workbook") && !NS.equals(xml.getNamespaceURI()))
                    throw new IOException("Only transitional OOXML XLSX is supported");
                if (xml.getLocalName().equals("workbookPr")) {
                    String flag = attr(xml, "date1904", "false");
                    if (!List.of("false", "true", "0", "1").contains(flag)) throw new IOException("Invalid date1904 flag");
                    date1904 = flag.equals("true") || flag.equals("1");
                }
                if (xml.getLocalName().equals("sheet")) {
                    String name = attr(xml, "name", null);
                    String rel = xml.getAttributeValue(REL_NS, "id");
                    if (name == null || rel == null) throw new IOException("Sheet metadata is incomplete");
                    if (result.stream().anyMatch(s -> s.name().equals(name) || s.relationship().equals(rel)))
                        throw new IOException("Duplicate sheet metadata");
                    result.add(new Sheet(name, attr(xml, "state", "visible"), rel));
                }
            }
        } finally { xml.close(); }
        return result;
    }

    private void readSheet(InputStream input, SharedStrings strings, int index, Sheet sheet) throws Exception {
        XMLStreamReader xml = xml(input);
        Map<String, Integer> mapping = null;
        List<Cell> cells = null;
        int row = 0, previousRow = 0, width = 0;
        long physical = 0, data = 0, empty = 0, absent = 0;
        boolean worksheet = false, inData = false;
        try {
            while (xml.hasNext()) {
                int event = xml.next();
                rejectDtd(event);
                if (event == XMLStreamConstants.START_ELEMENT) {
                    String name = xml.getLocalName();
                    if (name.equals("worksheet")) worksheet = NS.equals(xml.getNamespaceURI());
                    if (name.equals("sheetData")) inData = true;
                    if (name.equals("row") && inData) {
                        if (cells != null) throw new IOException("Nested sheet row");
                        row = Integer.parseInt(attr(xml, "r", Integer.toString(previousRow + 1)));
                        if (row <= previousRow || row > 1048576) throw new IOException("Invalid row order or coordinate");
                        absent += row - previousRow - 1;
                        previousRow = row;
                        cells = new ArrayList<>();
                    } else if (name.equals("c") && inData) {
                        if (cells == null) throw new IOException("Cell outside a row");
                        String reference = attr(xml, "r", Cell.columnName(cells.size()) + row);
                        int column = columnIndex(reference, row);
                        if (column < cells.size()) throw new IOException("Duplicate or out-of-order cell " + reference);
                        while (cells.size() < column) cells.add(Cell.missing(cells.size(), row));
                        cells.add(readCell(xml, strings, reference, column));
                    }
                } else if (event == XMLStreamConstants.END_ELEMENT) {
                    if (xml.getLocalName().equals("sheetData")) inData = false;
                    if (xml.getLocalName().equals("row") && inData) {
                        if (cells == null) throw new IOException("Unexpected row end");
                        physical++;
                        boolean isEmpty = cells.stream().allMatch(Cell::empty);
                        String kind;
                        if (isEmpty) { kind = "empty"; empty++; }
                        else if (mapping == null) {
                            kind = "header";
                            mapping = Profile.header(cells);
                            width = cells.size();
                        } else { kind = "data"; data++; }
                        while (cells.size() < width) cells.add(Cell.missing(cells.size(), row));
                        Json.line(raw, Json.object("schema_version", 1, "source_sha256", sourceHash,
                                "sheet_index", index, "sheet_name", sheet.name(), "row_number", row,
                                "kind", kind, "cells", cells));
                        if (kind.equals("data")) audit.row(index, sheet.name(), row, cells, mapping, date1904);
                        cells = null;
                    }
                }
            }
            if (!worksheet) throw new IOException("Unsupported sheet type: " + sheet.name());
        } catch (Exception failure) {
            throw new WorkbookFailure(index, sheet.name(), row, failure);
        } finally { xml.close(); }
        Map<String, String> schema = new LinkedHashMap<>();
        if (mapping != null) mapping.forEach((field, column) -> schema.put(field, Cell.columnName(column)));
        sheets.add(Json.object("index", index, "name", sheet.name(), "state", sheet.state(),
                "physical_rows", physical, "data_rows", data, "empty_rows", empty,
                "absent_rows", absent, "header_rows", mapping == null ? 0 : 1, "schema", schema));
    }

    private Cell readCell(XMLStreamReader xml, SharedStrings strings, String reference, int column) throws Exception {
        String type = attr(xml, "t", "n"), style = attr(xml, "s", null);
        if (!List.of("n", "s", "inlineStr", "str", "b", "e", "d").contains(type))
            throw new IOException("Unsupported cell type at " + reference);
        String rawValue = null;
        StringBuilder text = new StringBuilder();
        boolean hasInline = false;
        Map<String, String> formula = null;
        int phoneticDepth = 0;
        while (xml.hasNext()) {
            int event = xml.next();
            rejectDtd(event);
            if (event == XMLStreamConstants.START_ELEMENT) {
                switch (xml.getLocalName()) {
                    case "rPh" -> phoneticDepth++;
                    case "is" -> hasInline = true;
                    case "t" -> { if (phoneticDepth == 0) text.append(xml.getElementText()); }
                    case "v" -> rawValue = xml.getElementText();
                    case "f" -> {
                        formula = new LinkedHashMap<>();
                        for (int i = 0; i < xml.getAttributeCount(); i++)
                            formula.put(xml.getAttributeLocalName(i), xml.getAttributeValue(i));
                        formula.put("text", xml.getElementText());
                    }
                    default -> { }
                }
            } else if (event == XMLStreamConstants.END_ELEMENT) {
                if (xml.getLocalName().equals("rPh")) phoneticDepth--;
                if (xml.getLocalName().equals("c")) break;
            }
        }
        String value = rawValue;
        if (type.equals("inlineStr")) value = hasInline ? text.toString() : null;
        if (type.equals("s") && rawValue != null) {
            if (strings == null) throw new IOException("Missing shared strings table");
            value = strings.getItemAt(Integer.parseInt(rawValue)).getString();
        }
        String state = formula != null ? "formula" : value == null ? "blank" : value.isEmpty() ? "empty_string" : "value";
        return new Cell(Cell.columnName(column), reference, type, state, formula == null ? value : null,
                rawValue, style, formula, formula == null ? null : value);
    }

    private static int columnIndex(String reference, int row) throws IOException {
        int i = 0, result = 0;
        while (i < reference.length() && reference.charAt(i) >= 'A' && reference.charAt(i) <= 'Z') {
            result = result * 26 + reference.charAt(i++) - 'A' + 1;
            if (result > 16384) throw new IOException("Column outside XLSX bounds");
        }
        if (i == 0 || !reference.substring(i).equals(Integer.toString(row)))
            throw new IOException("Invalid cell coordinate " + reference);
        return result - 1;
    }

    private static XMLStreamReader xml(InputStream input) throws XMLStreamException {
        XMLInputFactory factory = XMLInputFactory.newFactory();
        factory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
        factory.setProperty("javax.xml.stream.isSupportingExternalEntities", false);
        factory.setXMLResolver((publicId, systemId, base, namespace) -> { throw new XMLStreamException("External entities disabled"); });
        return factory.createXMLStreamReader(input);
    }

    private static void rejectDtd(int event) throws IOException {
        if (event == XMLStreamConstants.DTD || event == XMLStreamConstants.ENTITY_REFERENCE)
            throw new IOException("DTD and entity references are unsupported");
    }

    private static String attr(XMLStreamReader xml, String name, String fallback) {
        String value = xml.getAttributeValue(null, name);
        return value == null ? fallback : value;
    }
}
