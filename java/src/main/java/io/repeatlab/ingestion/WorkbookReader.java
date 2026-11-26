package io.repeatlab.ingestion;

import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.openxml4j.opc.PackageAccess;
import org.apache.poi.openxml4j.opc.PackagePart;
import org.apache.poi.openxml4j.opc.PackagePartName;
import org.apache.poi.openxml4j.opc.PackageRelationshipTypes;
import org.apache.poi.openxml4j.opc.TargetMode;
import org.apache.poi.xssf.eventusermodel.ReadOnlySharedStringsTable;
import org.apache.poi.xssf.model.SharedStrings;
import org.apache.poi.xssf.usermodel.XSSFRichTextString;
import org.apache.poi.xssf.usermodel.XSSFRelation;

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
            var workbooks = pkg.getRelationshipsByType(PackageRelationshipTypes.CORE_DOCUMENT);
            if (workbooks.size() != 1 || workbooks.getRelationship(0).getTargetMode() != TargetMode.INTERNAL)
                throw new IOException("Expected one internal workbook relationship");
            var workbook = pkg.getPart(workbooks.getRelationship(0));
            if (workbook == null || !XSSFRelation.WORKBOOK.getContentType().equals(workbook.getContentType()))
                throw new IOException("Expected an XLSX workbook part");
            SharedStrings strings = sharedStrings(workbook);
            List<Sheet> descriptors;
            try (InputStream input = workbook.getInputStream()) { descriptors = workbook(input); }
            if (descriptors.isEmpty()) throw new IOException("Workbook has no sheets");
            var parts = new HashSet<PackagePartName>();
            for (int index = 0; index < descriptors.size(); index++) {
                Sheet sheet = descriptors.get(index);
                var rel = workbook.getRelationship(sheet.relationship());
                if (rel == null || rel.getTargetMode() != TargetMode.INTERNAL
                        || !XSSFRelation.WORKSHEET.getRelation().equals(rel.getRelationshipType()))
                    throw new WorkbookFailure(index + 1, sheet.name(), 0, new IOException("Invalid worksheet relationship"));
                var part = workbook.getRelatedPart(rel);
                if (!XSSFRelation.WORKSHEET.getContentType().equals(part.getContentType()) || !parts.add(part.getPartName()))
                    throw new WorkbookFailure(index + 1, sheet.name(), 0, new IOException("Invalid or repeated worksheet part"));
                try (InputStream input = part.getInputStream()) {
                    readSheet(input, strings, index + 1, sheet);
                }
            }
        }
    }

    private static SharedStrings sharedStrings(PackagePart workbook) throws Exception {
        var tables = workbook.getRelationshipsByType(XSSFRelation.SHARED_STRINGS.getRelation());
        if (tables.size() == 0) return null;
        if (tables.size() != 1 || tables.getRelationship(0).getTargetMode() != TargetMode.INTERNAL)
            throw new IOException("Expected one internal shared strings table");
        var part = workbook.getRelatedPart(tables.getRelationship(0));
        if (!XSSFRelation.SHARED_STRINGS.getContentType().equals(part.getContentType()))
            throw new IOException("Invalid shared strings part");
        return new ReadOnlySharedStringsTable(part, false);
    }

    private record Sheet(String name, String state, String relationship) {}

    private List<Sheet> workbook(InputStream input) throws Exception {
        XMLStreamReader xml = xml(input);
        List<Sheet> result = new ArrayList<>();
        var names = new HashSet<String>();
        var relationships = new HashSet<String>();
        boolean inSheets = false, seenSheets = false, seenProperties = false;
        try {
            root(xml, "workbook");
            while (xml.hasNext()) {
                int event = xml.next();
                rejectDtd(event);
                if (event == XMLStreamConstants.END_ELEMENT && element(xml, "sheets")) inSheets = false;
                if (event != XMLStreamConstants.START_ELEMENT) continue;
                if (!inSheets && element(xml, "workbookPr")) {
                    if (seenProperties) throw new IOException("Duplicate workbook properties");
                    seenProperties = true;
                    String flag = attr(xml, "date1904", "false");
                    if (!List.of("false", "true", "0", "1").contains(flag)) throw new IOException("Invalid date1904 flag");
                    date1904 = flag.equals("true") || flag.equals("1");
                    skip(xml);
                } else if (!inSheets && element(xml, "sheets")) {
                    if (seenSheets) throw new IOException("Duplicate workbook sheets");
                    inSheets = seenSheets = true;
                } else if (inSheets && element(xml, "sheet")) {
                    String name = attr(xml, "name", null);
                    String state = attr(xml, "state", "visible");
                    String rel = xml.getAttributeValue(REL_NS, "id");
                    if (name == null || name.isEmpty() || rel == null || rel.isEmpty())
                        throw new IOException("Sheet metadata is incomplete");
                    if (!List.of("visible", "hidden", "veryHidden").contains(state))
                        throw new IOException("Invalid sheet visibility");
                    if (!names.add(name) || !relationships.add(rel))
                        throw new IOException("Duplicate sheet metadata");
                    result.add(new Sheet(name, state, rel));
                    skip(xml);
                } else skip(xml);
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
        boolean seenData = false, inData = false;
        try {
            root(xml, "worksheet");
            while (xml.hasNext()) {
                int event = xml.next();
                rejectDtd(event);
                if (event == XMLStreamConstants.START_ELEMENT) {
                    if (!inData && element(xml, "sheetData")) {
                        if (seenData) throw new IOException("Duplicate worksheet sheetData");
                        seenData = inData = true;
                    } else if (!inData) {
                        skip(xml);
                    } else if (cells == null && element(xml, "row")) {
                        row = Integer.parseInt(attr(xml, "r", Integer.toString(previousRow + 1)));
                        if (row <= previousRow || row > 1048576) throw new IOException("Invalid row order or coordinate");
                        absent += row - previousRow - 1;
                        previousRow = row;
                        cells = new ArrayList<>();
                    } else if (cells != null && element(xml, "c")) {
                        String reference = attr(xml, "r", Cell.columnName(cells.size()) + row);
                        int column = columnIndex(reference, row);
                        if (column < cells.size()) throw new IOException("Duplicate or out-of-order cell " + reference);
                        while (cells.size() < column) cells.add(Cell.missing(cells.size(), row));
                        cells.add(readCell(xml, strings, reference, column));
                    } else if (cells != null && element(xml, "extLst")) skip(xml);
                    else throw new IOException("Unexpected worksheet grid element: " + xml.getName());
                } else if (event == XMLStreamConstants.END_ELEMENT) {
                    if (element(xml, "sheetData")) inData = false;
                    if (element(xml, "row") && inData) {
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
            if (!seenData) throw new IOException("Missing worksheet sheetData");
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
        while (xml.hasNext()) {
            int event = xml.next();
            rejectDtd(event);
            if (event == XMLStreamConstants.START_ELEMENT) {
                if (element(xml, "is")) {
                    if (hasInline || !type.equals("inlineStr"))
                        throw new IOException("Unexpected inline content at " + reference);
                    hasInline = true;
                    inlineText(xml, text);
                } else if (element(xml, "v")) {
                    if (rawValue != null) throw new IOException("Duplicate cell value at " + reference);
                    rawValue = xml.getElementText();
                } else if (element(xml, "f")) {
                    if (formula != null) throw new IOException("Duplicate formula at " + reference);
                    formula = new LinkedHashMap<>();
                    for (int i = 0; i < xml.getAttributeCount(); i++)
                        formula.put(xml.getAttributeLocalName(i), xml.getAttributeValue(i));
                    formula.put("text", xml.getElementText());
                } else skip(xml);
            } else if (event == XMLStreamConstants.END_ELEMENT) {
                if (element(xml, "c")) break;
            }
        }
        String value = rawValue;
        if (type.equals("inlineStr")) {
            if (rawValue != null) throw new IOException("Unexpected v for inline string at " + reference);
            String inline = text.toString();
            value = hasInline ? XSSFRichTextString.utfDecode(inline) : null;
            if (hasInline && !inline.equals(value)) rawValue = inline;
        }
        if (type.equals("str") && rawValue != null) value = XSSFRichTextString.utfDecode(rawValue);
        if (type.equals("s") && rawValue != null) {
            if (strings == null) throw new IOException("Missing shared strings table");
            value = strings.getItemAt(Integer.parseInt(rawValue)).getString();
        }
        String state = formula != null ? "formula" : value == null ? "blank" : value.isEmpty() ? "empty_string" : "value";
        return new Cell(Cell.columnName(column), reference, type, state, formula == null ? value : null,
                rawValue, style, formula, formula == null ? null : value);
    }

    private static void inlineText(XMLStreamReader xml, StringBuilder text) throws Exception {
        boolean run = false;
        while (xml.hasNext()) {
            int event = xml.next();
            rejectDtd(event);
            if (event == XMLStreamConstants.START_ELEMENT) {
                if (element(xml, "t")) text.append(xml.getElementText());
                else if (!run && element(xml, "r")) run = true;
                else skip(xml);
            } else if (event == XMLStreamConstants.END_ELEMENT) {
                if (element(xml, "is")) return;
                if (element(xml, "r")) run = false;
            }
        }
    }

    private static boolean element(XMLStreamReader xml, String name) {
        return name.equals(xml.getLocalName()) && NS.equals(xml.getNamespaceURI());
    }

    private static void root(XMLStreamReader xml, String name) throws Exception {
        while (xml.hasNext()) {
            int event = xml.next();
            rejectDtd(event);
            if (event == XMLStreamConstants.START_ELEMENT) {
                if (element(xml, name)) return;
                break;
            }
        }
        throw new IOException("Expected transitional OOXML " + name);
    }

    private static void skip(XMLStreamReader xml) throws Exception {
        int depth = 1;
        while (depth != 0) {
            int event = xml.next();
            rejectDtd(event);
            if (event == XMLStreamConstants.START_ELEMENT) depth++;
            if (event == XMLStreamConstants.END_ELEMENT) depth--;
        }
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
        String value = xml.getAttributeValue("", name);
        return value == null ? fallback : value;
    }
}
