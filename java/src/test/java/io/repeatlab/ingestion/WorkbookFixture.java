package io.repeatlab.ingestion;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

final class WorkbookFixture {
    private WorkbookFixture() {}

    static String text(String reference, String value) {
        return "<c r=\"" + reference + "\" t=\"inlineStr\"><is><t xml:space=\"preserve\">"
                + value + "</t></is></c>";
    }

    static String number(String reference, String value) {
        return "<c r=\"" + reference + "\"><v>" + value + "</v></c>";
    }

    static String header() {
        String[] names = {"InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate",
                "UnitPrice", "CustomerID", "Country", "Extra"};
        StringBuilder xml = new StringBuilder("<row r=\"1\">");
        for (int i = 0; i < names.length; i++) xml.append(text("" + (char) ('A' + i) + "1", names[i]));
        return xml.append("</row>").toString();
    }

    static String data(int row) {
        return "<row r=\"" + row + "\">" + text("A" + row, "X1") + text("B" + row, "0007")
                + text("C" + row, " product\u00a0 ") + number("D" + row, "2")
                + number("E" + row, "1.5") + number("F" + row, "0.1000")
                + text("G" + row, "00042") + text("H" + row, "Testland") + "</row>";
    }

    static Path write(Path path, boolean date1904, String... sheets) throws IOException {
        try (ZipOutputStream zip = new ZipOutputStream(Files.newOutputStream(path))) {
            StringBuilder types = new StringBuilder("<Types xmlns=\"http://schemas.openxmlformats.org/package/2006/content-types\"><Default Extension=\"rels\" ContentType=\"application/vnd.openxmlformats-package.relationships+xml\"/><Default Extension=\"xml\" ContentType=\"application/xml\"/><Override PartName=\"/xl/workbook.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml\"/>");
            StringBuilder book = new StringBuilder("<workbook xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\" xmlns:r=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships\"><workbookPr date1904=\"" + date1904 + "\"/><sheets>");
            StringBuilder rels = new StringBuilder("<Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\">");
            for (int i = 1; i <= sheets.length; i++) {
                types.append("<Override PartName=\"/xl/worksheets/sheet").append(i).append(".xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml\"/>");
                book.append("<sheet name=\"Sheet").append(i).append("\" sheetId=\"").append(i).append("\" state=\"").append(i == 2 ? "hidden" : "visible").append("\" r:id=\"rId").append(i).append("\"/>");
                rels.append("<Relationship Id=\"rId").append(i).append("\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet\" Target=\"worksheets/sheet").append(i).append(".xml\"/>");
                entry(zip, "xl/worksheets/sheet" + i + ".xml", "<worksheet xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\"><sheetData>" + sheets[i - 1] + "</sheetData></worksheet>");
            }
            types.append("<Override PartName=\"/xl/sharedStrings.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml\"/>");
            rels.append("<Relationship Id=\"strings\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings\" Target=\"sharedStrings.xml\"/>");
            entry(zip, "xl/sharedStrings.xml", "<sst xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\" count=\"2\" uniqueCount=\"2\"><si><r><t xml:space=\"preserve\"> 000</t></r><r><t xml:space=\"preserve\">42\u00a0 </t></r></si><si><t>BASE</t><rPh sb=\"0\" eb=\"4\"><t>READING</t></rPh></si></sst>");
            entry(zip, "[Content_Types].xml", types.append("</Types>").toString());
            entry(zip, "_rels/.rels", "<Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\"><Relationship Id=\"rId1\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument\" Target=\"xl/workbook.xml\"/></Relationships>");
            entry(zip, "xl/workbook.xml", book.append("</sheets></workbook>").toString());
            entry(zip, "xl/_rels/workbook.xml.rels", rels.append("</Relationships>").toString());
        }
        return path;
    }

    private static void entry(ZipOutputStream zip, String name, String xml) throws IOException {
        ZipEntry entry = new ZipEntry(name);
        entry.setTime(0);
        zip.putNextEntry(entry);
        zip.write(xml.getBytes(StandardCharsets.UTF_8));
        zip.closeEntry();
    }
}
