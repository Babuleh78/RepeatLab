"""Read the executable Java CLI's real output with Python's standard library."""

import hashlib
import json
import os
import shutil
import subprocess
from decimal import Decimal
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

import pytest

ROOT = Path(__file__).resolve().parents[2]
JAR = ROOT / "java" / "target" / "repeatlab-data.jar"


def workbook(path: Path, *, bad_price: bool = False) -> Path:
    headers = [
        "Invoice",
        "StockCode",
        "Description",
        "Quantity",
        "InvoiceDate",
        "Price",
        "Customer ID",
        "Country",
    ]
    head = "".join(
        f'<c r="{chr(65 + col)}1" t="inlineStr"><is><t>{value}</t></is></c>'
        for col, value in enumerate(headers)
    )
    price = "invalid" if bad_price else "12345678901234567890.1000"
    data = (
        '<c r="A2" t="inlineStr"><is><t>X0001</t></is></c>'
        '<c r="B2" t="inlineStr"><is><t>0007</t></is></c>'
        '<c r="C2" t="inlineStr"><is><t xml:space="preserve"> item\u00a0 </t></is></c>'
        '<c r="D2"><v>2</v></c><c r="E2"><v>1.5</v></c>'
        f'<c r="F2"><v>{price}</v></c>'
        '<c r="G2" t="inlineStr"><is><t>00042</t></is></c>'
        '<c r="H2" t="inlineStr"><is><t>Testland</t></is></c>'
    )
    with ZipFile(path, "w", ZIP_DEFLATED) as archive:
        archive.writestr(
            "[Content_Types].xml",
            '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
            '<Default Extension="rels" '
            'ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
            '<Default Extension="xml" ContentType="application/xml"/>'
            '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-'
            'officedocument.spreadsheetml.sheet.main+xml"/>'
            '<Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.'
            'openxmlformats-officedocument.spreadsheetml.worksheet+xml"/></Types>',
        )
        archive.writestr(
            "_rels/.rels",
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
            '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/'
            'relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>',
        )
        archive.writestr(
            "xl/workbook.xml",
            '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
            'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
            '<sheets><sheet name="Synthetic" sheetId="1" r:id="rId1"/></sheets></workbook>',
        )
        archive.writestr(
            "xl/_rels/workbook.xml.rels",
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
            '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/'
            'relationships/worksheet" Target="worksheets/sheet1.xml"/></Relationships>',
        )
        archive.writestr(
            "xl/worksheets/sheet1.xml",
            '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
            f'<sheetData><row r="1">{head}</row><row r="2">{data}</row></sheetData></worksheet>',
        )
    return path


def java_command() -> str:
    java_home = os.environ.get("JAVA_HOME")
    executable = "java.exe" if os.name == "nt" else "java"
    java = str(Path(java_home) / "bin" / executable) if java_home else shutil.which("java")
    assert java, "Java 25 required: set JAVA_HOME before running pytest"
    assert JAR.is_file(), "Build the executable first: ./mvnw -B -f java/pom.xml verify"
    return java


def test_executable_includes_dependency_licenses_and_notices() -> None:
    java_command()
    with ZipFile(JAR) as archive:
        assert "META-INF/LICENSE" in archive.namelist()
        assert b"Apache License" in archive.read("META-INF/LICENSE")
        assert b"Apache POI" in archive.read("META-INF/NOTICE")
        assert "META-INF/FastDoubleParser-ThirdParty-LICENSE" in archive.namelist()


@pytest.mark.parametrize("bad_price, expected_exit", [(False, 0), (True, 2)])
def test_java_raw_is_exact_and_manifest_matches_bytes(
    tmp_path: Path, bad_price: bool, expected_exit: int
) -> None:
    source = workbook(tmp_path / "synthetic.xlsx", bad_price=bad_price)
    before = source.read_bytes()
    target = tmp_path / "output"
    result = subprocess.run(
        [
            java_command(),
            "-jar",
            str(JAR),
            "ingest",
            "--input",
            str(source),
            "--output",
            str(target),
        ],
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=60,
        check=False,
    )
    assert result.returncode == expected_exit, result.stderr
    raw = [
        json.loads(line)
        for line in (target / "raw_transactions.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    assert len(raw) == 2
    assert raw[1]["cells"][6]["value"] == "00042"
    assert raw[1]["cells"][2]["value"] == " item\u00a0 "
    assert source.read_bytes() == before
    manifest = json.loads((target / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["schema_version"] == 1
    assert manifest["source"]["sha256"] == hashlib.sha256(before).hexdigest()
    for name, expected_hash in manifest["artifacts"].items():
        payload = (target / name).read_bytes()
        assert hashlib.sha256(payload).hexdigest() == expected_hash
        assert not payload.startswith(b"\xef\xbb\xbf")
        assert b"\r\n" not in payload
    issues = [
        json.loads(line)
        for line in (target / "issues.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    if bad_price:
        issue = next(item for item in issues if item["code"] == "INVALID_PRICE")
        assert (issue["severity"], issue["sheet_name"], issue["row_number"], issue["column"]) == (
            "error",
            "Synthetic",
            2,
            "F",
        )
        assert manifest["data_status"] == "errors"
    else:
        value = raw[1]["cells"][5]["value"]
        assert isinstance(value, str)
        assert value == "12345678901234567890.1000"
        assert Decimal(value).as_tuple() == Decimal("12345678901234567890.1000").as_tuple()
        assert manifest["data_status"] == "ok"
