"""Verify per-order product repeats through the packaged Java CLI."""

import hashlib
import json
import shutil
import subprocess
from pathlib import Path
from zipfile import ZipFile

import pytest
from test_java_analytics import records
from test_java_cleaning import run_clean
from test_java_cohorts import document, sha256
from test_java_ingestion import JAR, ROOT, java_command

from repeatlab.orders import write_orders_dataset

FROM = "2020-01-01T00:00:00"
FIRST = FROM + ".000001"
SECOND = FROM + ".000002"
THIRD = FROM + ".000003"
UNTIL = "2020-02-01T00:00:00"
MART = "product_repeats.jsonl"


def run_repeats(
    clean: Path, orders: Path, output: Path, *, start: str = FROM, end: str = UNTIL
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            java_command(),
            "-jar",
            str(JAR),
            "product-repeats",
            "--input",
            str(orders),
            "--clean",
            str(clean),
            "--observed-from",
            start,
            "--observed-until",
            end,
            "--output",
            str(output),
        ],
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=90,
        check=False,
    )


@pytest.fixture(scope="module")
def source_pair(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, Path]:
    root = tmp_path_factory.mktemp("product-inputs")
    clean = run_clean(
        root,
        [
            ("A4", "Q", "1", THIRD, "1", "A"),
            ("A0", "P", "1", FIRST, "1E+39", "A"),
            ("A1", "P", "1", FIRST, "1", "A"),
            ("A2", "P", "5", SECOND, "1", "A"),
            ("A2", "P", "5", SECOND, "1", "A"),
            ("A2", "P", "5", SECOND, "1", "A"),
            ("A2", "Q", "1", SECOND, "1", "A"),
            ("A2", "SERVICE", "1", SECOND, "1", "A"),
            ("A3", "Q", "1", SECOND, "1", "A"),
            ("A3", "R", "1", SECOND, "1", "A"),
            ("B0", "Q", "1", FROM, "1", "B"),
            ("C0", "R", "-1", FROM, "1", "A"),
            ("C1", "P", "-1", FIRST, "1", "A"),
            ("ANON", "R", "1", "2020-01-31T23:59:59.999999", "1", ""),
            ("S0", "SERVICE", "1", FROM, "1", "A"),
            ("BAD", "Q", "1", FROM, "invalid", "A"),
        ],
    )
    orders = root / "orders"
    write_orders_dataset(clean, orders)
    return clean, orders


def test_product_repeats_reproduce_per_order_history(
    tmp_path: Path, source_pair: tuple[Path, Path]
) -> None:
    clean, orders = source_pair
    first, second = tmp_path / "first", tmp_path / "second"
    for output in (first, second):
        result = run_repeats(clean, orders, output)
        assert result.returncode == 2, result.stderr

    rows = records(first / MART)
    assert [
        (
            row["order_key"],
            row["customer_id"],
            row["event_time"],
            row["has_prior_purchase"],
            row["distinct_products"],
            row["repeated_products"],
            row["repeat_share"],
        )
        for row in rows
    ] == [
        ("A0", "A", FIRST, False, 1, 0, None),
        ("A1", "A", FIRST, False, 1, 0, None),
        ("A2", "A", SECOND, True, 2, 1, {"numerator": 1, "denominator": 2}),
        ("A3", "A", SECOND, True, 2, 0, {"numerator": 0, "denominator": 2}),
        ("A4", "A", THIRD, True, 1, 1, {"numerator": 1, "denominator": 1}),
        ("B0", "B", FROM, False, 1, 0, None),
    ]
    manifest = document(first / "manifest.json")
    audit = document(first / "audit.json")
    for entry in (manifest, audit, *rows):
        assert (entry["schema"], entry["schema_version"]) == ("repeatlab-product-repeats", 1)
    assert (manifest["policy_version"], manifest["status"]) == ("product-repeats-v1", "completed")
    assert (
        manifest["observation"]
        == audit["observation"]
        == {
            "observed_from": FROM,
            "observed_until": UNTIL,
        }
    )
    assert manifest["data_status"] == audit["data_status"] == "quarantined"
    assert audit["product_repeats_rows"] == len(rows)
    assert audit["orders_counts"] == document(orders / "manifest.json")["counts"]
    assert audit["source_clean_counts"] == document(clean / "manifest.json")["counts"]
    assert manifest["input"] == {
        "clean_manifest_sha256": sha256(clean / "manifest.json"),
        "orders_manifest_sha256": sha256(orders / "manifest.json"),
    }
    assert manifest["artifacts"] == {name: sha256(first / name) for name in (MART, "audit.json")}
    with ZipFile(JAR) as jar:
        sql = jar.read("sql/product_repeats.sql")
        assert sql == (ROOT / "sql" / "product_repeats.sql").read_bytes()
    assert manifest["sql_sha256"] == {"product_repeats.sql": hashlib.sha256(sql).hexdigest()}
    for name in (*manifest["artifacts"], "manifest.json"):
        assert (first / name).read_bytes() == (second / name).read_bytes()
    assert not list((first / ".partial").glob("*"))

    original = {p.name: sha256(p) for p in first.iterdir() if p.is_file()}
    result = run_repeats(clean, orders, first)
    assert result.returncode == 1, result.stderr
    assert original == {p.name: sha256(p) for p in first.iterdir() if p.is_file()}


def test_no_known_positive_orders_publishes_empty_mart(tmp_path: Path) -> None:
    clean = run_clean(tmp_path, [("C0", "P", "-1", FROM, "1", "A"), ("S", "P", "1", FROM, "1", "")])
    orders, output = tmp_path / "orders", tmp_path / "out"
    write_orders_dataset(clean, orders)
    result = run_repeats(clean, orders, output)
    assert result.returncode == 0, result.stderr
    assert (output / MART).read_bytes() == b""
    assert document(output / "audit.json")["product_repeats_rows"] == 0
    assert document(output / "manifest.json")["status"] == "completed"


def test_rejects_clean_from_another_run_with_same_counts(tmp_path: Path) -> None:
    clean = run_clean(tmp_path / "first", [("A", "P", "1", FROM, "1", "B")])
    other = run_clean(tmp_path / "other", [("A", "Q", "1", FROM, "1", "B")])
    assert (
        document(clean / "manifest.json")["counts"] == document(other / "manifest.json")["counts"]
    )
    orders, output = tmp_path / "orders", tmp_path / "out"
    write_orders_dataset(clean, orders)
    result = run_repeats(other, orders, output)
    assert result.returncode == 1, result.stderr
    assert (output / "fatal.json").is_file()
    assert not (output / "manifest.json").exists()


def test_rejects_rehashed_lineage_swapping_two_invoices(
    tmp_path: Path, source_pair: tuple[Path, Path]
) -> None:
    clean, original_orders = source_pair
    orders = tmp_path / "orders"
    shutil.copytree(original_orders, orders)
    lineage = orders / "lineage.jsonl"
    swapped = {"A0": "A1", "A1": "A0"}
    rows = records(lineage)
    for row in rows:
        if row["invoice_id"] in swapped:
            row["invoice_id"] = row["order_key"] = swapped[row["invoice_id"]]
    lineage.write_bytes("".join(json.dumps(row) + "\n" for row in rows).encode())
    manifest = document(orders / "manifest.json")
    manifest["artifacts"][lineage.name] = sha256(lineage)
    (orders / "manifest.json").write_text(json.dumps(manifest) + "\n", encoding="utf-8")
    output = tmp_path / "out"
    result = run_repeats(clean, orders, output)
    assert result.returncode == 1, result.stderr
    assert (output / "fatal.json").is_file()
    assert not (output / "manifest.json").exists()


@pytest.mark.parametrize("change", ["replace-member", "truncate-group", "omit-group"])
def test_reconciles_rehashed_duplicate_groups_with_clean_contents(
    tmp_path: Path, source_pair: tuple[Path, Path], change: str
) -> None:
    clean, original_orders = source_pair
    orders = tmp_path / "orders"
    shutil.copytree(original_orders, orders)
    duplicates = orders / "duplicates.jsonl"
    [group] = records(duplicates)
    ids = group["event_ids"]
    assert len(ids) == 3
    manifest = document(orders / "manifest.json")
    if change == "omit-group":
        duplicates.write_bytes(b"")
        audit_path = orders / "audit.json"
        audit = document(audit_path)
        audit["counts"]["duplicate_groups"] = manifest["counts"]["duplicate_groups"] = 0
        audit_path.write_text(json.dumps(audit), encoding="utf-8")
        manifest["artifacts"][audit_path.name] = sha256(audit_path)
    else:
        members = ids[:-1]
        if change == "replace-member":
            # Q belongs to the same invoice and has the same lineage disposition as the P rows.
            members.append(
                next(
                    e["event_id"]
                    for e in records(clean / "clean_events.jsonl")
                    if e["invoice_id"] == "A2" and e["stock_code"] == "Q"
                )
            )
        group["event_ids"] = sorted(members)
        duplicates.write_text(json.dumps(group) + "\n", encoding="utf-8")
    manifest["artifacts"][duplicates.name] = sha256(duplicates)
    (orders / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    output = tmp_path / "out"
    result = run_repeats(clean, orders, output)
    assert result.returncode == 1, result.stderr
    assert "duplicate" in json.dumps(document(output / "fatal.json")).lower()
    assert not (output / "manifest.json").exists()


def test_coverage_checks_anonymous_order_and_argument_bounds(
    tmp_path: Path, source_pair: tuple[Path, Path]
) -> None:
    clean, orders = source_pair
    output = tmp_path / "out"
    result = run_repeats(clean, orders, output, end=FROM)
    assert result.returncode == 64, result.stderr
    assert not output.exists()
    result = run_repeats(clean, orders, output, end="2020-01-31T23:59:59.999999")
    assert result.returncode == 1, result.stderr
    assert (output / "fatal.json").is_file()
    assert not (output / "manifest.json").exists()


def directory_alias(link: Path, target: Path) -> Path:
    try:
        link.symlink_to(target, target_is_directory=True)
    except OSError as error:
        if getattr(error, "winerror", None) in (5, 1314):
            pytest.skip("creating symlinks requires host permission")
        raise
    return link


@pytest.mark.parametrize("aliased", [False, True], ids=["direct", "symlink"])
def test_output_cannot_be_created_in_either_input(
    tmp_path: Path, source_pair: tuple[Path, Path], aliased: bool
) -> None:
    clean, orders = source_pair
    for source in (clean, orders):
        parent = directory_alias(tmp_path / source.name, source) if aliased else source
        output = parent / "nested-output"
        result = run_repeats(clean, orders, output)
        assert result.returncode == 1, result.stderr
        assert not output.exists()


@pytest.mark.parametrize("missing_input", ["clean", "orders"])
@pytest.mark.parametrize("aliased", [False, True], ids=["direct", "two-parent-aliases"])
def test_reserving_output_preserves_absent_input(
    tmp_path: Path, source_pair: tuple[Path, Path], missing_input: str, aliased: bool
) -> None:
    real_parent = tmp_path / "real"
    real_parent.mkdir()
    input_parent = output_parent = real_parent
    if aliased:
        input_parent = directory_alias(tmp_path / "input-parent", real_parent)
        output_parent = directory_alias(tmp_path / "output-parent", real_parent)
    absent = input_parent / "missing"
    clean, orders = source_pair
    if missing_input == "clean":
        clean = absent
    else:
        orders = absent
    result = run_repeats(clean, orders, output_parent / "missing")
    assert result.returncode == 1, result.stderr
    assert not (real_parent / "missing").exists()


@pytest.mark.parametrize("missing_input", ["clean", "orders"])
def test_absent_input_writes_diagnostic_in_separate_output(
    tmp_path: Path, source_pair: tuple[Path, Path], missing_input: str
) -> None:
    absent, output = tmp_path / "missing", tmp_path / "out"
    clean, orders = source_pair
    if missing_input == "clean":
        clean = absent
    else:
        orders = absent
    result = run_repeats(clean, orders, output)
    assert result.returncode == 1, result.stderr
    assert not absent.exists()
    assert document(output / "fatal.json")["status"] == "failed"
    assert not (output / "manifest.json").exists()


@pytest.mark.parametrize("missing_input", ["clean", "orders"])
def test_dangling_input_link_preserves_its_missing_target(
    tmp_path: Path, source_pair: tuple[Path, Path], missing_input: str
) -> None:
    target = tmp_path / "future-output"
    relative_target = Path(target.name)
    link = directory_alias(tmp_path / "source-link", relative_target)
    clean, orders = source_pair
    if missing_input == "clean":
        clean = link
    else:
        orders = link
    separate = tmp_path / "diagnostics"
    for output in (target, separate):
        result = run_repeats(clean, orders, output)
        assert result.returncode == 1, result.stderr
        assert not target.exists()
        assert link.readlink() == relative_target
    assert document(separate / "fatal.json")["status"] == "failed"
    assert not (separate / "manifest.json").exists()
