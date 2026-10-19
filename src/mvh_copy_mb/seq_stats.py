"""
Statistics by Art der Sequenzierung, read from the DuckDB database.
"""

from datetime import datetime
from pathlib import Path
from typing import Optional

import click
import duckdb
from dotenv import load_dotenv
from openpyxl import Workbook
from rich.console import Console
from rich.table import Table

load_dotenv()

SEQ_LABELS = {"0": "None", "1": "WGS", "2": "WES", "3": "Panel", "4": "WGS_LR"}
AREA_LABELS = {
    "R": "Seltene Erkrankungen",
    "O": "Onkologische Erkrankungen",
    "H": "Hereditäre Tumorprädispositionssyndrome",
}

# Leistungsdatum filter; a NULL bound means no limit on that side
DATE_CTE = """
WITH m AS (
    SELECT * FROM meldebestaetigungen
    WHERE ($date_from IS NULL OR output_date >= $date_from)
      AND ($date_to IS NULL OR output_date <= $date_to)
)
"""

# Only our own case IDs (HUMGEN_*) go into the table; all others are counted as excluded
EXCLUDED_SQL = (
    DATE_CTE
    + """
SELECT CASE WHEN case_id IS NULL THEN 'Unresolved'
            WHEN case_id LIKE 'Onkostar%' THEN 'Onkostar'
            WHEN case_id LIKE 'PathoPro%' THEN 'PathoPro'
            ELSE 'Other' END AS kind,
       count(*)
FROM m
WHERE coalesce(case_id, '') NOT LIKE 'HUMGEN%'
GROUP BY kind ORDER BY kind
"""
)

# Art der Sequenzierung is field 10 of the '&'-separated hash string (third '+' segment)
BASE_SQL = (
    DATE_CTE
    + """
, t AS (
    SELECT *, split_part(split_part(meldebestaetigung, '+', 3), '&', 10) AS seq
    FROM m
    WHERE case_id LIKE 'HUMGEN%'
)
"""
)

RECORDS_SQL = (
    BASE_SQL
    + """
SELECT indikationsbereich, seq,
       count(*) FILTER (art_der_daten = 'G') AS g,
       count(*) FILTER (art_der_daten = 'C') AS c
FROM t GROUP BY indikationsbereich, seq
"""
)

# Pairing like the CLI: exact case_id match, only initial reports with passed QC
PAIRS_SQL = (
    BASE_SQL
    + """
, p AS (
    SELECT case_id,
           bool_or(art_der_daten = 'G') AS has_g,
           bool_or(art_der_daten = 'C') AS has_c,
           string_agg(DISTINCT indikationsbereich, '+' ORDER BY indikationsbereich) AS areas,
           string_agg(DISTINCT seq, '+' ORDER BY seq) AS seqs
    FROM t
    WHERE ergebnis_qc = '1' AND typ_der_meldung = '0'
    GROUP BY case_id
)
SELECT areas, seqs,
       count(*) FILTER (has_g AND has_c) AS pairs,
       count(*) FILTER (has_g AND NOT has_c) AS only_g,
       count(*) FILTER (has_c AND NOT has_g) AS only_c
FROM p GROUP BY areas, seqs
"""
)


def _label(seqs: str) -> str:
    return " / ".join(f"{s} {SEQ_LABELS.get(s, '?')}" for s in seqs.split("+"))


def _area_label(areas: str) -> str:
    return " / ".join(f"{AREA_LABELS.get(a, a)} ({a})" for a in areas.split("+"))


COLUMNS = ["Sequencing type", "G", "C", "Pairs", "G only", "C only"]


def _write_xlsx(
    path: Path,
    title: str,
    blocks: list[tuple[str, list[list[Optional[int] | str]]]],
    footer: list[str],
) -> None:
    """Write the same layout as the console output to one Excel sheet."""
    wb = Workbook()
    ws = wb.active
    assert ws is not None
    ws.title = "Statistics"
    ws.append([title])
    for area_label, rows in blocks:
        ws.append([])
        ws.append([area_label])
        ws.append(COLUMNS)
        for row in rows:
            ws.append(row)
    ws.append([])
    for line in footer:
        ws.append([line])
    ws.column_dimensions["A"].width = 20
    wb.save(path)


@click.command()
@click.option(
    "--db-path",
    envvar="DB_PATH",
    type=click.Path(exists=True, dir_okay=False),
    required=True,
    help="Path to DuckDB database file",
)
@click.option(
    "--from",
    "date_from",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="Only Leistungsdatum on or after this date (YYYY-MM-DD)",
)
@click.option(
    "--to",
    "date_to",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="Only Leistungsdatum on or before this date (YYYY-MM-DD)",
)
@click.option(
    "--xlsx",
    "xlsx_path",
    type=click.Path(dir_okay=False),
    help="Also write the statistics to this Excel file",
)
def main(
    db_path: str,
    date_from: Optional[datetime],
    date_to: Optional[datetime],
    xlsx_path: Optional[str],
) -> None:
    """Show Meldebestaetigung statistics by Art der Sequenzierung."""
    params = {
        "date_from": date_from.date() if date_from else None,
        "date_to": date_to.date() if date_to else None,
    }
    with duckdb.connect(str(Path(db_path)), read_only=True) as conn:
        records = {r[:2]: r[2:] for r in conn.execute(RECORDS_SQL, params).fetchall()}
        pairs = {r[:2]: r[2:] for r in conn.execute(PAIRS_SQL, params).fetchall()}
        excluded = conn.execute(EXCLUDED_SQL, params).fetchall()

    title = "STATISTICS BY SEQUENCING TYPE"
    if date_from or date_to:
        title += f" (Leistungsdatum {params['date_from'] or '…'} to {params['date_to'] or '…'})"
    note = (
        "Pairs: exact Case ID match, initial reports (Typ 0) with passed QC only. "
        "Rows with '/' are cases whose G and C report different sequencing types."
    )
    excluded_text = "Excluded (no HUMGEN case ID): " + (
        ", ".join(f"{kind} {n}" for kind, n in excluded) or "none"
    )

    # One block per Indikationsbereich present; mixed-area cases ("H+R") come last.
    # Within a block: single types first, mixed G/C type combinations ("1+4") after.
    keys = records.keys() | pairs.keys()
    blocks: list[tuple[str, list[list[Optional[int] | str]]]] = []
    for area in sorted({a for a, _ in keys}, key=lambda a: ("+" in a, a)):
        seqs = sorted({s for a, s in keys if a == area}, key=lambda s: ("+" in s, s))
        rows = [
            [
                _label(seq),
                *(records.get((area, seq)) or (None, None)),
                *pairs.get((area, seq), (0, 0, 0)),
            ]
            for seq in seqs
        ]
        blocks.append((_area_label(area), rows))

    console = Console()
    console.print(title, style="bold")
    for area_label, rows in blocks:
        tbl = Table(title=area_label)
        tbl.add_column(COLUMNS[0])
        for col in COLUMNS[1:]:
            tbl.add_column(col, justify="right")
        for row in rows:
            tbl.add_row(*("" if v is None else str(v) for v in row))
        console.print(tbl)
    console.print(note)
    console.print(excluded_text)

    if xlsx_path:
        _write_xlsx(Path(xlsx_path), title, blocks, [note, excluded_text])
        console.print(f"Written to {xlsx_path}")


if __name__ == "__main__":
    main()  # type: ignore[call-arg]
