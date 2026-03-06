"""Query database columns with PK/FK metadata and output columns.json."""

import json
import sys
from datetime import datetime
from pathlib import Path

from db import get_config, connect, format_type


def query_columns(conn, config):
    """Query all columns with PK/FK info and return result dict."""
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            t.name AS table_name,
            c.name AS column_name,
            tp.name AS type_name,
            c.max_length,
            c.precision,
            c.scale,
            c.is_nullable,
            c.is_identity,
            c.column_id,
            CASE WHEN pk.column_id IS NOT NULL THEN 1 ELSE 0 END AS is_primary_key,
            fk_ref.referenced_table,
            fk_ref.referenced_column
        FROM sys.columns c
        JOIN sys.tables t ON c.object_id = t.object_id AND t.type = 'U'
        JOIN sys.types tp ON c.user_type_id = tp.user_type_id
        LEFT JOIN (
            SELECT ic.object_id, ic.column_id
            FROM sys.index_columns ic
            JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
            WHERE i.is_primary_key = 1
        ) pk ON pk.object_id = c.object_id AND pk.column_id = c.column_id
        LEFT JOIN (
            SELECT
                fkc.parent_object_id,
                fkc.parent_column_id,
                tr.name AS referenced_table,
                cr.name AS referenced_column
            FROM sys.foreign_key_columns fkc
            JOIN sys.tables tr ON fkc.referenced_object_id = tr.object_id
            JOIN sys.columns cr ON fkc.referenced_object_id = cr.object_id AND fkc.referenced_column_id = cr.column_id
        ) fk_ref ON fk_ref.parent_object_id = c.object_id AND fk_ref.parent_column_id = c.column_id
        ORDER BY t.name, c.column_id
    """)

    columns = []
    for row in cursor.fetchall():
        tbl, col, type_name, max_len, prec, scale, nullable, identity, col_id, is_pk, ref_tbl, ref_col = row
        full_type = format_type(type_name, max_len, prec, scale)
        entry = {
            "table": tbl,
            "column": col,
            "ordinal": col_id,
            "type": full_type,
            "nullable": bool(nullable),
        }
        if identity:
            entry["identity"] = True
        if is_pk:
            entry["primaryKey"] = True
        if ref_tbl is not None:
            entry["foreignKey"] = {"table": ref_tbl, "column": ref_col}
        columns.append(entry)

    print(f"Found {len(columns)} columns.", file=sys.stderr)

    return {
        "database": config["database"],
        "server": config["server"],
        "generated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "columns": columns,
        "columnCount": len(columns),
    }


def main(output_path=None):
    config = get_config()
    print(f"Connecting to {config['server']} as {config['user']}...", file=sys.stderr)
    conn = connect(config)
    print("Connected.", file=sys.stderr)

    result = query_columns(conn, config)
    conn.close()

    if output_path is None:
        output_path = Path(__file__).resolve().parent / "data" / "columns.json"
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Written to {output_path}", file=sys.stderr)
    return result


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else None
    main(path)
