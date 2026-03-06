"""Query database schema and output tables.json with tables, columns, PKs, and FKs."""

import json
import sys
from datetime import datetime
from pathlib import Path

from db import get_config, connect, format_type


def query_tables(conn, config):
    """Query database schema and return tables dict."""
    cursor = conn.cursor()

    # 1. Get all user tables
    cursor.execute("SELECT t.name FROM sys.tables t WHERE t.type = 'U' ORDER BY t.name")
    tables = [row[0] for row in cursor.fetchall()]
    print(f"Found {len(tables)} tables.", file=sys.stderr)

    # 2. Get all columns
    cursor.execute("""
        SELECT t.name, c.name, tp.name, c.max_length, c.is_nullable, c.precision, c.scale, c.is_identity
        FROM sys.columns c
        JOIN sys.tables t ON c.object_id = t.object_id
        JOIN sys.types tp ON c.user_type_id = tp.user_type_id
        WHERE t.type = 'U'
        ORDER BY t.name, c.column_id
    """)
    columns = {}
    for row in cursor.fetchall():
        tbl, col_name, type_name, max_len, nullable, prec, scale, identity = row
        full_type = format_type(type_name, max_len, prec, scale)
        columns.setdefault(tbl, []).append({
            "name": col_name,
            "type": full_type,
            "nullable": bool(nullable),
            **({"identity": True} if identity else {}),
        })

    # 3. Get primary keys
    cursor.execute("""
        SELECT t.name, c.name
        FROM sys.indexes i
        JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
        JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        JOIN sys.tables t ON i.object_id = t.object_id
        WHERE i.is_primary_key = 1
        ORDER BY t.name, ic.key_ordinal
    """)
    primary_keys = {}
    for tbl, col in cursor.fetchall():
        primary_keys.setdefault(tbl, []).append(col)

    # 4. Get foreign keys
    cursor.execute("""
        SELECT tp.name, cp.name, tr.name, cr.name
        FROM sys.foreign_keys fk
        JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
        JOIN sys.tables tp ON fkc.parent_object_id = tp.object_id
        JOIN sys.columns cp ON fkc.parent_object_id = cp.object_id AND fkc.parent_column_id = cp.column_id
        JOIN sys.tables tr ON fkc.referenced_object_id = tr.object_id
        JOIN sys.columns cr ON fkc.referenced_object_id = cr.object_id AND fkc.referenced_column_id = cr.column_id
        ORDER BY tp.name, fkc.constraint_column_id
    """)
    foreign_keys = {}
    for tbl, col, ref_tbl, ref_col in cursor.fetchall():
        foreign_keys.setdefault(tbl, []).append({
            "column": col,
            "references": f"{ref_tbl}.{ref_col}",
        })

    # Build output
    result = {
        "database": config["database"],
        "server": config["server"],
        "generated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "tableCount": len(tables),
        "tables": {},
    }
    for tbl in tables:
        result["tables"][tbl] = {
            "primaryKey": primary_keys.get(tbl, []),
            "foreignKeys": foreign_keys.get(tbl, []),
            "columns": columns.get(tbl, []),
        }

    return result


def main(output_path=None):
    config = get_config()
    print(f"Connecting to {config['server']} as {config['user']}...", file=sys.stderr)
    conn = connect(config)
    print("Connected.", file=sys.stderr)

    result = query_tables(conn, config)
    conn.close()

    if output_path is None:
        output_path = Path(__file__).resolve().parent / "data" / "tables.json"
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Written to {output_path}", file=sys.stderr)
    return result


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else None
    main(path)
