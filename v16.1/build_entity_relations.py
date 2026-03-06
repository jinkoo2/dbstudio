"""Build entity-relations.json from tables.json for the ER Explorer web app."""

import json
import sys
from datetime import datetime
from pathlib import Path


def build_entity_relations(tables_data):
    """Transform tables.json data into entity-relations.json format."""
    tables = tables_data["tables"]
    entities = {}
    relationships = []
    rel_set = set()

    for table_name, tbl in tables.items():
        # Build entity
        entity = {
            "primaryKey": tbl.get("primaryKey", []),
            "columns": [
                {
                    "name": c["name"],
                    "type": c["type"],
                    "nullable": c["nullable"],
                    **({"identity": True} if c.get("identity") else {}),
                }
                for c in tbl.get("columns", [])
            ],
        }
        entities[table_name] = entity

        # Build relationships from foreign keys
        for fk in tbl.get("foreignKeys", []):
            from_col = fk["column"]
            ref_parts = fk["references"].split(".")
            to_table = ref_parts[0]
            to_col = ref_parts[1]

            key = f"{table_name}.{from_col}->{to_table}.{to_col}"
            if key in rel_set:
                continue
            rel_set.add(key)

            relationships.append({
                "fromTable": table_name,
                "fromColumn": from_col,
                "toTable": to_table,
                "toColumn": to_col,
                "type": "many-to-one",
            })

    print(f"Entities: {len(entities)}, Relationships: {len(relationships)}", file=sys.stderr)

    return {
        "metadata": {
            "database": tables_data.get("database", ""),
            "server": tables_data.get("server", ""),
            "version": "16.1",
            "generated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "entityCount": len(entities),
            "relationshipCount": len(relationships),
        },
        "entities": entities,
        "relationships": relationships,
    }


def main(data_dir=None, output_path=None):
    if data_dir is None:
        data_dir = Path(__file__).resolve().parent / "data"
    data_dir = Path(data_dir)

    tables_path = data_dir / "tables.json"
    print(f"Reading {tables_path}...", file=sys.stderr)
    tables_data = json.loads(tables_path.read_text(encoding="utf-8-sig"))

    result = build_entity_relations(tables_data)

    if output_path is None:
        output_path = data_dir / "entity-relations.json"
    output_path = Path(output_path)
    output_path.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Written to {output_path}", file=sys.stderr)
    return result


if __name__ == "__main__":
    d = sys.argv[1] if len(sys.argv) > 1 else None
    o = sys.argv[2] if len(sys.argv) > 2 else None
    main(d, o)
