"""DB Studio - FastAPI Server"""

import json
import sys
import time
from pathlib import Path

import pyodbc
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from db import get_config, connect
from query_tables import query_tables
from query_columns import query_columns
from build_entity_relations import build_entity_relations

app = FastAPI(title="DB Studio")

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
WEB_DIR = BASE_DIR / "web"

MAX_ROWS = 1000


class QueryRequest(BaseModel):
    query: str


def generate_data_files():
    """Generate tables.json, columns.json, and entity-relations.json if missing."""
    tables_path = DATA_DIR / "tables.json"
    columns_path = DATA_DIR / "columns.json"
    er_path = DATA_DIR / "entity-relations.json"

    needed = []
    if not tables_path.exists():
        needed.append("tables.json")
    if not columns_path.exists():
        needed.append("columns.json")
    if not er_path.exists():
        needed.append("entity-relations.json")

    if not needed:
        return

    print(f"Generating missing data files: {', '.join(needed)}", file=sys.stderr)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    config = get_config()
    if not config["server"] or not config["user"]:
        print("WARNING: DB_SERVER/DB_USER not set in .env — skipping data generation.", file=sys.stderr)
        return

    try:
        print(f"Connecting to {config['server']} as {config['user']}...", file=sys.stderr)
        conn = connect(config)
        print("Connected.", file=sys.stderr)
    except Exception as e:
        print(f"WARNING: Could not connect to database — skipping data generation: {e}", file=sys.stderr)
        return

    try:
        # Generate tables.json (needed for entity-relations.json too)
        tables_data = None
        if not tables_path.exists() or not er_path.exists():
            tables_data = query_tables(conn, config)
            tables_path.write_text(json.dumps(tables_data, indent=2, ensure_ascii=False), encoding="utf-8")
            print(f"Generated {tables_path}", file=sys.stderr)

        # Generate columns.json
        if not columns_path.exists():
            columns_data = query_columns(conn, config)
            columns_path.write_text(json.dumps(columns_data, indent=2, ensure_ascii=False), encoding="utf-8")
            print(f"Generated {columns_path}", file=sys.stderr)

        # Generate entity-relations.json from tables.json
        if not er_path.exists():
            if tables_data is None:
                tables_data = json.loads(tables_path.read_text(encoding="utf-8-sig"))
            er_data = build_entity_relations(tables_data)
            er_path.write_text(json.dumps(er_data, indent=2, ensure_ascii=False), encoding="utf-8")
            print(f"Generated {er_path}", file=sys.stderr)
    finally:
        conn.close()

    print("Data generation complete.", file=sys.stderr)


@app.get("/")
def root():
    return RedirectResponse(url="/static/index.html")


@app.get("/api/data")
def get_data():
    path = DATA_DIR / "entity-relations.json"
    if not path.exists():
        raise HTTPException(status_code=404, detail="entity-relations.json not found")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


@app.post("/api/query")
def run_query(req: QueryRequest):
    query = req.query.strip()
    if not query:
        raise HTTPException(status_code=400, detail="Query cannot be empty")

    try:
        conn = connect()
    except pyodbc.Error as e:
        raise HTTPException(status_code=400, detail=f"Connection failed: {e}")

    try:
        cursor = conn.cursor()
        start = time.perf_counter()
        cursor.execute(query)

        if cursor.description is None:
            elapsed = round((time.perf_counter() - start) * 1000, 1)
            row_count = cursor.rowcount
            conn.close()
            return {
                "columns": [],
                "rows": [],
                "rowCount": row_count if row_count >= 0 else 0,
                "elapsed": elapsed,
                "message": f"Statement executed successfully. {row_count} row(s) affected."
            }

        columns = [col[0] for col in cursor.description]
        rows = []
        for i, row in enumerate(cursor):
            if i >= MAX_ROWS:
                break
            rows.append([str(v) if v is not None else None for v in row])

        elapsed = round((time.perf_counter() - start) * 1000, 1)
        total_rows = len(rows)
        truncated = total_rows >= MAX_ROWS

        conn.close()
        return {
            "columns": columns,
            "rows": rows,
            "rowCount": total_rows,
            "elapsed": elapsed,
            "truncated": truncated,
            "message": f"Returned {total_rows} row(s){' (limit reached)' if truncated else ''} in {elapsed} ms"
        }
    except pyodbc.Error as e:
        conn.close()
        raise HTTPException(status_code=400, detail=str(e))


app.mount("/static", StaticFiles(directory=str(WEB_DIR)), name="static")

if __name__ == "__main__":
    generate_data_files()
    uvicorn.run(app, host="0.0.0.0", port=8080)
