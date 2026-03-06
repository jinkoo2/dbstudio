# DB Studio

Web-based SQL Server database explorer with an interactive ER diagram viewer and SQL query editor.

## Features

- **ER Diagram** -- Interactive entity-relationship graph (Cytoscape.js) with search, expand, and detail panel
- **SQL Query** -- Execute queries against SQL Server with tabular results, row limits, and execution timing
- **Context menu** -- Right-click any table in the ER diagram to run `SELECT TOP 10 *`
- **Auto-generation** -- Schema data files are generated from the database on first startup if missing
- **Dark/Light theme** -- Toggle between dark and light mode

## Quick Start

### Windows (conda)

```bash
# One-time setup
conda create -n dbstudio python=3.12 -y
conda activate dbstudio
pip install -r requirements.txt

# Configure
cp .env.example .env   # then edit .env with your DB credentials

# Run
python server.py       # or double-click web/serve.bat
```

Open http://localhost:8080

### Docker

```bash
cp .env.example .env   # then edit .env with your DB credentials
docker compose up --build
```

## Configuration

All configuration is via `.env` file (see `.env.example`):

| Variable | Default | Description |
|---|---|---|
| `DB_SERVER` | *(required)* | SQL Server hostname\instance |
| `DB_USER` | *(required)* | SQL Server username |
| `DB_PASSWORD` | *(required)* | SQL Server password |
| `DB_NAME` | `VARIAN` | Database name |
| `DB_DRIVER` | `SQL Server` | ODBC driver name (use `ODBC Driver 18 for SQL Server` for Linux/Docker) |

## Project Structure

```
dbstudio/
├── server.py                  # FastAPI app (entry point)
├── db.py                      # Shared DB connection utility
├── query_tables.py            # Schema query: tables, columns, PKs, FKs
├── query_columns.py           # Schema query: flat column list with metadata
├── build_entity_relations.py  # Transforms tables.json -> entity-relations.json
├── query_patients.py          # Simple test query script
├── requirements.txt
├── .env.example
├── Dockerfile
├── docker-compose.yml
├── data/                      # Generated schema data (auto-created)
│   ├── tables.json
│   ├── columns.json
│   └── entity-relations.json
└── web/
    ├── index.html             # Single-page app (ER diagram + SQL query)
    └── serve.bat              # Windows launcher
```

## API Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/` | Redirect to `/static/index.html` |
| `GET` | `/api/data` | Returns `entity-relations.json` |
| `POST` | `/api/query` | Execute SQL query (body: `{query}`). DB credentials come from `.env`. |

## Data Generation

On startup, if `data/tables.json`, `data/columns.json`, or `data/entity-relations.json` are missing, the server connects to the database using `.env` credentials and generates them automatically.

To regenerate, delete the files in `data/` and restart the server.

## Standalone Scripts

The schema query scripts can also be run independently:

```bash
python query_tables.py           # -> data/tables.json
python query_columns.py          # -> data/columns.json
python build_entity_relations.py # -> data/entity-relations.json (from tables.json)
python query_patients.py         # prints top 10 patients (test script)
```
