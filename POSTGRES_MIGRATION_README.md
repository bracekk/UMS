# UMS PostgreSQL migration package

## What changed
- `app.py` now uses `ums_core/db_compat.py` instead of importing Python's built-in `sqlite3` directly.
- `ums_core/db_compat.py` keeps your existing SQLite-style queries working while switching to PostgreSQL whenever `DATABASE_URL` is set.
- `ums_core/bootstrap.py` now uses the same compatibility layer.
- `requirements.txt` now installs `psycopg[binary]`.
- `Procfile` and `render.yaml` now start Gunicorn with explicit Render port binding and a safer production command.
- `migrate_sqlite_to_postgres.py` copies data from your existing `database.db` into PostgreSQL.

## Render env vars
Set these on Render:
- `DATABASE_URL` = your Render PostgreSQL internal connection string
- `SECRET_KEY` = your Flask secret key
- keep any other existing env vars you already use

You no longer need to point the app to SQLite for production.

## Migration steps
1. Create a PostgreSQL instance on Render.
2. Copy its internal database URL into your web service as `DATABASE_URL`.
3. Deploy this code.
4. Open a Render shell for the web service and run:
   ```bash
   python migrate_sqlite_to_postgres.py --sqlite-path database.db
   ```
   If your SQLite file is somewhere else, pass that full path instead.
5. Restart the service.

## Local development
- If `DATABASE_URL` is **not** set, the app still uses SQLite exactly like before.
- If `DATABASE_URL` **is** set, the app uses PostgreSQL.

## Notes
- This package is designed to minimize rewrites by preserving your existing SQLite-style SQL in `app.py`.
- It is the fastest path to get your current UMS codebase onto PostgreSQL without rewriting hundreds of queries first.
