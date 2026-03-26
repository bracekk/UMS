import argparse
import os
import sqlite3
from pathlib import Path

import psycopg

TABLE_ORDER = [
    "companies",
    "users",
    "password_reset_tokens",
    "product_groups",
    "workstation_groups",
    "suppliers",
    "items",
    "products",
    "workstations",
    "dashboard_layouts",
    "stock_destinations",
    "orders",
    "order_batches",
    "batch_orders",
    "bom",
    "product_job_templates",
    "order_jobs",
    "purchase_requests",
    "user_permissions",
    "shortages",
    "product_transfers_out",
    "production_reports",
]


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def list_source_tables(source_conn: sqlite3.Connection) -> list[str]:
    cur = source_conn.cursor()
    cur.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
          AND name NOT LIKE 'sqlite_%'
        ORDER BY name
        """
    )
    return [row[0] for row in cur.fetchall()]


def reset_sequence(target_conn, table_name: str) -> None:
    with target_conn.cursor() as cur:
        cur.execute(
            f"SELECT COALESCE(MAX(id), 0) FROM {quote_ident(table_name)}"
        )
        max_id = cur.fetchone()[0]
        cur.execute(
            "SELECT pg_get_serial_sequence(%s, 'id')",
            (table_name,),
        )
        sequence_name = cur.fetchone()[0]
        if sequence_name:
            cur.execute("SELECT setval(%s, %s, %s)", (sequence_name, max_id if max_id > 0 else 1, max_id > 0))


def truncate_target_tables(target_conn, tables: list[str]) -> None:
    with target_conn.cursor() as cur:
        for table_name in reversed(tables):
            cur.execute(f"TRUNCATE TABLE {quote_ident(table_name)} RESTART IDENTITY CASCADE")
    target_conn.commit()


def copy_table(source_conn, target_conn, table_name: str) -> int:
    source_cur = source_conn.cursor()
    source_cur.execute(f"SELECT * FROM {quote_ident(table_name)}")
    rows = source_cur.fetchall()
    if not rows:
        return 0

    columns = [description[0] for description in source_cur.description]
    column_sql = ", ".join(quote_ident(column) for column in columns)
    placeholder_sql = ", ".join(["%s"] * len(columns))

    with target_conn.cursor() as target_cur:
        target_cur.executemany(
            f"INSERT INTO {quote_ident(table_name)} ({column_sql}) VALUES ({placeholder_sql})",
            rows,
        )
    target_conn.commit()

    if "id" in columns:
        reset_sequence(target_conn, table_name)
        target_conn.commit()

    return len(rows)


def ensure_postgres_schema() -> None:
    import app  # noqa: F401  # importing app initializes and upgrades schema


def main() -> None:
    parser = argparse.ArgumentParser(description="Copy data from SQLite UMS database into PostgreSQL.")
    parser.add_argument("--sqlite-path", default="database.db", help="Path to the source SQLite database file.")
    parser.add_argument(
        "--skip-truncate",
        action="store_true",
        help="Do not truncate target PostgreSQL tables before copying data.",
    )
    args = parser.parse_args()

    database_url = os.environ.get("DATABASE_URL", "").strip()
    if not database_url.startswith(("postgres://", "postgresql://")):
        raise SystemExit("DATABASE_URL must point to PostgreSQL before running this migration script.")

    sqlite_path = Path(args.sqlite_path)
    if not sqlite_path.exists():
        raise SystemExit(f"SQLite file not found: {sqlite_path}")

    ensure_postgres_schema()

    source_conn = sqlite3.connect(sqlite_path)
    target_conn = psycopg.connect(database_url)

    source_tables = set(list_source_tables(source_conn))
    tables_to_copy = [table for table in TABLE_ORDER if table in source_tables]
    extra_tables = sorted(source_tables - set(TABLE_ORDER))
    tables_to_copy.extend(extra_tables)

    if not args.skip_truncate:
        truncate_target_tables(target_conn, tables_to_copy)

    print("Starting SQLite -> PostgreSQL copy...")
    for table_name in tables_to_copy:
        copied = copy_table(source_conn, target_conn, table_name)
        print(f"  {table_name}: {copied} rows")

    source_conn.close()
    target_conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
