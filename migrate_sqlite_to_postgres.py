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


def truncate_target_tables(target_conn, table_names):
    with target_conn.cursor() as cur:
        existing_tables = set()

        cur.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
        """)
        for row in cur.fetchall():
            existing_tables.add(row[0])

        for table_name in table_names:
            if table_name not in existing_tables:
                print(f"Skipping missing target table: {table_name}")
                continue

            cur.execute(f'TRUNCATE TABLE "{table_name}" RESTART IDENTITY CASCADE')

    target_conn.commit()


def copy_table(source_conn, target_conn, table_name: str) -> int:
    source_cur = source_conn.cursor()
    source_cur.execute(f"SELECT * FROM {quote_ident(table_name)}")
    rows = source_cur.fetchall()
    if not rows:
        return 0

    source_columns = [description[0] for description in source_cur.description]

    # source -> target column renames
    column_renames = {
        "users": {
            "company": "company_id",
        },
        "items": {
            "measurement_unit": "unit",
        },
    }

    # read real target columns from postgres
    with target_conn.cursor() as cur:
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = %s
            ORDER BY ordinal_position
        """, (table_name,))
        target_columns = {row[0] for row in cur.fetchall()}

    mapped_columns = []
    keep_indexes = []
    seen = set()

    for idx, col in enumerate(source_columns):
        mapped = column_renames.get(table_name, {}).get(col, col)

        # skip columns that do not exist in target table
        if mapped not in target_columns:
            continue

        # skip duplicates after rename
        if mapped in seen:
            continue

        seen.add(mapped)
        mapped_columns.append(mapped)
        keep_indexes.append(idx)

    if not mapped_columns:
        return 0

    filtered_rows = []
    for row in rows:
        new_row = [row[idx] for idx in keep_indexes]

        # special transform for users.company -> users.company_id
        if table_name == "users" and "company_id" in mapped_columns:
            company_id_idx = mapped_columns.index("company_id")

            company_name_to_id = {}
            with target_conn.cursor() as cur:
                cur.execute("SELECT id, name FROM companies")
                for company_id, company_name in cur.fetchall():
                    if company_name:
                        company_name_to_id[str(company_name).strip().lower()] = company_id

            value = new_row[company_id_idx]
            if value not in (None, "", 0):
                try:
                    new_row[company_id_idx] = int(value)
                except (TypeError, ValueError):
                    company_key = str(value).strip().lower()
                    new_row[company_id_idx] = company_name_to_id.get(company_key)
            else:
                new_row[company_id_idx] = None

        filtered_rows.append(tuple(new_row))

    column_sql = ", ".join(quote_ident(column) for column in mapped_columns)
    placeholder_sql = ", ".join(["%s"] * len(mapped_columns))

    with target_conn.cursor() as target_cur:
        target_cur.executemany(
            f"INSERT INTO {quote_ident(table_name)} ({column_sql}) VALUES ({placeholder_sql})",
            filtered_rows,
        )

    target_conn.commit()

    if "id" in mapped_columns:
        reset_sequence(target_conn, table_name)
        target_conn.commit()

    return len(filtered_rows)

def list_target_tables(target_conn) -> set[str]:
    with target_conn.cursor() as cur:
        cur.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
        """)
        return {row[0] for row in cur.fetchall()}

def ensure_postgres_schema() -> None:
    import app as app_module

    print("Creating PostgreSQL schema...")
    app_module.init_db()
    app_module.ensure_workstation_groups_and_batch_delete_schema()
    print("Schema created.")

    with psycopg.connect(os.environ["DATABASE_URL"]) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                ORDER BY table_name
            """)
            print("POSTGRES TABLES:", [row[0] for row in cur.fetchall()])


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
    target_tables = list_target_tables(target_conn)

    tables_to_copy = [
        table for table in TABLE_ORDER
        if table in source_tables and table in target_tables
    ]

    extra_tables = sorted(source_tables - set(TABLE_ORDER))
    tables_to_copy.extend([table for table in extra_tables if table in target_tables])

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
