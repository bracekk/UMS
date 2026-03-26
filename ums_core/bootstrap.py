from ums_core import db_compat as sqlite3
from pathlib import Path


def table_exists(cursor, table_name: str) -> bool:
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    )
    return cursor.fetchone() is not None


def column_exists(cursor, table_name: str, column_name: str) -> bool:
    cursor.execute(f"PRAGMA table_info({table_name})")
    return any(row[1] == column_name for row in cursor.fetchall())


def bootstrap_database(db_path: str = "database.db") -> None:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Core tables / columns created by the legacy app plus safe incremental upgrades.
    if table_exists(cur, "users") and not column_exists(cur, "users", "is_active"):
        cur.execute("ALTER TABLE users ADD COLUMN is_active INTEGER DEFAULT 1")

    if table_exists(cur, "production_reports") and not column_exists(cur, "production_reports", "unit"):
        cur.execute("ALTER TABLE production_reports ADD COLUMN unit TEXT DEFAULT 'pcs'")

    if table_exists(cur, "workstations") and not column_exists(cur, "workstations", "cost_per_hour"):
        cur.execute("ALTER TABLE workstations ADD COLUMN cost_per_hour REAL NOT NULL DEFAULT 0")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS product_groups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(company_id, name)
        )
        """
    )

    if table_exists(cur, "products") and not column_exists(cur, "products", "group_id"):
        cur.execute("ALTER TABLE products ADD COLUMN group_id INTEGER")

    if table_exists(cur, "products"):
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_products_company_group ON products(company_id, group_id)"
        )

    if table_exists(cur, "batch_orders"):
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_batch_orders_batch_company ON batch_orders(batch_id, company_id)"
        )

    if table_exists(cur, "product_job_templates"):
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_pjt_product_company ON product_job_templates(product_id, company_id)"
        )

    if table_exists(cur, "bom"):
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_bom_product_company ON bom(product_id, company_id)"
        )

    conn.commit()
    conn.close()
