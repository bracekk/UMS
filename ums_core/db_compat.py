import os
import re
import sqlite3 as _sqlite3
from collections.abc import Sequence
from typing import Any

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
USE_POSTGRES = DATABASE_URL.startswith(("postgres://", "postgresql://"))

if USE_POSTGRES:
    import psycopg
    from psycopg import errors as psycopg_errors

    IntegrityError = psycopg.IntegrityError
    OperationalError = psycopg.OperationalError
else:
    IntegrityError = _sqlite3.IntegrityError
    OperationalError = _sqlite3.OperationalError


class Row(Sequence):
    def __init__(self, columns: list[str], values: Sequence[Any]):
        self._columns = list(columns)
        self._values = tuple(values)
        self._index = {name: idx for idx, name in enumerate(self._columns)}

    def __getitem__(self, key):
        if isinstance(key, str):
            return self._values[self._index[key]]
        return self._values[key]

    def __len__(self) -> int:
        return len(self._values)

    def __iter__(self):
        return iter(self._values)

    def keys(self):
        return list(self._columns)

    def __contains__(self, item):
        return item in self._columns

    def __repr__(self) -> str:
        return f"Row({dict(zip(self._columns, self._values))})"


if not USE_POSTGRES:
    connect = _sqlite3.connect
else:
    SQLITE_MASTER_TABLE_QUERY = re.compile(
        r"select\s+name\s+from\s+sqlite_master\s+where\s+type\s*=\s*'table'\s+and\s+name\s*=\s*\?",
        re.IGNORECASE | re.DOTALL,
    )
    PRAGMA_TABLE_INFO_QUERY = re.compile(
        r"pragma\s+table_info\((?P<table>[a-zA-Z_][a-zA-Z0-9_]*)\)",
        re.IGNORECASE,
    )

    SQLITE_TO_POSTGRES_TYPES = {
        "integer": "INTEGER",
        "bigint": "BIGINT",
        "double precision": "REAL",
        "numeric": "REAL",
        "real": "REAL",
        "character varying": "TEXT",
        "text": "TEXT",
        "timestamp without time zone": "TIMESTAMP",
        "timestamp with time zone": "TIMESTAMP",
        "date": "DATE",
        "boolean": "INTEGER",
    }

    def _convert_type_name(type_name: str) -> str:
        normalized = type_name.lower()
        return SQLITE_TO_POSTGRES_TYPES.get(normalized, type_name.upper())

    def _normalize_sql(sql: str) -> str:
        translated = sql
        translated = re.sub(
            r"INTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT",
            "BIGSERIAL PRIMARY KEY",
            translated,
            flags=re.IGNORECASE,
        )
        translated = translated.replace("AUTOINCREMENT", "")
        translated = re.sub(r"\bIFNULL\s*\(", "COALESCE(", translated, flags=re.IGNORECASE)

        if re.match(r"\s*CREATE\s+TABLE", translated, flags=re.IGNORECASE):
            # Remove SQLite-style FOREIGN KEY constraint lines without corrupting
            # nearby UNIQUE(...) clauses or leaving dangling parentheses.
            lines = translated.splitlines()
            cleaned = []
            for line in lines:
                if re.search(r"\bFOREIGN\s+KEY\b", line, flags=re.IGNORECASE):
                    continue
                cleaned.append(line)
            translated = "\n".join(cleaned)
            translated = re.sub(r",\s*\)", "\n)", translated, flags=re.DOTALL)

        translated = translated.replace("?", "%s")
        return translated


    class CursorWrapper:
        def __init__(self, connection: "ConnectionWrapper"):
            self.connection = connection
            self._cursor = connection._raw.cursor()
            self.lastrowid = None
            self._manual_results = None
            self.description = None

        def _set_manual_results(self, columns: list[str], rows: list[Sequence[Any]]):
            self._manual_results = [Row(columns, row) if self.connection.row_factory is Row else tuple(row) for row in rows]
            self.description = [(name, None, None, None, None, None, None) for name in columns]

        def _wrap_row(self, row):
            if row is None:
                return None
            if self.connection.row_factory is not Row:
                return row
            columns = [desc[0] for desc in self.description or self._cursor.description or []]
            return Row(columns, row)

        def _wrap_rows(self, rows):
            return [self._wrap_row(row) for row in rows]

        def execute(self, sql: str, params=None):
            params = tuple(params or ())
            self.lastrowid = None
            self._manual_results = None
            stripped = sql.strip()

            pragma_match = PRAGMA_TABLE_INFO_QUERY.fullmatch(stripped.rstrip(";"))
            if pragma_match:
                table_name = pragma_match.group("table")
                self._cursor.execute(
                    """
                    SELECT
                        ordinal_position - 1 AS cid,
                        column_name AS name,
                        data_type,
                        CASE WHEN is_nullable = 'NO' THEN 1 ELSE 0 END AS notnull,
                        column_default AS dflt_value,
                        CASE
                            WHEN column_name = 'id' AND column_default LIKE 'nextval(%%' THEN 1
                            ELSE 0
                        END AS pk
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = %s
                    ORDER BY ordinal_position
                    """,
                    (table_name,),
                )
                rows = []
                for cid, name, data_type, notnull, dflt_value, pk in self._cursor.fetchall():
                    rows.append((cid, name, _convert_type_name(data_type), notnull, dflt_value, pk))
                self._set_manual_results(["cid", "name", "type", "notnull", "dflt_value", "pk"], rows)
                return self

            lowered = stripped.lower().rstrip(";")
            if lowered.startswith("pragma "):
                self._set_manual_results([], [])
                return self

            if SQLITE_MASTER_TABLE_QUERY.fullmatch(lowered):
                table_name = params[0] if params else None
                self._cursor.execute(
                    """
                    SELECT table_name AS name
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                      AND table_name = %s
                    """,
                    (table_name,),
                )
                rows = self._cursor.fetchall()
                self._set_manual_results(["name"], rows)
                return self

            translated_sql = _normalize_sql(sql)
            self._cursor.execute(translated_sql, params)
            self.description = self._cursor.description

            if re.match(r"\s*INSERT\s+INTO\s+([a-zA-Z_][a-zA-Z0-9_]*)", sql, flags=re.IGNORECASE):
                table_name = re.match(
                    r"\s*INSERT\s+INTO\s+([a-zA-Z_][a-zA-Z0-9_]*)",
                    sql,
                    flags=re.IGNORECASE,
                ).group(1)
                helper = self.connection._raw.cursor()
                try:
                    helper.execute(
                        "SELECT currval(pg_get_serial_sequence(%s, 'id'))",
                        (table_name,),
                    )
                    row = helper.fetchone()
                    self.lastrowid = row[0] if row else None
                except Exception:
                    self.lastrowid = None
                finally:
                    helper.close()

            return self

        def fetchone(self):
            if self._manual_results is not None:
                if not self._manual_results:
                    return None
                return self._manual_results.pop(0)
            return self._wrap_row(self._cursor.fetchone())

        def fetchall(self):
            if self._manual_results is not None:
                rows = self._manual_results
                self._manual_results = []
                return rows
            return self._wrap_rows(self._cursor.fetchall())

        def close(self):
            self._cursor.close()


    class ConnectionWrapper:
        def __init__(self, dsn: str):
            self._raw = psycopg.connect(dsn)
            self.row_factory = None

        def cursor(self):
            return CursorWrapper(self)

        def execute(self, sql: str, params=None):
            cursor = self.cursor()
            cursor.execute(sql, params)
            return cursor

        def commit(self):
            self._raw.commit()

        def rollback(self):
            self._raw.rollback()

        def close(self):
            self._raw.close()


    def connect(_database=None, timeout=30, **_kwargs):
        return ConnectionWrapper(DATABASE_URL)
