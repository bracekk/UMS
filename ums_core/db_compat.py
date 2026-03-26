import os
import re
import sqlite3 as _sqlite3
from collections.abc import Sequence
from typing import Any

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
USE_POSTGRES = DATABASE_URL.startswith(("postgres://", "postgresql://"))

if USE_POSTGRES:
    import psycopg

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

    def items(self):
        return [(name, self._values[idx]) for idx, name in enumerate(self._columns)]

    def values(self):
        return list(self._values)

    def get(self, key, default=None):
        return self._values[self._index[key]] if key in self._index else default

    def __contains__(self, item):
        return item in self._index

    def __repr__(self) -> str:
        return f"Row({dict(self.items())})"


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
        "numeric": "NUMERIC",
        "real": "REAL",
        "character varying": "TEXT",
        "text": "TEXT",
        "timestamp without time zone": "TIMESTAMP",
        "timestamp with time zone": "TIMESTAMP",
        "date": "DATE",
        "boolean": "BOOLEAN",
    }

    def _convert_type_name(type_name: str) -> str:
        normalized = (type_name or "").lower()
        return SQLITE_TO_POSTGRES_TYPES.get(normalized, (type_name or "TEXT").upper())

    def _normalize_sql(sql: str) -> str:
        translated = sql
        translated = re.sub(
            r"INTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT",
            "BIGSERIAL PRIMARY KEY",
            translated,
            flags=re.IGNORECASE,
        )
        translated = translated.replace("AUTOINCREMENT", "")
        translated = re.sub(r"IFNULL\s*\(", "COALESCE(", translated, flags=re.IGNORECASE)
        translated = translated.replace("?", "%s")
        return translated

    class CursorWrapper:
        def __init__(self, connection: "ConnectionWrapper"):
            self.connection = connection
            self._cursor = connection._raw.cursor()
            self.lastrowid = None
            self._manual_results = None
            self.description = None

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            self.close()

        def _set_manual_results(self, columns: list[str], rows: list[Sequence[Any]]):
            self._manual_results = [
                Row(columns, row) if self.connection.row_factory is Row else tuple(row)
                for row in rows
            ]
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
            lowered = stripped.lower().rstrip(";")

            pragma_match = PRAGMA_TABLE_INFO_QUERY.fullmatch(lowered)
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
                            WHEN column_name = 'id' AND column_default LIKE 'nextval(%' THEN 1
                            ELSE 0
                        END AS pk
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = %s
                    ORDER BY ordinal_position
                    """,
                    (table_name,),
                )
                rows = [
                    (cid, name, _convert_type_name(data_type), notnull, dflt_value, pk)
                    for cid, name, data_type, notnull, dflt_value, pk in self._cursor.fetchall()
                ]
                self._set_manual_results(["cid", "name", "type", "notnull", "dflt_value", "pk"], rows)
                return self

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

            insert_match = re.match(r"\s*INSERT\s+INTO\s+([a-zA-Z_][a-zA-Z0-9_]*)", sql, flags=re.IGNORECASE)
            if insert_match:
                table_name = insert_match.group(1)
                helper = self.connection._raw.cursor()
                try:
                    helper.execute("SELECT currval(pg_get_serial_sequence(%s, 'id'))", (table_name,))
                    row = helper.fetchone()
                    self.lastrowid = row[0] if row else None
                except Exception:
                    self.lastrowid = None
                finally:
                    helper.close()

            return self

        def executemany(self, sql: str, seq_of_params):
            translated_sql = _normalize_sql(sql)
            self._cursor.executemany(translated_sql, seq_of_params)
            self.description = self._cursor.description
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

        def fetchmany(self, size=None):
            if self._manual_results is not None:
                size = size or len(self._manual_results)
                rows = self._manual_results[:size]
                self._manual_results = self._manual_results[size:]
                return rows
            return self._wrap_rows(self._cursor.fetchmany(size))

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

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            if exc_type is None:
                self.commit()
            else:
                self.rollback()
            self.close()

    def connect(_database=None, timeout=30, **_kwargs):
        return ConnectionWrapper(DATABASE_URL)
