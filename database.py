import sqlite3
from pathlib import Path


DB_PATH = Path("data/nexus.db")


def get_connection():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(
        DB_PATH,
        timeout=10          
                           )
    conn.row_factory = sqlite3.Row

    # Helps SQLite handle concurrent access from NEXUS and workers.
    conn.execute("PRAGMA journal_mode=WAL")

    return conn


def initialize_database():
    conn = get_connection()

    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY,
            type TEXT NOT NULL,
            payload TEXT NOT NULL,
            state TEXT NOT NULL,

            attempt_count INTEGER NOT NULL DEFAULT 0,
            max_attempts INTEGER NOT NULL DEFAULT 5,

            assigned_worker TEXT,
            lease_until TEXT,

            last_error TEXT,

            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS attempts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,

            job_id TEXT NOT NULL,
            worker_id TEXT NOT NULL,

            attempt_number INTEGER NOT NULL,

            started_at TEXT NOT NULL,
            finished_at TEXT,

            status TEXT NOT NULL,

            error TEXT,
            duplicate INTEGER NOT NULL DEFAULT 0,

            FOREIGN KEY (job_id) REFERENCES jobs(id)
        );

        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,

            timestamp TEXT NOT NULL,

            event_type TEXT NOT NULL,

            entity_type TEXT NOT NULL,
            entity_id TEXT NOT NULL,

            reason TEXT,

            metadata TEXT
        );

        CREATE TABLE IF NOT EXISTS workers (
            id TEXT PRIMARY KEY,

            state TEXT NOT NULL,

            version TEXT NOT NULL DEFAULT 'v1',

            last_heartbeat TEXT,

            restart_count INTEGER NOT NULL DEFAULT 0,

            last_failure TEXT,
            last_success TEXT,

            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        """
    )

    # Existing databases created before lease support
    # may not have this column.
    existing_columns = conn.execute(
        "PRAGMA table_info(jobs)"
    ).fetchall()

    column_names = {
        column["name"]
        for column in existing_columns
    }


    if "lease_until" not in column_names:
        conn.execute(
            "ALTER TABLE jobs ADD COLUMN lease_until TEXT"
        )

    if "next_attempt_at" not in column_names:
        conn.execute(
            "ALTER TABLE jobs ADD COLUMN next_attempt_at TEXT"
    )

    existing_worker_columns = conn.execute(
        "PRAGMA table_info(workers)"
    ).fetchall()


    worker_column_names = {
        column["name"]
        for column in existing_worker_columns
    }

    if "restart_window_start" not in worker_column_names:
        conn.execute(
            """
            ALTER TABLE workers
            ADD COLUMN restart_window_start TEXT
            """
        )

    conn.commit()
    conn.close()