import json
import uuid
from datetime import datetime, timezone

from database import get_connection


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def accept_work(work_type: str, payload: dict):
    job_id = str(uuid.uuid4())
    timestamp = utc_now()

    conn = get_connection()

    try:
        conn.execute("BEGIN")

        conn.execute(
            """
            INSERT INTO jobs (
                id,
                type,
                payload,
                state,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                job_id,
                work_type,
                json.dumps(payload),
                "QUEUED",
                timestamp,
                timestamp,
            ),
        )

        conn.execute(
            """
            INSERT INTO events (
                timestamp,
                event_type,
                entity_type,
                entity_id,
                reason,
                metadata
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                timestamp,
                "WORK_ACCEPTED",
                "job",
                job_id,
                "Work persisted successfully",
                None,
            ),
        )

        conn.commit()

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()

    return job_id