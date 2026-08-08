import json
import time
from datetime import datetime, timezone, timedelta

from config import (
    WORKER_ID,
    WORKER_POLL_INTERVAL,
    WORKER_LEASE_SECONDS,
    HEARTBEAT_INTERVAL,
)

from database import get_connection


def utc_now():
    return datetime.now(timezone.utc)


def register_worker():
    timestamp = utc_now().isoformat()

    conn = get_connection()

    existing = conn.execute(
        """
        SELECT *
        FROM workers
        WHERE id = ?
        """,
        (WORKER_ID,),
    ).fetchone()

    if existing and existing["state"] == "QUARANTINED":
        conn.close()

        print(
            f"[{WORKER_ID}] "
            f"Worker is QUARANTINED. "
            f"Startup refused."
        )

        raise RuntimeError(
            f"Worker {WORKER_ID} is quarantined."
        )

    conn.execute(
        """
        INSERT INTO workers (
            id,
            state,
            version,
            last_heartbeat,
            created_at,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(id)
        DO UPDATE SET
            state = 'RUNNING',
            last_heartbeat = excluded.last_heartbeat,
            updated_at = excluded.updated_at
        """,
        (
            WORKER_ID,
            "RUNNING",
            "v1",
            timestamp,
            timestamp,
            timestamp,
        ),
    )

    conn.commit()
    conn.close()

    print(
        f"[{WORKER_ID}] Registered with NEXUS."
    )

def send_heartbeat():
    now = utc_now()

    lease_until = (
        now + timedelta(
            seconds=WORKER_LEASE_SECONDS
        )
    ).isoformat()

    conn = get_connection()

    conn.execute(
        """
        UPDATE workers
        SET
            state = 'RUNNING',
            last_heartbeat = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (
            now.isoformat(),
            now.isoformat(),
            WORKER_ID,
        ),
    )

    conn.execute(
        """
        UPDATE jobs
        SET
            lease_until = ?
        WHERE assigned_worker = ?
        AND state = 'PROCESSING'
        """,
        (
            lease_until,
            WORKER_ID,
        ),
    )

    conn.commit()
    conn.close()


def claim_job():
    conn = get_connection()

    try:
        # Prevent two workers from claiming the same job
        # at the same time.
        conn.execute("BEGIN IMMEDIATE")

        job = conn.execute(
            """
            SELECT *
            FROM jobs
            WHERE state = 'QUEUED'
            ORDER BY created_at
            LIMIT 1
            """
        ).fetchone()

        if job is None:
            conn.rollback()
            return None

        attempt_number = job["attempt_count"] + 1
        timestamp = utc_now()

        # Move the job from QUEUED → PROCESSING
        
        lease_until = (
            datetime.now(timezone.utc)
            + timedelta(seconds=WORKER_LEASE_SECONDS)
        ).isoformat()

        conn.execute(
            """
            UPDATE jobs
            SET
                state = 'PROCESSING',
                attempt_count = ?,
                assigned_worker = ?,
                lease_until = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                attempt_number,
                WORKER_ID,
                lease_until,
                timestamp,
                job["id"],
            ),
        )

        # Create an execution attempt
        cursor = conn.execute(
            """
            INSERT INTO attempts (
                job_id,
                worker_id,
                attempt_number,
                started_at,
                status
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                job["id"],
                WORKER_ID,
                attempt_number,
                timestamp,
                "PROCESSING",
            ),
        )

        attempt_id = cursor.lastrowid

        # Record what happened
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
                "JOB_CLAIMED",
                "job",
                job["id"],
                "Worker claimed queued job",
                json.dumps({
                    "worker_id": WORKER_ID,
                    "attempt_number": attempt_number,
                    "attempt_id": attempt_id,
                }),
            ),
        )

        conn.commit()

        return {
            "job_id": job["id"],
            "attempt_id": attempt_id,
            "attempt_number": attempt_number,
            "payload": json.loads(job["payload"]),
        }

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()


def process_job(job):
    print(
        f"[{WORKER_ID}] Processing "
        f"{job['job_id']} "
        f"(attempt {job['attempt_number']})"
    )

    print(
        f"[{WORKER_ID}] Payload: "
        f"{job['payload']}"
    )

    # Temporary simulation of actual work.
    time.sleep(2)


def complete_job(job_id, attempt_id):
    timestamp = utc_now()

    conn = get_connection()

    try:
        conn.execute("BEGIN")

        # PROCESSING → COMPLETED
        conn.execute(
            """
            UPDATE jobs
            SET
                state = 'COMPLETED',
                assigned_worker = NULL,
                lease_until = NULL,
                updated_at = ?
            WHERE id = ?
            AND state = 'PROCESSING'
            """,
            (
                timestamp,
                job_id,
            ),
        )

        # Finish the attempt
        conn.execute(
            """
            UPDATE attempts
            SET
                status = 'COMPLETED',
                finished_at = ?
            WHERE id = ?
            """,
            (
                timestamp,
                attempt_id,
            ),
        )

        # Record completion
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
                "JOB_COMPLETED",
                "job",
                job_id,
                "Worker completed job successfully",
                json.dumps({
                    "worker_id": WORKER_ID,
                    "attempt_id": attempt_id,
                }),
            ),
        )

        conn.commit()

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()


def run_worker():
    print(f"[{WORKER_ID}] Worker started.")

    register_worker()

    last_heartbeat = 0

    while True:

        current_time = time.time()

        if (
            current_time - last_heartbeat
            >= HEARTBEAT_INTERVAL
        ):
            send_heartbeat()
            last_heartbeat = current_time

            print(
                f"[{WORKER_ID}] Heartbeat sent."
            )

        job = claim_job()

        if job is None:
            time.sleep(WORKER_POLL_INTERVAL)
            continue

        try:
            process_job(job)

            complete_job(
                job_id=job["job_id"],
                attempt_id=job["attempt_id"],
            )

            print(
                f"[{WORKER_ID}] Completed "
                f"{job['job_id']}"
            )

        except Exception as exc:
            print(
                f"[{WORKER_ID}] "
                f"Job failed: {exc}"
            )


if __name__ == "__main__":
    run_worker()
