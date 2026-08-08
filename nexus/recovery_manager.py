import json
from datetime import datetime, timezone, timedelta
from nexus.restart_controller import restart_worker

from database import get_connection
from config import (
    WORKER_DEAD_THRESHOLD_SECONDS,
    MAX_JOB_ATTEMPTS,
    RETRY_BASE_DELAY,
    RETRY_MAX_DELAY,
    MAX_WORKER_RESTARTS,
    WORKER_RESTART_WINDOW,
)

from config import (
    WORKER_DEAD_THRESHOLD_SECONDS,
    MAX_JOB_ATTEMPTS,
    RETRY_BASE_DELAY,
    RETRY_MAX_DELAY,
)


def utc_now():
    return datetime.now(timezone.utc)


def schedule_retry(
    conn,
    job,
    attempt,
    worker_id,
    now,
):
    """
    Decide whether a failed job should be retried
    or moved permanently to DEAD_LETTER.
    """

    attempt_number = job["attempt_count"]

    # ---------------------------------------------------------
    # Retry budget exhausted
    # ---------------------------------------------------------

    if attempt_number >= MAX_JOB_ATTEMPTS:

        conn.execute(
            """
            UPDATE jobs
            SET
                state = 'DEAD_LETTER',
                assigned_worker = NULL,
                lease_until = NULL,
                next_attempt_at = NULL,
                last_error = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                "Maximum retry attempts exhausted",
                now.isoformat(),
                job["id"],
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
                now.isoformat(),
                "DEAD_LETTERED",
                "job",
                job["id"],
                "Maximum retry attempts exhausted",
                json.dumps({
                    "attempt_count": attempt_number,
                    "max_attempts": MAX_JOB_ATTEMPTS,
                    "worker_id": worker_id,
                }),
            ),
        )

        print(
            f"[RECOVERY] Job {job['id']} "
            f"moved to DEAD_LETTER."
        )

        return

    # ---------------------------------------------------------
    # Calculate exponential backoff
    # ---------------------------------------------------------

    delay = min(
        RETRY_BASE_DELAY * (
            2 ** (attempt_number - 1)
        ),
        RETRY_MAX_DELAY,
    )

    retry_at = (
        now + timedelta(seconds=delay)
    )

    # ---------------------------------------------------------
    # Schedule retry
    # ---------------------------------------------------------

    conn.execute(
        """
        UPDATE jobs
        SET
            state = 'RETRY_SCHEDULED',
            assigned_worker = NULL,
            lease_until = NULL,
            next_attempt_at = ?,
            last_error = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (
            retry_at.isoformat(),
            "Worker lease expired",
            now.isoformat(),
            job["id"],
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
            now.isoformat(),
            "RETRY_SCHEDULED",
            "job",
            job["id"],
            "Retry budget remains",
            json.dumps({
                "attempt_number": attempt_number,
                "retry_delay_seconds": delay,
                "next_attempt_at":
                    retry_at.isoformat(),
                "worker_id": worker_id,
            }),
        ),
    )

    print(
        f"[RECOVERY] Job {job['id']} "
        f"retry scheduled in {delay}s."
    )


def handle_worker_failure(
    conn,
    worker,
    now,
):
    worker_id = worker["id"]

    current_restart_count = worker["restart_count"] or 0
    window_start = worker["restart_window_start"]

    # ---------------------------------------------------------
    # Start a new restart window if necessary
    # ---------------------------------------------------------

    if window_start:

        window_time = datetime.fromisoformat(
            window_start
        )

        window_age = (
            now - window_time
        ).total_seconds()

        if window_age >= WORKER_RESTART_WINDOW:
            current_restart_count = 0
            window_start = None

    # ---------------------------------------------------------
    # Start restart window
    # ---------------------------------------------------------

    if window_start is None:

        window_start = now.isoformat()
        current_restart_count = 0

    # ---------------------------------------------------------
    # Consume one restart budget
    # ---------------------------------------------------------

    current_restart_count += 1

    # ---------------------------------------------------------
    # Budget exhausted → QUARANTINE
    # ---------------------------------------------------------

    if current_restart_count > MAX_WORKER_RESTARTS:

        conn.execute(
            """
            UPDATE workers
            SET
                state = 'QUARANTINED',
                restart_count = ?,
                restart_window_start = ?,
                last_failure = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                current_restart_count,
                window_start,
                now.isoformat(),
                now.isoformat(),
                worker_id,
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
                now.isoformat(),
                "WORKER_QUARANTINED",
                "worker",
                worker_id,
                "Worker restart budget exhausted",
                json.dumps({
                    "restart_count":
                        current_restart_count,
                    "max_restarts":
                        MAX_WORKER_RESTARTS,
                    "window_seconds":
                        WORKER_RESTART_WINDOW,
                }),
            ),
        )

        print(
            f"[RECOVERY] Worker {worker_id} "
            f"QUARANTINED."
        )

        return False

    # ---------------------------------------------------------
    # Restart is allowed
    # ---------------------------------------------------------

    conn.execute(
        """
        UPDATE workers
        SET
            state = 'RECOVERING',
            restart_count = ?,
            restart_window_start = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (
            current_restart_count,
            window_start,
            now.isoformat(),
            worker_id,
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
            now.isoformat(),
            "WORKER_RESTART_ALLOWED",
            "worker",
            worker_id,
            "Worker restart budget available",
            json.dumps({
                "restart_count":
                    current_restart_count,
                "max_restarts":
                    MAX_WORKER_RESTARTS,
                "window_seconds":
                    WORKER_RESTART_WINDOW,
            }),
        ),
    )

    print(
        f"[RECOVERY] Worker {worker_id} "
        f"restart allowed "
        f"({current_restart_count}/"
        f"{MAX_WORKER_RESTARTS})."
    )

    return True


def recover_dead_workers():

    now = utc_now()

    conn = get_connection()
    workers_to_restart = []

    try:

        workers = conn.execute(
            """
            SELECT *
            FROM workers
            WHERE state = 'RUNNING'
            """
        ).fetchall()

        for worker in workers:

            if not worker["last_heartbeat"]:
                continue

            heartbeat = datetime.fromisoformat(
                worker["last_heartbeat"]
            )

            age = (
                now - heartbeat
            ).total_seconds()

            if age <= WORKER_DEAD_THRESHOLD_SECONDS:
                continue

            worker_id = worker["id"]

            print(
                f"[RECOVERY] Worker {worker_id} "
                f"appears dead. Last heartbeat "
                f"{age:.1f}s ago."
            )

            restart_allowed = handle_worker_failure(
                conn=conn,
                worker=worker,
                now=now,
            )

            if restart_allowed:
                workers_to_restart.append(worker_id)
                

        
            
            # -------------------------------------------------
            # Find jobs owned by worker
            # -------------------------------------------------

            jobs = conn.execute(
                """
                SELECT *
                FROM jobs
                WHERE state = 'PROCESSING'
                AND assigned_worker = ?
                """,
                (worker_id,),
            ).fetchall()

            for job in jobs:

                # -------------------------------------------------
                # Find active attempt
                # -------------------------------------------------

                attempt = conn.execute(
                    """
                    SELECT *
                    FROM attempts
                    WHERE job_id = ?
                    AND worker_id = ?
                    AND status = 'PROCESSING'
                    ORDER BY attempt_number DESC
                    LIMIT 1
                    """,
                    (
                        job["id"],
                        worker_id,
                    ),
                ).fetchone()

                # -------------------------------------------------
                # Mark attempt failed
                # -------------------------------------------------

                if attempt:

                    conn.execute(
                        """
                        UPDATE attempts
                        SET
                            status = 'FAILED',
                            finished_at = ?,
                            error = ?
                        WHERE id = ?
                        """,
                        (
                            now.isoformat(),
                            "Worker lease expired",
                            attempt["id"],
                        ),
                    )

                # -------------------------------------------------
                # Record worker lost
                # -------------------------------------------------

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
                        now.isoformat(),
                        "WORKER_LOST",
                        "worker",
                        worker_id,
                        "Worker heartbeat lease expired",
                        json.dumps({
                            "worker_id": worker_id,
                            "job_id": job["id"],
                            "heartbeat_age_seconds": age,
                            "attempt_id":
                                attempt["id"]
                                if attempt
                                else None,
                        }),
                    ),
                )

                # -------------------------------------------------
                # Decide retry vs dead-letter
                # -------------------------------------------------

                schedule_retry(
                    conn=conn,
                    job=job,
                    attempt=attempt,
                    worker_id=worker_id,
                    now=now,
                )

        # ---------------------------------------------------------
        # Move scheduled retries back to QUEUED
        # ---------------------------------------------------------

        retry_jobs = conn.execute(
            """
            SELECT *
            FROM jobs
            WHERE state = 'RETRY_SCHEDULED'
            AND next_attempt_at IS NOT NULL
            AND next_attempt_at <= ?
            """,
            (now.isoformat(),),
        ).fetchall()

        for job in retry_jobs:

            conn.execute(
                """
                UPDATE jobs
                SET
                    state = 'QUEUED',
                    next_attempt_at = NULL,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    now.isoformat(),
                    job["id"],
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
                    now.isoformat(),
                    "RETRY_READY",
                    "job",
                    job["id"],
                    "Retry delay elapsed",
                    json.dumps({
                        "attempt_count":
                            job["attempt_count"],
                    }),
                ),
            )

            print(
                f"[RECOVERY] Job {job['id']} "
                f"is ready for retry."
            )

        conn.commit()
        for worker_id in workers_to_restart:
            restart_worker(worker_id)

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()