import json
from fastapi.responses import FileResponse
from pathlib import Path

from nexus.recovery_loop import start_recovery_loop
from contextlib import asynccontextmanager
from fastapi import FastAPI
from pydantic import BaseModel
from datetime import datetime, timezone

from fastapi import HTTPException
from database import get_connection, initialize_database
from nexus.work_manager import accept_work


class WorkRequest(BaseModel):
    type: str
    payload: dict


@asynccontextmanager
async def lifespan(app: FastAPI):
    initialize_database()

    start_recovery_loop()
    yield


app = FastAPI(
    title="NEXUS",
    description="Explainable Reliability Engine",
    version="0.1.0",
    lifespan=lifespan,
)


@app.post("/work")
def submit_work(work: WorkRequest):
    job_id = accept_work(
        work_type=work.type,
        payload=work.payload,
    )

    return {
        "accepted": True,
        "job_id": job_id,
        "state": "QUEUED",
    }


@app.get("/work")
def list_work():
    conn = get_connection()

    rows = conn.execute(
        """
        SELECT *
        FROM jobs
        ORDER BY created_at DESC
        """
    ).fetchall()

    conn.close()

    return [dict(row) for row in rows]


@app.get("/work/{job_id}")
def get_work(job_id: str):
    conn = get_connection()

    row = conn.execute(
        """
        SELECT *
        FROM jobs
        WHERE id = ?
        """,
        (job_id,),
    ).fetchone()

    conn.close()

    if row is None:
        return {
            "error": "Job not found"
        }

    return dict(row)



@app.get("/events")
def list_events():
    conn = get_connection()

    rows = conn.execute(
        """
        SELECT *
        FROM events
        ORDER BY id ASC
        """
    ).fetchall()

    conn.close()

    return [dict(row) for row in rows]


@app.post("/workers/{worker_id}/unquarantine")
def unquarantine_worker(worker_id: str):
    conn = get_connection()

    worker = conn.execute(
        """
        SELECT *
        FROM workers
        WHERE id = ?
        """,
        (worker_id,),
    ).fetchone()

    if worker is None:
        conn.close()

        raise HTTPException(
            status_code=404,
            detail="Worker not found",
        )

    if worker["state"] != "QUARANTINED":
        conn.close()

        return {
            "worker_id": worker_id,
            "state": worker["state"],
            "message": "Worker is not quarantined",
        }

    timestamp = datetime.now(
        timezone.utc
    ).isoformat()

    conn.execute(
        """
        UPDATE workers
        SET
            state = 'RECOVERING',
            restart_count = 0,
            restart_window_start = NULL,
            last_failure = NULL,
            updated_at = ?
        WHERE id = ?
        """,
        (
            timestamp,
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
            timestamp,
            "WORKER_UNQUARANTINED",
            "worker",
            worker_id,
            "Operator approved worker recovery",
            json.dumps({
                "worker_id": worker_id,
            }),
        ),
    )

    conn.commit()
    conn.close()

    return {
        "worker_id": worker_id,
        "state": "RECOVERING",
        "message": "Worker quarantine removed",
    }

@app.get("/")
def dashboard():
    return FileResponse(
        Path(__file__).parent / "frontend" / "index.html"
    )



@app.get("/workers")
def get_workers():
    conn = get_connection()

    rows = conn.execute(
        """
        SELECT *
        FROM workers
        ORDER BY id
        """
    ).fetchall()

    conn.close()

    return [
        dict(row)
        for row in rows
    ]