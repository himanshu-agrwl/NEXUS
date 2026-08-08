import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent


def restart_worker(worker_id):
    print(
        f"[RESTART] Starting {worker_id}..."
    )

    try:
        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "nexus.worker",
            ],
            cwd=PROJECT_ROOT,
        )

        print(
            f"[RESTART] {worker_id} "
            f"started successfully "
            f"(PID {process.pid})."
        )

        return process

    except Exception as exc:

        print(
            f"[RESTART] Failed to start "
            f"{worker_id}: {exc}"
        )

        return None