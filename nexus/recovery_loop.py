import threading
import time

from nexus.recovery_manager import recover_dead_workers


RECOVERY_INTERVAL = 5


def recovery_loop():
    print("[RECOVERY] Recovery manager started.")

    while True:
        try:
            recover_dead_workers()
        except Exception as exc:
            print(
                f"[RECOVERY] Error: {exc}"
            )

        time.sleep(RECOVERY_INTERVAL)


def start_recovery_loop():
    thread = threading.Thread(
        target=recovery_loop,
        daemon=True,
    )

    thread.start()

    return thread