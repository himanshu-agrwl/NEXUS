# NEXUS — Autonomous Reliability Control Plane

NEXUS is a reliability control plane for background job-processing systems.

It is designed to handle a critical distributed-systems failure scenario: a worker may perform work successfully but crash before acknowledging completion, causing the same job to be delivered again.

Instead of blindly retrying failed jobs, NEXUS independently monitors **job health** and **worker health** and makes bounded recovery decisions.

---

## Problem

A background worker can fail at an unsafe point in its execution:

```text
Job received
    ↓
Business operation executed
    ↓
Worker crashes
    ↓
Completion acknowledgement never happens
    ↓
Job appears unfinished
    ↓
Job is delivered again
    ↓
Duplicate execution
