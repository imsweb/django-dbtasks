## 0.5.0 (TBD)

* Catch exceptions during task scheduling (not just task execution), and retry with a backoff and a fresh database connection
* Added a `Worker` model for current task runners/workers, along with a heartbeat mechanism
    * Prevent multiple runners from using the same `worker_id` concurrently
* Restart orphaned tasks left in the RUNNING state if a runner crashes
