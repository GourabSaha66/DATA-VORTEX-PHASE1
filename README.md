# Data Vortex '26 — Phase 1: Social Engine System Recovery

## Project Architecture & Infrastructure
This repository contains the production-grade data engineering pipeline and detailed exploratory data analysis (EDA) for **Data Vortex (Aaruush '26) Phase 1**.

Following a critical failure of database microservices (database.service code 137, live_signal carrier loss), raw corrupted data payloads were extracted directly from recovery node `node_07`. This pipeline restores 100% data integrity across entity streams.

### Core Pipeline Capabilities
* **Inbound Payload Ingestion:** Vectorized loading of raw dimension (`Social_Engine_Users.csv`) and fact logs (`Social_Engine_Posts_Corrupted.csv`).
* **Packet Deduplication:** Primary key integrity enforcement removing re-transmission loops across `user_id` and `post_id`.
* **Telemetry Anomaly Correction:** Reversal of bit-flipped negative engagement metrics ($|likes|$) caused by process memory corruption.
* **Temporal Normalization:** Fallback-based datetime parser handling Unix epoch seconds, ISO 8601 strings, and European standard date strings into unified `YYYY-MM-DD HH:MM:SS`.
* **Imputation Engine:** Platform-grouped median imputation preserving empirical distribution characteristics without introducing artificial variance skew.

---

## Workspace Setup & Execution

### 1. Install Dependencies
```bash
pip install -r requirements.txt