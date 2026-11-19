-- Run once before deploying the updated API; retain a database backup.
RENAME TABLE elastic_jobs TO jobs;
ALTER TABLE jobs
 ADD COLUMN job_type VARCHAR(512) NULL,
 ADD COLUMN platform_type VARCHAR(512) NULL,
 ADD COLUMN platform_task_id VARCHAR(512) NULL,
 ADD COLUMN elasticity_enabled BOOLEAN NULL;
-- Existing classification remains unknown until authoritative snapshots arrive.
