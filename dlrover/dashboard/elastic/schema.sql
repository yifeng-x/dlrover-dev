-- MySQL 8.x / InnoDB. Run once using `python -m dlrover.dashboard.elastic init-db`.
-- Application queries need SELECT, INSERT, UPDATE, not DDL privileges.
CREATE TABLE IF NOT EXISTS elastic_jobs (
    uid VARCHAR(512) COLLATE utf8mb4_bin NOT NULL PRIMARY KEY,
    name VARCHAR(512) NOT NULL,
    cluster VARCHAR(512) COLLATE utf8mb4_bin NOT NULL,
    namespace VARCHAR(512) COLLATE utf8mb4_bin NOT NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'unknown',
    submitted_at DOUBLE NULL,
    completed_at DOUBLE NULL,
    lifecycle_observed_at DOUBLE NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS elastic_events (
    seq BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    event_id VARCHAR(512) COLLATE utf8mb4_bin NOT NULL,
    job_uid VARCHAR(512) COLLATE utf8mb4_bin NOT NULL,
    occurred_at DOUBLE NOT NULL,
    payload LONGTEXT NOT NULL,
    UNIQUE KEY uq_elastic_event_id (event_id),
    KEY ix_elastic_event_job_time (job_uid, occurred_at, seq),
    CONSTRAINT fk_elastic_event_job FOREIGN KEY (job_uid) REFERENCES elastic_jobs(uid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
