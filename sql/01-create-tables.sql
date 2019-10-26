create database if not exists tke_mesh charset = utf8mb4;

use tke_mesh;

-- # CREATE TABLE #1 (
-- #     `id`  BIGINT(11)  NOT NULL AUTO_INCREMENT,
-- #     PRIMARY KEY (`id`)
-- # )
-- #   AUTO_INCREMENT = 1
-- #   DEFAULT CHARSET = utf8mb4;

CREATE TABLE job_event (
    `id`            BIGINT(11)  NOT NULL AUTO_INCREMENT,
    `uuid`          CHAR(36) NOT NULL DEFAULT '',
    `userid`        CHAR(32) NOT NULL DEFAULT '',
    `class`         CHAR(32) NOT NULL DEFAULT '',
    `data`          TEXT NOT NULL,
    `created_at`    TIMESTAMP NOT NULL DEFAULT NOW(),
    `deleted_at`    TIMESTAMP DEFAULT NULL,
    `deleted_flag`  BIGINT(11)  NOT NULL DEFAULT 0, -- TODO not implement
    PRIMARY KEY (`id`),
    KEY `idx_user_class_queue` (`userid`, `class`),
    UNIQUE KEY `idx_job_uuid` (`uuid`)
)
    AUTO_INCREMENT = 1
    DEFAULT CHARSET = utf8mb4;

create table lock_timer (
    `id`                BIGINT(11)  NOT NULL AUTO_INCREMENT,
    `key`               CHAR(48) NOT NULL DEFAULT '',
    `agent_name`        CHAR(16) NOT NULL DEFAULT '', -- the last agent to successfully acquire the lock
    `last_seen`         TIMESTAMP NOT NULL DEFAULT NOW(), -- heartbeat
    PRIMARY KEY (`id`),
    UNIQUE KEY `idx_lock_name` (`key`)
)
    AUTO_INCREMENT = 1
    DEFAULT CHARSET = utf8mb4;


CREATE TABLE flow (
    `id`                BIGINT(11)  NOT NULL AUTO_INCREMENT,
    `event_uuid`        CHAR(36) NOT NULL DEFAULT '',
    `userid`            CHAR(32) NOT NULL DEFAULT '',
    `class`             CHAR(32) NOT NULL DEFAULT '',
    `storage`           TEXT NOT NULL,
    `status`            CHAR(16) NOT NULL DEFAULT '',  -- pending, running, success, failure
    `state`             TEXT NOT NULL,
    -- `job_created_at`    TIMESTAMP NOT NULL, -- TODO
    `running_cnt`       INT(11) NOT NULL DEFAULT 0,
    `started_at`        TIMESTAMP DEFAULT NULL,
    `ended_at`          TIMESTAMP DEFAULT NULL,
    `agent_name`        CHAR(16) NOT NULL DEFAULT '', -- the last agent which executes flow task
    `created_at`        TIMESTAMP NOT NULL DEFAULT NOW(),
    `updated_at`        TIMESTAMP NOT NULL DEFAULT now() ON UPDATE NOW(),
    `deleted_at`        TIMESTAMP DEFAULT NULL,
    `deleted_flag`      BIGINT(11)  NOT NULL DEFAULT 0,
    UNIQUE KEY `idx_job_event_uuid` (`event_uuid`),
    PRIMARY KEY (`id`)
)
    AUTO_INCREMENT = 1
    DEFAULT CHARSET = utf8mb4;

CREATE TABLE flow_task (
    `id`                BIGINT(11)  NOT NULL AUTO_INCREMENT,
    `flow_id`           BIGINT(11)  NOT NULL,
    `name`              CHAR(32) NOT NULL default '',
    `status`            CHAR(16) NOT NULL DEFAULT '',  -- pending, running, success, failure
    `error_msg`         TEXT DEFAULT NULL,
    `started_at`        TIMESTAMP DEFAULT NULL,
    `ended_at`          TIMESTAMP DEFAULT NULL,
    `executed`          TINYINT(1) NOT NULL DEFAULT 0,
    UNIQUE KEY `idx_flow_task` (`flow_id`, `name`),
    PRIMARY KEY (`id`)
)
    AUTO_INCREMENT = 1
    DEFAULT CHARSET = utf8mb4;

-- CREATE TABLE job_user_limit (
--     `id`                BIGINT(11)  NOT NULL AUTO_INCREMENT,
--     `userid`            CHAR(32) NOT NULL DEFAULT '',
--     `job_concurrency`   SMALLINT(5) NOT NULL DEFAULT 2,
-- )
--     AUTO_INCREMENT = 1
--     DEFAULT CHARSET = utf8mb4;
-- 
