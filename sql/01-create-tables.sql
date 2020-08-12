#create database if not exists dandelion charset = utf8mb4;
#use dandelion;

-- # CREATE TABLE #1 (
-- #     `id`  BIGINT(11)  NOT NULL AUTO_INCREMENT,
-- #     PRIMARY KEY (`id`)
-- # )
-- #   AUTO_INCREMENT = 1
-- #   DEFAULT CHARSET = utf8mb4;

CREATE TABLE process_meta
(
    `id`           BIGINT(11) NOT NULL AUTO_INCREMENT,
    `uuid`         CHAR(36)   NOT NULL DEFAULT '',
    `user`         CHAR(32)   NOT NULL DEFAULT '',
    `class`        CHAR(32)   NOT NULL DEFAULT '',
    `data`         TEXT       NOT NULL,
    `rerun`        TINYINT(1) NOT NULL DEFAULT 0,
    `created_at`   TIMESTAMP  NOT NULL DEFAULT NOW(),
    `deleted_at`   TIMESTAMP           DEFAULT NULL,
    `deleted_flag` BIGINT(11) NOT NULL DEFAULT 0, -- TODO not implement
    PRIMARY KEY (`id`),
    KEY `idx_user_class_queue` (`user`, `class`),
    UNIQUE KEY `idx_flow_meta_uuid` (`uuid`, `rerun`)
)
    AUTO_INCREMENT = 1
    DEFAULT CHARSET = utf8mb4;

create table lock_timer
(
    `id`         BIGINT(11) NOT NULL AUTO_INCREMENT,
    `key`        CHAR(48)   NOT NULL DEFAULT '',
    `agent_name` CHAR(16)   NOT NULL DEFAULT '',    -- the last agent to successfully acquire the lock
    `last_seen`  TIMESTAMP  NOT NULL DEFAULT NOW(), -- heartbeat
    PRIMARY KEY (`id`),
    UNIQUE KEY `idx_lock_name` (`key`)
)
    AUTO_INCREMENT = 1
    DEFAULT CHARSET = utf8mb4;


CREATE TABLE process
(
    `id`           BIGINT(11) NOT NULL AUTO_INCREMENT,
    `uuid`         CHAR(36)   NOT NULL DEFAULT '',
    `user`         CHAR(32)   NOT NULL DEFAULT '',
    `class`        CHAR(32)   NOT NULL DEFAULT '',
    `storage`      TEXT       NOT NULL,
    `status`       CHAR(16)   NOT NULL DEFAULT '', -- pending, running, success, failure
    `plan_state`   TEXT       NOT NULL,
    `running_cnt`  INT(11)    NOT NULL DEFAULT 0,
    `started_at`   TIMESTAMP           DEFAULT NULL,
    `ended_at`     TIMESTAMP           DEFAULT NULL,
    `agent_name`   CHAR(16)   NOT NULL DEFAULT '', -- the last agent which executes process
    `created_at`   TIMESTAMP  NOT NULL DEFAULT NOW(),
    `deleted_at`   TIMESTAMP           DEFAULT NULL,
    `deleted_flag` BIGINT(11) NOT NULL DEFAULT 0,  -- TODO not implement
    UNIQUE KEY `idx_flow_uuid` (`uuid`),
    PRIMARY KEY (`id`)
)
    AUTO_INCREMENT = 1
    DEFAULT CHARSET = utf8mb4;

CREATE TABLE process_task
(
    `id`         BIGINT(11) NOT NULL AUTO_INCREMENT,
    `process_id` BIGINT(11) NOT NULL,
    `name`       CHAR(32)   NOT NULL default '',
    `status`     CHAR(16)   NOT NULL DEFAULT '', -- pending, running, success, failure
    `error`      TEXT                DEFAULT NULL,
    `started_at` TIMESTAMP           DEFAULT NULL,
    `ended_at`   TIMESTAMP           DEFAULT NULL,
    `executed`   TINYINT(1) NOT NULL DEFAULT 0,
    UNIQUE KEY `idx_flow_task` (`process_id`, `name`),
    PRIMARY KEY (`id`)
)
    AUTO_INCREMENT = 1
    DEFAULT CHARSET = utf8mb4;

-- CREATE TABLE user_limit (
--     `id`                BIGINT(11)  NOT NULL AUTO_INCREMENT,
--     `user`            CHAR(32) NOT NULL DEFAULT '',
--     `concurrency`   SMALLINT(5) NOT NULL DEFAULT 2,
-- )
--     AUTO_INCREMENT = 1
--     DEFAULT CHARSET = utf8mb4;
-- 
