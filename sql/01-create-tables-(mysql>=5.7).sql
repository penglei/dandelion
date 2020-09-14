#create database if not exists dandelion charset = utf8mb4;
#use dandelion;

-- # CREATE TABLE #1 (
-- #     `id`  BIGINT(11)  NOT NULL AUTO_INCREMENT,
-- #     PRIMARY KEY (`id`)
-- # )
-- #   AUTO_INCREMENT = 1
-- #   DEFAULT CHARSET = utf8mb4;

CREATE TABLE process_trigger
(
    `id`           BIGINT(11) NOT NULL AUTO_INCREMENT,
    `uuid`         CHAR(36)   NOT NULL DEFAULT '',
    `user`         CHAR(32)   NOT NULL DEFAULT '',
    `class`        CHAR(32)   NOT NULL DEFAULT '',
    `data`         TEXT       NOT NULL,
    `event`        CHAR(32)   NOT NULL DEFAULT '',
    `created_at`   TIMESTAMP  NOT NULL DEFAULT NOW(),
    `deleted_at`   TIMESTAMP  NULL,
    `deleted_flag` BIGINT(11) NOT NULL DEFAULT 0,
    PRIMARY KEY (`id`),
    KEY `idx_user_class_queue` (`user`, `class`),
    UNIQUE KEY `idx_unique_process` (`uuid`, `deleted_flag`)
)
    AUTO_INCREMENT = 1
    DEFAULT CHARSET = utf8mb4;

create table lock_timer
(
    `id`         BIGINT(11) NOT NULL AUTO_INCREMENT,
    `key`        CHAR(48)   NOT NULL DEFAULT '',
    `agent_name` CHAR(32)   NOT NULL DEFAULT '',    -- the last agent to successfully acquire the lock
    `last_seen`  TIMESTAMP  NOT NULL DEFAULT NOW(), -- heartbeat
    PRIMARY KEY (`id`),
    UNIQUE KEY `idx_lock_name` (`key`)
)
    AUTO_INCREMENT = 1
    DEFAULT CHARSET = utf8mb4;


CREATE TABLE process
(
    `id`              BIGINT(11) NOT NULL AUTO_INCREMENT,
    `uuid`            CHAR(36)   NOT NULL DEFAULT '',
    `user`            CHAR(32)   NOT NULL DEFAULT '',
    `class`           CHAR(32)   NOT NULL DEFAULT '',
    `latest_event`    CHAR(32)   NOT NULL DEFAULT '',
    `stage_committed` TINYINT(1) NOT NULL DEFAULT 0,
    `storage`         TEXT       NOT NULL,
    `status`          CHAR(16)   NOT NULL DEFAULT '',
    `state`           TEXT       NOT NULL,
    `started_at`      TIMESTAMP  NULL,
    `ended_at`        TIMESTAMP  NULL,
    `agent_name`      CHAR(32)   NOT NULL DEFAULT '', -- the last agent which executes process
    `created_at`      TIMESTAMP  NOT NULL DEFAULT NOW(),
    UNIQUE KEY `idx_unique_uuid` (`uuid`),
    PRIMARY KEY (`id`)
)
    AUTO_INCREMENT = 1
    DEFAULT CHARSET = utf8mb4;

CREATE TABLE process_task
(
    `id`           BIGINT(11)    NOT NULL AUTO_INCREMENT,
    `process_uuid` CHAR(36)      NOT NULL DEFAULT '',
    `name`         CHAR(64)      NOT NULL default '',
    `status`       CHAR(16)      NOT NULL DEFAULT '',
    `err_code`     VARCHAR(32)   NOT NULL DEFAULT '',
    `err_msg`      VARCHAR(8192) NOT NULL DEFAULT '',
    `started_at`   TIMESTAMP     NULL,
    `ended_at`     TIMESTAMP     NULL,
    `created_at`   TIMESTAMP     NOT NULL DEFAULT NOW(),
    UNIQUE KEY `idx_unique_process_task` (`process_uuid`, `name`),
    PRIMARY KEY (`id`)
)
    AUTO_INCREMENT = 1
    DEFAULT CHARSET = utf8mb4;
