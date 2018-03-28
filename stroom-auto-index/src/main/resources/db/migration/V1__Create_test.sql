-- JOOQ
CREATE TABLE auto_index_doc_ref (
    uuid                VARCHAR(255) NOT NULL,
    name                VARCHAR(127) NOT NULL,
    updateUser          VARCHAR(255) NOT NULL,
    updateTime          BIGINT UNSIGNED NOT NULL,
    createUser          VARCHAR(255) NOT NULL,
    createTime          BIGINT UNSIGNED NOT NULL,
    raw_type            VARCHAR(255),
    raw_uuid            VARCHAR(255),
    raw_name            VARCHAR(255),
    index_type          VARCHAR(255),
    index_uuid          VARCHAR(255),
    index_name          VARCHAR(255),
    timeField           VARCHAR(255),
    indexWindow         BIGINT UNSIGNED,
    PRIMARY KEY         (uuid)
) ENGINE=InnoDB DEFAULT CHARSET latin1;

CREATE TABLE timeline_bounds (
    docRefUuid      VARCHAR(255) NOT NULL,
    fromValue       BIGINT UNSIGNED NOT NULL,
    toValue         BIGINT UNSIGNED NOT NULL,
    PRIMARY KEY     (docRefUuid)
) ENGINE=InnoDB DEFAULT CHARSET latin1;

CREATE TABLE tracker_window (
    docRefUuid      VARCHAR(255) NOT NULL,
    fromValue       BIGINT UNSIGNED NOT NULL,
    toValue         BIGINT UNSIGNED NOT NULL,
    PRIMARY KEY     (docRefUuid, fromValue)
) ENGINE=InnoDB DEFAULT CHARSET latin1;

CREATE TABLE index_job (
    jobId           VARCHAR(255) NOT NULL,
    docRefUuid      VARCHAR(255) NOT NULL,
    fromValue       BIGINT UNSIGNED NOT NULL,
    toValue         BIGINT UNSIGNED NOT NULL,
    createTime      BIGINT UNSIGNED NOT NULL,
    startedTime     BIGINT UNSIGNED NOT NULL,
    PRIMARY KEY     (jobId)
) ENGINE=InnoDB DEFAULT CHARSET latin1;