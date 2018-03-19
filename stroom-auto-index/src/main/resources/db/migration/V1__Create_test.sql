-- JOOQ
CREATE TABLE auto_index_doc_ref (
    uuid            VARCHAR(255) NOT NULL,
    name            VARCHAR(127) NOT NULL,
    updateUser      VARCHAR(255) NOT NULL,
    updateTime      BIGINT UNSIGNED NOT NULL,
    createUser      VARCHAR(255) NOT NULL,
    createTime      BIGINT UNSIGNED NOT NULL,
    raw_type        VARCHAR(255),
    raw_uuid        VARCHAR(255),
    raw_name        VARCHAR(255),
    index_type      VARCHAR(255),
    index_uuid      VARCHAR(255),
    index_name      VARCHAR(255),
    PRIMARY KEY     (uuid)
) ENGINE=InnoDB DEFAULT CHARSET latin1;