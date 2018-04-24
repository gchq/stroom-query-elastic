-- JOOQ
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
