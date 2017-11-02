-- Elastic Search Indexing
CREATE TABLE ELASTIC_INDEX (
  UUID              varchar(255) NOT NULL,
  INDEX_NAME 	    varchar(255),
  INDEXED_TYPE	    varchar(255),
  PRIMARY KEY       (UUID)
);

TRUNCATE ELASTIC_INDEX;