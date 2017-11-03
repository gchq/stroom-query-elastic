-- Elastic Search Indexing
CREATE TABLE ELASTIC_INDEX (
  UUID              varchar(255) NOT NULL,
  INDEX_NAME 	    varchar(255),
  INDEXED_TYPE	    varchar(255),
  PRIMARY KEY       (UUID)
);

INSERT INTO ELASTIC_INDEX
    (UUID, INDEX_NAME, INDEXED_TYPE)
VALUES
    ("36b4aa5c-79b6-4735-b3e4-a87deb0fd808", "shakespeare", "line");