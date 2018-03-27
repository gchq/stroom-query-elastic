package stroom.autoindex.indexing;

import stroom.datasource.api.v2.DataSource;
import stroom.query.api.v2.DocRef;
import stroom.query.api.v2.SearchResponse;

public interface IndexWriter {
    void writeResults(DocRef elasticDocRef, DataSource dataSource, SearchResponse response);
}
