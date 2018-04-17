package stroom.autoindex.indexing;

import stroom.query.api.v2.SearchResponse;

public interface IndexJobHandler {
    SearchResponse search(IndexJob indexJob);
    IndexJob write(IndexJob indexJob, SearchResponse searchResponse);
    IndexJob complete(IndexJob indexJob);
}
