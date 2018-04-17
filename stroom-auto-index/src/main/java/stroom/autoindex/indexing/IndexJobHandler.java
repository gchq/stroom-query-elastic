package stroom.autoindex.indexing;

import stroom.query.api.v2.SearchResponse;

/**
 * Index Job's go through a series of steps.
 * Each step is handled in a separate process.
 */
public interface IndexJobHandler {
    SearchResponse search(IndexJob indexJob);
    IndexJob write(IndexJob indexJob, SearchResponse searchResponse);
}
