package stroom.autoindex.indexing;

import stroom.query.api.v2.SearchResponse;

public final class IndexJobMessages {

    public static SearchIndexJob search(final IndexJob indexJob) {
        return new SearchIndexJob(indexJob);
    }

    public static WriteIndexJob write(final IndexJob indexJob,
                                      final SearchResponse searchResponse) {
        return new WriteIndexJob(indexJob, searchResponse);
    }

    public static CompleteIndexJob complete(final IndexJob indexJob) {
        return new CompleteIndexJob(indexJob);
    }

    private static class IndexJobMessage {
        private final IndexJob indexJob;

        protected IndexJobMessage(final IndexJob indexJob) {
            this.indexJob = indexJob;
        }

        public IndexJob getIndexJob() {
            return indexJob;
        }
    }

    public static class SearchIndexJob extends IndexJobMessage {
        public SearchIndexJob(final IndexJob indexJob) {
            super(indexJob);
        }
    }

    public static class WriteIndexJob extends IndexJobMessage {
        private final SearchResponse searchResponse;

        public WriteIndexJob(final IndexJob indexJob,
                             final SearchResponse searchResponse) {
            super(indexJob);

            this.searchResponse = searchResponse;
        }

        public SearchResponse getSearchResponse() {
            return searchResponse;
        }
    }

    public static class CompleteIndexJob extends IndexJobMessage {
        public CompleteIndexJob(final IndexJob indexJob) {
            super(indexJob);
        }
    }

    private IndexJobMessages() {

    }
}
