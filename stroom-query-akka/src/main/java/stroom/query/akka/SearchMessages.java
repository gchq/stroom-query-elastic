package stroom.query.akka;

import stroom.query.api.v2.SearchRequest;
import stroom.query.api.v2.SearchResponse;

public interface SearchMessages {

    class SearchAndIndexJob {
        private final String docRefType;
        private final SearchRequest request;

        public SearchAndIndexJob(final String docRefType,
                                 final SearchRequest request) {
            this.docRefType = docRefType;
            this.request = request;
        }

        public String getDocRefType() {
            return docRefType;
        }

        public SearchRequest getRequest() {
            return request;
        }
    }

    class SearchJob {
        private final String docRefType;
        private final SearchRequest request;

        public SearchJob(final String docRefType,
                         final SearchRequest request) {
            this.docRefType = docRefType;
            this.request = request;
        }

        public String getDocRefType() {
            return docRefType;
        }

        public SearchRequest getRequest() {
            return request;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("SearchJob{");
            sb.append("docRefType='").append(docRefType).append('\'');
            sb.append(", request=").append(request);
            sb.append('}');
            return sb.toString();
        }
    }

    class SearchJobComplete {
        private final SearchRequest request;
        private final SearchResponse response;
        private final String error;


        public SearchJobComplete(final SearchRequest request,
                                 final SearchResponse response) {
            this(request, response, null);
        }


        public SearchJobComplete(final SearchRequest request,
                                 final Throwable error) {
            this(request, null, error.getLocalizedMessage());
        }

        private SearchJobComplete(final SearchRequest request,
                                 final SearchResponse response,
                                 final String error) {
            this.request = request;
            this.response = response;
            this.error = error;
        }

        public SearchRequest getRequest() {
            return request;
        }

        public SearchResponse getResponse() {
            return response;
        }

        public String getError() {
            return error;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("SearchJobComplete{");
            sb.append("request=").append(request);
            sb.append(", response=").append(response);
            sb.append(", error='").append(error).append('\'');
            sb.append('}');
            return sb.toString();
        }
    }

    class IndexJob {
        private final SearchResponse response;

        public IndexJob(final SearchResponse response) {
            this.response = response;
        }

        public SearchResponse getResponse() {
            return response;
        }
    }

    class IndexComplete {

    }
}
