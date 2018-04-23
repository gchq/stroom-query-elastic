package stroom.autoindex.search;

import stroom.query.api.v2.DocRef;
import stroom.query.api.v2.SearchRequest;
import stroom.query.api.v2.SearchResponse;
import stroom.security.ServiceUser;

public interface SearchMessages {

    static SearchJob search(final ServiceUser user,
                             final String docRefType,
                             final SearchRequest request) {
        return new SearchJob(user, docRefType, request);
    }

    class SearchJob {
        private final ServiceUser user;
        private final String docRefType;
        private final SearchRequest request;

        private SearchJob(final ServiceUser user,
                          final String docRefType,
                          final SearchRequest request) {
            this.user = user;
            this.docRefType = docRefType;
            this.request = request;
        }

        public ServiceUser getUser() {
            return user;
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
            sb.append("user=").append(user);
            sb.append(", docRefType='").append(docRefType).append('\'');
            sb.append(", request=").append(request);
            sb.append('}');
            return sb.toString();
        }
    }

    static SplitSearchJobComplete splitComplete(final ServiceUser user,
                                                final SearchRequest originalSearchRequest,
                                                final DocRef docRef,
                                                final SplitSearchRequest splitSearchRequest) {
        return new SplitSearchJobComplete(user, originalSearchRequest, docRef, splitSearchRequest, null);
    }

    static SplitSearchJobComplete splitFailed(final ServiceUser user,
                                              final SearchRequest originalSearchRequest,
                                              final Throwable exception) {
        return new SplitSearchJobComplete(user, originalSearchRequest, null, null, exception.getLocalizedMessage());
    }

    class SplitSearchJobComplete {
        private final ServiceUser user;
        private final SearchRequest originalSearchRequest;
        private final DocRef docRef;
        private final SplitSearchRequest splitSearchRequest;
        private final String error;

        private SplitSearchJobComplete(final ServiceUser user,
                                       final SearchRequest originalSearchRequest,
                                       final DocRef docRef,
                                       final SplitSearchRequest splitSearchRequest,
                                       final String error) {
            this.user = user;
            this.originalSearchRequest = originalSearchRequest;
            this.docRef = docRef;
            this.splitSearchRequest = splitSearchRequest;
            this.error = error;
        }

        public ServiceUser getUser() {
            return user;
        }

        public SearchRequest getOriginalSearchRequest() {
            return originalSearchRequest;
        }

        public DocRef getDocRef() {
            return docRef;
        }

        public SplitSearchRequest getSplitSearchRequest() {
            return splitSearchRequest;
        }

        public String getError() {
            return error;
        }
    }

    static SearchJobComplete searchComplete(final ServiceUser user,
                                            final SearchRequest request,
                                            final SearchResponse response) {
        return new SearchJobComplete(user, request, response, null);
    }

    static SearchJobComplete searchFailed(final ServiceUser user,
                                          final SearchRequest request,
                                          final Throwable exception) {
        return new SearchJobComplete(user, request, null, exception.getLocalizedMessage());
    }

    class SearchJobComplete {
        private final ServiceUser user;
        private final SearchRequest request;
        private final SearchResponse response;
        private final String error;

        private SearchJobComplete(final ServiceUser user,
                                  final SearchRequest request,
                                  final SearchResponse response,
                                  final String error) {
            this.user = user;
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
            sb.append("user=").append(user);
            sb.append(", request=").append(request);
            sb.append(", response=").append(response);
            sb.append(", error='").append(error).append('\'');
            sb.append('}');
            return sb.toString();
        }
    }
}
