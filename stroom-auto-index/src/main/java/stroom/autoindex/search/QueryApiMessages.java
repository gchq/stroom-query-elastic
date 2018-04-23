package stroom.autoindex.search;

import stroom.datasource.api.v2.DataSource;
import stroom.query.api.v2.DocRef;
import stroom.query.api.v2.SearchRequest;
import stroom.query.api.v2.SearchResponse;
import stroom.security.ServiceUser;

public interface QueryApiMessages {

    class JobComplete<REQUEST, RESPONSE> {
        private final REQUEST request;
        private final RESPONSE response;
        private final String error;

        private JobComplete(final REQUEST request,
                            final RESPONSE response,
                            final String error) {
            this.request = request;
            this.response = response;
            this.error = error;
        }

        public REQUEST getRequest() {
            return request;
        }

        public RESPONSE getResponse() {
            return response;
        }

        public String getError() {
            return error;
        }
    }

    static SearchError error(final String msg) {
        return new SearchError(msg);
    }

    class SearchError {
        private final String msg;

        private SearchError(final String msg) {
            this.msg = msg;
        }

        public String getMsg() {
            return msg;
        }
    }

    static DataSourceJob dataSource(final ServiceUser user, final DocRef docRef) {
        return new DataSourceJob(user, docRef);
    }

    class DataSourceJob {
        private final ServiceUser user;
        private final DocRef docRef;

        public DataSourceJob(final ServiceUser user,
                             final DocRef docRef) {
            this.user = user;
            this.docRef = docRef;
        }

        public ServiceUser getUser() {
            return user;
        }

        public DocRef getDocRef() {
            return docRef;
        }
    }

    static DataSourceJobComplete complete(final DataSourceJob request,
                                          final DataSource response) {
        return new DataSourceJobComplete(request, response, null);
    }

    static DataSourceJobComplete failed(final DataSourceJob request,
                                        final Throwable exception) {
        return new DataSourceJobComplete(request, null, exception.getLocalizedMessage());
    }

    class DataSourceJobComplete extends JobComplete<DataSourceJob, DataSource> {
        private DataSourceJobComplete(final DataSourceJob request,
                                      final DataSource response,
                                      final String error) {
            super(request, response, error);
        }
    }

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

    static SearchJobComplete complete(final SearchJob searchJob,
                                      final SearchResponse response) {
        return new SearchJobComplete(searchJob, response, null);
    }

    static SearchJobComplete failed(final SearchJob searchJob,
                                    final Throwable exception) {
        return new SearchJobComplete(searchJob, null, exception.getLocalizedMessage());
    }

    class SearchJobComplete extends JobComplete<SearchJob, SearchResponse> {
        private SearchJobComplete(final SearchJob request,
                                  final SearchResponse response,
                                  final String error) {
            super(request, response, error);
        }
    }
}
