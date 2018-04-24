package stroom.autoindex.search;

import stroom.query.api.v2.DocRef;
import stroom.query.api.v2.SearchRequest;
import stroom.security.ServiceUser;

public interface AutoIndexMessages {

    static SplitSearchJobComplete complete(final ServiceUser user,
                                           final SearchRequest originalSearchRequest,
                                           final DocRef docRef,
                                           final SplitSearchRequest splitSearchRequest) {
        return new SplitSearchJobComplete(user, originalSearchRequest, docRef, splitSearchRequest, null);
    }

    static SplitSearchJobComplete failed(final ServiceUser user,
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
}
