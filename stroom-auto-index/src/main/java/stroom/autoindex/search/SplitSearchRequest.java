package stroom.autoindex.search;

import stroom.query.api.v2.DocRef;
import stroom.query.api.v2.SearchRequest;
import stroom.tracking.TrackerWindow;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This is the output from the Search Request Splitter.
 * It encapsulates the multiple search requests that must be sent to different underlying stores.
 */
public class SplitSearchRequest {
    private final Map<DocRef, Map<TrackerWindow, SearchRequest>> requests;

    private SplitSearchRequest(final Builder builder) {
        this.requests = builder.requests;
    }

    public Map<DocRef, Map<TrackerWindow, SearchRequest>> getRequests() {
        return requests;
    }

    public static Builder start() {
        return new Builder();
    }

    public static class Builder {
        private final ConcurrentHashMap<DocRef, Map<TrackerWindow, SearchRequest>> requests;

        private Builder() {
            this.requests = new ConcurrentHashMap<>();
        }

        public Builder withRequest(final DocRef docRef,
                                   final TrackerWindow trackerWindow,
                                   final SearchRequest request) {
            this.requests
                    .computeIfAbsent(docRef, (d) -> new HashMap<>())
                    .put(trackerWindow, request);
            return this;
        }

        public SplitSearchRequest build() {
            return new SplitSearchRequest(this);
        }
    }
}
