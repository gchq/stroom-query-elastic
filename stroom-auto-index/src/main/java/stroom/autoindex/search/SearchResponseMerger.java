package stroom.autoindex.search;

import stroom.query.api.v2.SearchResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class SearchResponseMerger {
    private final List<SearchResponse> responses = new ArrayList<>();

    public static SearchResponseMerger start() {
        return new SearchResponseMerger();
    }

    private SearchResponseMerger() {
    }

    public SearchResponseMerger response(final SearchResponse response) {
        this.responses.add(response);
        return this;
    }

    public final Optional<SearchResponse> merge() {
        if (responses.size() > 0) {
            return Optional.of(responses.get(0)); // To be implemented
        } else {
            return Optional.empty();
        }
    }
}
