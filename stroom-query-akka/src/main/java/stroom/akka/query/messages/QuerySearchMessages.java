package stroom.akka.query.messages;

import stroom.query.api.v2.SearchRequest;
import stroom.query.api.v2.SearchResponse;

public interface QuerySearchMessages {

    static JobComplete complete(final SearchRequest job,
                                final SearchResponse response) {
        return new JobComplete(job, response, null);
    }

    static JobComplete failed(final SearchRequest job,
                              final Throwable exception) {
        return new JobComplete(job, null, exception.getLocalizedMessage());
    }

    class JobComplete extends stroom.akka.JobComplete<SearchRequest, SearchResponse> {
        private JobComplete(final SearchRequest request,
                            final SearchResponse response,
                            final String error) {
            super(request, response, error);
        }
    }
}
