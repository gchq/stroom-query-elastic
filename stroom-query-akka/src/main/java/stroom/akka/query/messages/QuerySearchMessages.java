package stroom.akka.query.messages;

import stroom.akka.ApiMessage;
import stroom.query.api.v2.SearchRequest;
import stroom.query.api.v2.SearchResponse;
import stroom.security.ServiceUser;

public interface QuerySearchMessages {

    static Job search(final ServiceUser user,
                      final String docRefType,
                      final SearchRequest request) {
        return new Job(user, docRefType, request);
    }

    static JobComplete complete(final Job job,
                                final SearchResponse response) {
        return new JobComplete(job, response, null);
    }

    static JobComplete failed(final Job job,
                              final Throwable exception) {
        return new JobComplete(job, null, exception.getLocalizedMessage());
    }

    class Job extends ApiMessage {
        private final SearchRequest request;

        private Job(final ServiceUser user,
                    final String docRefType,
                    final SearchRequest request) {
            super(user, docRefType);
            this.request = request;
        }

        public SearchRequest getRequest() {
            return request;
        }
    }

    class JobComplete extends stroom.akka.JobComplete {
        private JobComplete(final Job request,
                            final SearchResponse response,
                            final String error) {
            super(request, response, error);
        }
    }
}
