package stroom.akka.query.messages;

import stroom.akka.ApiMessage;
import stroom.datasource.api.v2.DataSource;
import stroom.query.api.v2.DocRef;
import stroom.security.ServiceUser;

public interface QueryDataSourceMessages {

    static Job dataSource(final ServiceUser user,
                          final DocRef docRef) {
        return new Job(user, docRef);
    }

    static JobComplete complete(final Job request,
                                final DataSource response) {
        return new JobComplete(request, response, null);
    }

    static JobComplete failed(final Job request,
                              final Throwable exception) {
        return new JobComplete(request, null, exception.getLocalizedMessage());
    }

    class Job extends ApiMessage {
        private final DocRef docRef;

        public Job(final ServiceUser user,
                   final DocRef docRef) {
            super(user, docRef.getType());
            this.docRef = docRef;
        }

        public DocRef getDocRef() {
            return docRef;
        }
    }

    class JobComplete extends stroom.akka.JobComplete {
        private JobComplete(final Job request,
                            final DataSource response,
                            final String error) {
            super(request, response, error);
        }
    }
}
