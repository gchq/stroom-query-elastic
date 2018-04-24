package stroom.akka.docref.messages;

import stroom.akka.ApiMessage;
import stroom.query.api.v2.DocRef;
import stroom.security.ServiceUser;

public interface DeleteDocRefMessages {

    static Job deleteDoc(final ServiceUser user,
                         final DocRef docRef) {
        return new Job(user, docRef);
    }

    static JobComplete complete(final Job update,
                                final Boolean response) {
        return new JobComplete(update, response, null);
    }

    static JobComplete failed(final Job update,
                              final String error) {
        return new JobComplete(update, null, error);
    }

    class Job extends ApiMessage {
        final String uuid;

        public Job(final ServiceUser user,
                   final DocRef docRef) {
            super(user, docRef.getType());
            this.uuid = docRef.getUuid();
        }

        public String getUuid() {
            return uuid;
        }
    }

    class JobComplete extends stroom.akka.JobComplete {

        private JobComplete(final Job request,
                            final Boolean response,
                            final String error) {
            super(request, response, error);
        }
    }
}
