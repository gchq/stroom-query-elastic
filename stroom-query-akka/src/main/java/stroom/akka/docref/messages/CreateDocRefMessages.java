package stroom.akka.docref.messages;

import stroom.akka.ApiMessage;
import stroom.query.api.v2.DocRef;
import stroom.query.audit.model.DocRefEntity;
import stroom.security.ServiceUser;

public interface CreateDocRefMessages {

    static Job create(final ServiceUser user,
                      final DocRef docRef) {
        return new Job(user, docRef);
    }

    static <D extends DocRefEntity> JobComplete<D> complete(final Job update,
                                                            final D response) {
        return new JobComplete<>(update, response, null);
    }

    static <D extends DocRefEntity> JobComplete<D> failed(final Job update,
                                                          final String error) {
        return new JobComplete<>(update, null, error);
    }

    class Job extends ApiMessage {
        final String uuid;
        final String name;

        public Job(final ServiceUser user,
                   final DocRef docRef) {
            super(user, docRef.getType());
            this.uuid = docRef.getUuid();
            this.name = docRef.getName();
        }

        public String getUuid() {
            return uuid;
        }

        public String getName() {
            return name;
        }
    }

    class JobComplete<D extends DocRefEntity> extends stroom.akka.JobComplete {

        private JobComplete(final Job request,
                            final D response,
                            final String error) {
            super(request, response, error);
        }
    }
}