package stroom.akka.docref.messages;

import stroom.akka.ApiMessage;
import stroom.query.api.v2.DocRef;
import stroom.query.audit.model.DocRefEntity;
import stroom.security.ServiceUser;

public interface MoveDocRefMessages {

    static <D extends DocRefEntity> Job move(final ServiceUser user,
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

        public Job(final ServiceUser user,
                   final DocRef docRef) {
            super(user, docRef.getType());
            this.uuid = docRef.getUuid();
        }

        public String getUuid() {
            return uuid;
        }
    }

    class JobComplete<D extends DocRefEntity> extends stroom.akka.JobComplete {

        private JobComplete(final Job update,
                            final D d,
                            final String error) {
            super(update, d, error);
        }
    }
}
