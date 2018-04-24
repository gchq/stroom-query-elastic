package stroom.akka.docref.messages;

import stroom.akka.ApiMessage;
import stroom.query.api.v2.DocRef;
import stroom.query.audit.model.DocRefEntity;
import stroom.security.ServiceUser;

public interface RenameDocRefMessages {

    static Job rename(final ServiceUser user,
                      final DocRef existingDocRef,
                      final String name) {
        return new Job(user, existingDocRef, name);
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
                   final DocRef existingDocRef,
                   final String name) {
            super(user, existingDocRef.getType());
            this.uuid = existingDocRef.getUuid();
            this.name = name;
        }

        public String getUuid() {
            return uuid;
        }

        public String getName() {
            return name;
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
