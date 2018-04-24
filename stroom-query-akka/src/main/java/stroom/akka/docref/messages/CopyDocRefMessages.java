package stroom.akka.docref.messages;

import stroom.akka.ApiMessage;
import stroom.query.api.v2.DocRef;
import stroom.query.audit.model.DocRefEntity;
import stroom.security.ServiceUser;

public interface CopyDocRefMessages {
    
    static Job copy(final ServiceUser user,
                    final DocRef original,
                    final String copyUuid) {
        return new Job(user, original, copyUuid);
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
        final String originalUuid;
        final String copyUuid;

        public Job(final ServiceUser user,
                   final DocRef original,
                   final String copyUuid) {
            super(user, original.getType());
            this.originalUuid = original.getUuid();
            this.copyUuid = copyUuid;
        }

        public String getOriginalUuid() {
            return originalUuid;
        }

        public String getCopyUuid() {
            return copyUuid;
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
