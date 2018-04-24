package stroom.akka.docref.messages;

import stroom.akka.ApiMessage;
import stroom.query.audit.model.DocRefEntity;
import stroom.security.ServiceUser;

public interface GetDocRefMessages {

    static Job get(final ServiceUser user,
                   final String docRefType,
                   final String uuid) {
        return new Job(user, docRefType, uuid);
    }

    static <D extends DocRefEntity> JobComplete complete(final Job job,
                                                         final D response) {
        return new JobComplete<>(job, response, null);
    }

    static <D extends DocRefEntity> JobComplete<D> failed(final Job job,
                                                          final String error) {
        return new JobComplete<>(job, null, error);
    }

    class Job extends ApiMessage {
        final String uuid;

        public Job(final ServiceUser user,
                   final String docRefType,
                   final String uuid) {
            super(user, docRefType);
            this.uuid = uuid;
        }

        public String getUuid() {
            return uuid;
        }
    }

    class JobComplete<D extends DocRefEntity> extends stroom.akka.JobComplete {

        private JobComplete(final Job job,
                            final D d,
                            final String error) {
            super(job, d, error);
        }
    }
}
