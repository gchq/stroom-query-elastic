package stroom.akka.docref.messages;

import stroom.akka.ApiMessage;
import stroom.query.api.v2.DocRef;
import stroom.query.audit.ExportDTO;
import stroom.security.ServiceUser;

public interface ExportDocRefMessages {

    static Job export(final ServiceUser user,
                      final DocRef docRef) {
        return new Job(user, docRef);
    }

    static JobComplete complete(final Job request,
                                final ExportDTO response) {
        return new JobComplete(request, response, null);
    }

    static JobComplete failed(final Job request,
                              final String error) {
        return new JobComplete(request, null, error);
    }

    class Job extends ApiMessage {
        final String uuid;

        private Job(final ServiceUser user, final DocRef docRef) {
            super(user, docRef.getType());
            this.uuid = docRef.getUuid();
        }

        public String getUuid() {
            return uuid;
        }
    }

    class JobComplete extends stroom.akka.JobComplete {

        private JobComplete(final Job job,
                            final ExportDTO exportDTO,
                            final String error) {
            super(job, exportDTO, error);
        }
    }
}
