package stroom.akka.docref.messages;

import stroom.akka.ApiMessage;
import stroom.query.api.v2.DocRef;
import stroom.query.audit.model.DocRefEntity;
import stroom.security.ServiceUser;

import java.util.Map;

public interface ImportDocRefMessages {

    static Job importDoc(final ServiceUser user,
                         final DocRef docRef,
                         final Boolean confirmed,
                         final Map<String, String> dataMap) {
        return new Job(user, docRef, confirmed, dataMap);
    }

    static <D extends DocRefEntity> JobComplete<D> complete(final Job request,
                                                            final D response) {
        return new JobComplete<>(request, response, null);
    }

    static <D extends DocRefEntity> JobComplete<D> failed(final Job request,
                                                          final String error) {
        return new JobComplete<>(request, null, error);
    }

    class Job extends ApiMessage {
        final String uuid;
        final String name;
        final Boolean confirmed;
        final Map<String, String> dataMap;

        public Job(final ServiceUser user,
                   final DocRef docRef,
                   final Boolean confirmed,
                   final Map<String, String> dataMap) {
            super(user, docRef.getType());
            this.uuid = docRef.getUuid();
            this.name = docRef.getName();
            this.confirmed = confirmed;
            this.dataMap = dataMap;
        }
    }

    class JobComplete<D extends DocRefEntity> extends stroom.akka.JobComplete {

        protected JobComplete(final Job anJob,
                              final D d,
                              final String error) {
            super(anJob, d, error);
        }
    }
}
