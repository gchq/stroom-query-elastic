package stroom.akka.docref.messages;

import stroom.akka.ApiMessage;
import stroom.query.audit.model.DocRefEntity;
import stroom.security.ServiceUser;

public interface UpdateDocRefMessages {

    static <D extends DocRefEntity> Update<D> update(final ServiceUser user,
                                                     final String uuid,
                                                     final D entity) {
        return new Update<>(user, uuid, entity);
    }

    static <D extends DocRefEntity> JobComplete<D> complete(final Update<D> update,
                                                            final D response) {
        return new JobComplete<>(update, response, null);
    }

    static <D extends DocRefEntity> JobComplete<D> failed(final Update<D> update,
                                                          final String error) {
        return new JobComplete<>(update, null, error);
    }

    class Update<D extends DocRefEntity> extends ApiMessage {
        final String uuid;
        final D entity;

        public Update(final ServiceUser user,
                      final String uuid,
                      final D entity) {
            super(user, entity.getType());
            this.uuid = uuid;
            this.entity = entity;
        }

        public String getUuid() {
            return uuid;
        }

        public D getEntity() {
            return entity;
        }
    }

    class JobComplete<D extends DocRefEntity> extends stroom.akka.JobComplete {

        private JobComplete(final Update<D> job,
                            final D d,
                            final String error) {
            super(job, d, error);
        }
    }
}
