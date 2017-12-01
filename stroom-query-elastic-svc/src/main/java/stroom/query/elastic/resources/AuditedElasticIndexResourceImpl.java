package stroom.query.elastic.resources;

import event.logging.Event;
import event.logging.EventLoggingService;
import stroom.query.elastic.hibernate.ElasticIndexConfig;
import stroom.util.shared.QueryApiException;

import javax.inject.Inject;
import javax.ws.rs.core.Response;

public class AuditedElasticIndexResourceImpl implements ElasticIndexResource {

    private final ElasticIndexResource resource;

    private final EventLoggingService eventLoggingService;

    @Inject
    public AuditedElasticIndexResourceImpl(final ElasticIndexResource resource,
                                           final EventLoggingService eventLoggingService) {
        this.resource = resource;
        this.eventLoggingService = eventLoggingService;
    }

    @Override
    public Response update(final String uuid,
                           final ElasticIndexConfig updatedConfig) throws QueryApiException {
        Response response;
        Exception exception = null;

        try {
            response = resource.update(uuid, updatedConfig);

            return response;
        } finally {
            final Event event = eventLoggingService.createEvent();
            final Event.EventDetail eventDetail = event.getEventDetail();

            eventDetail.setTypeId("UPDATE_ELASTIC_INDEX");
            eventDetail.setDescription("Set the specific details of an elastic index doc ref");

            eventLoggingService.log(event);
        }
    }
}
