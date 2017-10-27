package stroom.query.elastic;

import event.logging.EventLoggingService;
import org.elasticsearch.client.transport.TransportClient;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import stroom.query.elastic.audit.ElasticEventLoggingService;
import stroom.query.elastic.resources.QueryResource;
import stroom.query.elastic.resources.QueryResourceImpl;
public class Module extends AbstractBinder {

    private final TransportClient transportClient;

    Module(final TransportClient transportClient) {
        this.transportClient = transportClient;
    }

    @Override
    protected void configure() {
        bind(new QueryResourceImpl(transportClient)).to(QueryResource.class);
        bind(ElasticEventLoggingService.class).to(EventLoggingService.class);
        bind(transportClient).to(TransportClient.class);
    }
}
