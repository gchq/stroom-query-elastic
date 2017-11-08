package stroom.query.elastic;

import org.elasticsearch.client.transport.TransportClient;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import stroom.query.elastic.hibernate.ElasticIndexConfigService;
import stroom.query.elastic.hibernate.ElasticIndexConfigServiceImpl;

public class Module extends AbstractBinder {

    private final TransportClient transportClient;

    Module(final TransportClient transportClient) {
        this.transportClient = transportClient;
    }

    @Override
    protected void configure() {
        bind(transportClient).to(TransportClient.class);
        bind(ElasticIndexConfigServiceImpl.class).to(ElasticIndexConfigService.class);
        bind(transportClient).to(TransportClient.class);
    }
}
