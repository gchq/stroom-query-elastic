package stroom.query.elastic;

import org.elasticsearch.client.transport.TransportClient;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import stroom.query.elastic.resources.ElasticIndexResource;
import stroom.query.elastic.resources.ElasticIndexResourceImpl;
import stroom.query.elastic.service.ElasticDocRefService;
import stroom.query.elastic.service.ElasticDocRefServiceImpl;

public class Module extends AbstractBinder {

    private final TransportClient transportClient;

    Module(final TransportClient transportClient) {
        this.transportClient = transportClient;
    }

    @Override
    protected void configure() {
        bind(transportClient).to(TransportClient.class);
        bind(ElasticDocRefServiceImpl.class).to(ElasticDocRefService.class);
        bind(ElasticIndexResourceImpl.class).to(ElasticIndexResource.class);
        bind(transportClient).to(TransportClient.class);
    }
}
