package stroom.query.elastic;

import org.elasticsearch.client.transport.TransportClient;
import org.glassfish.hk2.utilities.binding.AbstractBinder;

public class Module extends AbstractBinder {

    private final TransportClient transportClient;

    Module(final TransportClient transportClient) {
        this.transportClient = transportClient;
    }

    @Override
    protected void configure() {
        bind(transportClient).to(TransportClient.class);
    }
}
