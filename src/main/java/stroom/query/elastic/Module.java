package stroom.query.elastic;

import event.logging.EventLoggingService;
import org.elasticsearch.client.transport.TransportClient;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.hibernate.SessionFactory;
import stroom.query.elastic.audit.ElasticEventLoggingService;
import stroom.query.elastic.hibernate.ElasticIndexConfig;
import stroom.query.elastic.hibernate.ElasticIndexConfigService;
import stroom.query.elastic.hibernate.ElasticIndexConfigServiceImpl;
import stroom.query.elastic.resources.QueryResource;
import stroom.query.elastic.resources.QueryResourceImpl;
public class Module extends AbstractBinder {

    private final TransportClient transportClient;

    private final SessionFactory sessionFactory;

    Module(final TransportClient transportClient,
           final SessionFactory sessionFactory) {
        this.transportClient = transportClient;
        this.sessionFactory = sessionFactory;
    }

    @Override
    protected void configure() {
        bind(transportClient).to(TransportClient.class);
        bind(sessionFactory).to(SessionFactory.class);
        bind(ElasticIndexConfigServiceImpl.class).to(ElasticIndexConfigService.class);
        bind(QueryResourceImpl.class).to(QueryResource.class);
        bind(ElasticEventLoggingService.class).to(EventLoggingService.class);
        bind(transportClient).to(TransportClient.class);
    }
}
