package stroom.autoindex;

import io.dropwizard.Application;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.collect.Tuple;
import org.glassfish.hk2.api.TypeLiteral;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import stroom.query.audit.AuditedQueryBundle;
import stroom.query.audit.client.QueryResourceHttpClient;
import stroom.query.audit.service.DocRefService;
import stroom.query.elastic.ElasticConfig;
import stroom.query.elastic.health.ElasticHealthCheck;
import stroom.query.elastic.transportClient.TransportClientBundle;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

public class App extends Application<Config> {

    public TransportClientBundle<Config> transportClientBundle = new TransportClientBundle<Config>() {

        @Override
        protected Map<String, Integer> getHosts(final Config config) {
            return Arrays.stream(config.getElasticConfig().getTransportHosts().split(ElasticConfig.ENTRY_DELIMITER))
                    .map(h -> h.split(ElasticConfig.HOST_PORT_DELIMITER))
                    .filter(h -> (h.length == 2))
                    .map(h -> new Tuple<>(h[0], Integer.parseInt(h[1])))
                    .collect(Collectors.toMap(Tuple::v1, Tuple::v2));
        }

        @Override
        protected String getClusterName(final Config config) {
            return config.getElasticConfig().getClusterName();
        }
    };

    private final AuditedQueryBundle<Config,
            AutoIndexDocRefServiceImpl,
            AutoIndexDocRefEntity,
            AutoIndexQueryServiceImpl> auditedQueryBundle =
            new AuditedQueryBundle<>(
                    AutoIndexDocRefServiceImpl.class,
                    AutoIndexDocRefEntity.class,
                    AutoIndexQueryServiceImpl.class);

    @Override
    public void run(final Config configuration,
                    final Environment environment) throws Exception {
        environment.healthChecks().register(
                "Elastic",
                new ElasticHealthCheck(transportClientBundle.getTransportClient())
        );

        environment.jersey().register(new AbstractBinder() {
            @Override
            protected void configure() {
                final ConcurrentHashMap<String, QueryResourceHttpClient> cache =
                        new ConcurrentHashMap<>();
                final Function<String, Optional<QueryResourceHttpClient>> cacheNamed =
                        (type) -> Optional.ofNullable(configuration.getQueryResourceUrlsByType())
                                .map(m -> m.get(type))
                                .map(url -> cache.computeIfAbsent(url, QueryResourceHttpClient::new));

                bind(cacheNamed)
                        .named(AutoIndexQueryServiceImpl.QUERY_HTTP_CLIENT_CACHE)
                        .to(new TypeLiteral<Function<String, Optional<QueryResourceHttpClient>>>() {});
                bind(transportClientBundle.getTransportClient()).to(TransportClient.class);
                bind(AutoIndexDocRefServiceImpl.class).to(new TypeLiteral<DocRefService<AutoIndexDocRefEntity>>(){});
            }
        });
    }

    @Override
    public void initialize(final Bootstrap<Config> bootstrap) {
        super.initialize(bootstrap);

        // This allows us to use templating in the YAML configuration.
        bootstrap.setConfigurationSourceProvider(new SubstitutingSourceProvider(
                bootstrap.getConfigurationSourceProvider(),
                new EnvironmentVariableSubstitutor(false)));

        bootstrap.addBundle(this.transportClientBundle);
        bootstrap.addBundle(this.auditedQueryBundle);

    }
}
