package stroom.query.elastic;

import io.dropwizard.Application;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.eclipse.jetty.servlets.CrossOriginFilter;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.collect.Tuple;
import org.glassfish.hk2.api.TypeLiteral;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import stroom.query.audit.AuditedQueryBundle;
import stroom.query.audit.service.DocRefService;
import stroom.query.elastic.config.Config;
import stroom.query.elastic.health.ElasticHealthCheck;
import stroom.query.elastic.hibernate.ElasticIndexDocRefEntity;
import stroom.query.elastic.service.ElasticDocRefServiceImpl;
import stroom.query.elastic.service.ElasticQueryServiceImpl;
import stroom.query.elastic.transportClient.TransportClientBundle;

import javax.servlet.DispatcherType;
import javax.servlet.FilterRegistration;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Map;
import java.util.stream.Collectors;

public class App extends Application<Config> {

    public TransportClientBundle<Config> transportClientBundle = new TransportClientBundle<Config>() {

        @Override
        protected Map<String, Integer> getHosts(final Config config) {
            return Arrays.stream(config.getElasticConfig().getHosts().split(ElasticConfig.ENTRY_DELIMITER))
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
            ElasticDocRefServiceImpl,
            ElasticIndexDocRefEntity,
            ElasticQueryServiceImpl> auditedQueryBundle =
            new AuditedQueryBundle<>(
                    ElasticDocRefServiceImpl.class,
                    ElasticIndexDocRefEntity.class,
                    ElasticQueryServiceImpl.class);

    public static void main(String[] args) throws Exception {
        new App().run(args);
    }

    @Override
    public void run(final Config configuration, final Environment environment) {
        environment.healthChecks().register(
                "Elastic",
                new ElasticHealthCheck(transportClientBundle.getTransportClient())
        );
        environment.jersey().register(
                new AbstractBinder() {
                    @Override
                    protected void configure() {
                        bind(transportClientBundle.getTransportClient()).to(TransportClient.class);
                        bind(ElasticDocRefServiceImpl.class).to(new TypeLiteral<DocRefService<ElasticIndexDocRefEntity>>() {});
                    }
                }
        );

        configureCors(environment);
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

    private static void configureCors(final Environment environment) {
        FilterRegistration.Dynamic cors = environment.servlets().addFilter("CORS", CrossOriginFilter.class);
        cors.addMappingForUrlPatterns(EnumSet.allOf(DispatcherType.class), true, new String[]{"/*"});
        cors.setInitParameter("allowedMethods", "GET,PUT,POST,DELETE,OPTIONS");
        cors.setInitParameter("allowedOrigins", "*");
        cors.setInitParameter("Access-Control-Allow-Origin", "*");
        cors.setInitParameter("allowedHeaders", "Content-Type,Authorization,X-Requested-With,Content-Length,Accept,Origin");
        cors.setInitParameter("allowCredentials", "true");
    }
}
