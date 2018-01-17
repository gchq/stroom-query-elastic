package stroom.query.elastic;

import event.logging.EventLoggingService;
import io.dropwizard.Application;
import io.dropwizard.auth.AuthDynamicFeature;
import io.dropwizard.auth.AuthValueFactoryProvider;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.eclipse.jetty.servlets.CrossOriginFilter;
import org.elasticsearch.common.collect.Tuple;
import org.glassfish.jersey.server.filter.RolesAllowedDynamicFeature;
import stroom.query.audit.AuditedQueryBundle;
import stroom.query.audit.authorisation.AuthorisationService;
import stroom.query.audit.rest.AuditedDocRefResourceImpl;
import stroom.query.audit.security.RobustJwtAuthFilter;
import stroom.query.audit.security.ServiceUser;
import stroom.query.audit.security.TokenConfig;
import stroom.query.audit.service.DocRefService;
import stroom.query.elastic.config.Config;
import stroom.query.elastic.health.ElasticHealthCheck;
import stroom.query.elastic.hibernate.ElasticIndexConfig;
import stroom.query.elastic.service.ElasticDocRefServiceImpl;
import stroom.query.elastic.service.ElasticQueryServiceImpl;
import stroom.query.elastic.transportClient.TransportClientBundle;

import javax.inject.Inject;
import javax.servlet.DispatcherType;
import javax.servlet.FilterRegistration;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Map;
import java.util.function.Supplier;
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

    public static final class AuditedElasticDocRefResource extends AuditedDocRefResourceImpl<ElasticIndexConfig> {

        @Inject
        public AuditedElasticDocRefResource(final DocRefService<ElasticIndexConfig> service,
                                            final EventLoggingService eventLoggingService,
                                            final AuthorisationService authorisationService) {
            super(service, eventLoggingService, authorisationService);
        }
    }

    private final AuditedQueryBundle auditedQueryBundle =
            new AuditedQueryBundle<>(
                    ElasticQueryServiceImpl.class,
                    ElasticIndexConfig.class,
                    AuditedElasticDocRefResource.class,
                    ElasticDocRefServiceImpl.class);

    public static void main(String[] args) throws Exception {
        new App().run(args);
    }

    @Override
    public void run(final Config configuration, final Environment environment) {

        // And we want to configure authentication before the resources
        configureAuthentication(configuration.getTokenConfig(), environment);

        environment.healthChecks().register(
                "Elastic",
                new ElasticHealthCheck(transportClientBundle.getTransportClient())
        );
        environment.jersey().register(
                new Module(transportClientBundle.getTransportClient())
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

    private static void configureAuthentication(final TokenConfig tokenConfig,
                                                final Environment environment) {
        environment.jersey().register(
                new AuthDynamicFeature(
                        new RobustJwtAuthFilter(tokenConfig)
                ));
        environment.jersey().register(new AuthValueFactoryProvider.Binder<>(ServiceUser.class));
        environment.jersey().register(RolesAllowedDynamicFeature.class);
    }

    private static void configureCors(Environment environment) {
        FilterRegistration.Dynamic cors = environment.servlets().addFilter("CORS", CrossOriginFilter.class);
        cors.addMappingForUrlPatterns(EnumSet.allOf(DispatcherType.class), true, new String[]{"/*"});
        cors.setInitParameter("allowedMethods", "GET,PUT,POST,DELETE,OPTIONS");
        cors.setInitParameter("allowedOrigins", "*");
        cors.setInitParameter("Access-Control-Allow-Origin", "*");
        cors.setInitParameter("allowedHeaders", "Content-Type,Authorization,X-Requested-With,Content-Length,Accept,Origin");
        cors.setInitParameter("allowCredentials", "true");
    }
}
