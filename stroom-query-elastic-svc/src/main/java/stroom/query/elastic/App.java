package stroom.query.elastic;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import io.dropwizard.Application;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.eclipse.jetty.servlets.CrossOriginFilter;
import org.elasticsearch.client.transport.TransportClient;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import stroom.query.audit.AuditedQueryBundle;
import stroom.query.elastic.config.Config;
import stroom.query.elastic.health.ElasticHealthCheck;
import stroom.query.elastic.model.ElasticIndexDocRefEntity;
import stroom.query.elastic.service.ElasticIndexDocRefServiceImpl;
import stroom.query.elastic.service.ElasticQueryServiceImpl;
import stroom.query.elastic.transportClient.TransportClientBundle;

import javax.servlet.DispatcherType;
import javax.servlet.FilterRegistration;
import java.util.EnumSet;

public class App extends Application<Config> {

    private Injector injector;

    private TransportClientBundle<Config> transportClientBundle = new TransportClientBundle<>();

    private AuditedQueryBundle<Config,
            ElasticIndexDocRefServiceImpl,
            ElasticIndexDocRefEntity,
            ElasticQueryServiceImpl> auditedQueryBundle;

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
                    }
                }
        );

        configureCors(environment);
    }

    private Module getGuiceModule(final Config config) {
        return Modules.combine(new AbstractModule() {
            @Override
            protected void configure() {
                bind(TransportClient.class).toInstance(transportClientBundle.getTransportClient());

            }
        }, auditedQueryBundle.getGuiceModule(config));
    }

    @Override
    public void initialize(final Bootstrap<Config> bootstrap) {
        super.initialize(bootstrap);

        auditedQueryBundle =
                new AuditedQueryBundle<>(
                        (c) -> {
                            this.injector = Guice.createInjector(getGuiceModule(c));
                            return injector;
                        },
                        ElasticIndexDocRefServiceImpl.class,
                        ElasticIndexDocRefEntity.class,
                        ElasticQueryServiceImpl.class);

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
