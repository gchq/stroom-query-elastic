package stroom.query.elastic;

import io.dropwizard.Application;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.db.DataSourceFactory;
import io.dropwizard.hibernate.HibernateBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.eclipse.jetty.servlets.CrossOriginFilter;
import stroom.query.audit.AuditedQueryBundle;
import stroom.query.elastic.health.QueryHealthCheck;
import stroom.query.elastic.hibernate.ElasticIndexConfig;
import stroom.query.elastic.resources.QueryResourceImpl;
import stroom.query.elastic.transportClient.TransportClientBundle;

import javax.servlet.DispatcherType;
import javax.servlet.FilterRegistration;
import java.util.EnumSet;
import java.util.Map;

public class App extends Application<Config> {

    private final HibernateBundle<Config> hibernateBundle = new HibernateBundle<Config>(ElasticIndexConfig.class) {
        @Override
        public DataSourceFactory getDataSourceFactory(Config configuration) {
            return configuration.getDataSourceFactory();
        }
    };

    public TransportClientBundle<Config> transportClientBundle = new TransportClientBundle<Config>() {

        @Override
        protected Map<String, Integer> getHosts(final Config config) {
            return config.getElasticConfig().getHosts();
        }

        @Override
        protected String getClusterName(final Config config) {
            return config.getElasticConfig().getClusterName();
        }
    };

    private final AuditedQueryBundle auditedQueryBundle = new AuditedQueryBundle<>(QueryResourceImpl.class);

    public static void main(String[] args) throws Exception {
        new App().run(args);
    }

    @Override
    public void run(final Config configuration, final Environment environment) throws Exception {

        environment.healthChecks().register("Elastic", new QueryHealthCheck(transportClientBundle.getTransportClient()));
        environment.jersey().register(new Module(transportClientBundle.getTransportClient(), hibernateBundle.getSessionFactory()));

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
        bootstrap.addBundle(this.hibernateBundle);
        bootstrap.addBundle(this.auditedQueryBundle);

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
