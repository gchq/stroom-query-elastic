package stroom.autoindex.app;

import com.codahale.metrics.health.HealthCheck;
import com.google.inject.*;
import com.google.inject.name.Names;
import com.google.inject.util.Modules;
import io.dropwizard.Application;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.elasticsearch.client.transport.TransportClient;
import stroom.autoindex.AutoIndexConstants;
import stroom.autoindex.QueryClientCache;
import stroom.autoindex.indexing.*;
import stroom.autoindex.service.AutoIndexDocRefEntity;
import stroom.autoindex.service.AutoIndexDocRefServiceImpl;
import stroom.autoindex.service.AutoIndexQueryServiceImpl;
import stroom.autoindex.tracker.AutoIndexTrackerDao;
import stroom.autoindex.tracker.AutoIndexTrackerDaoImpl;
import stroom.query.audit.client.DocRefResourceHttpClient;
import stroom.query.audit.client.QueryResourceHttpClient;
import stroom.query.audit.security.ServiceUser;
import stroom.query.elastic.transportClient.TransportClientBundle;
import stroom.query.jooq.AuditedJooqDocRefBundle;

import java.util.Timer;
import java.util.function.Consumer;

public class App extends Application<Config> {

    private Injector injector;

    private AuditedJooqDocRefBundle<Config,
            AutoIndexDocRefServiceImpl,
            AutoIndexDocRefEntity,
            AutoIndexQueryServiceImpl> auditedQueryBundle;

    private TransportClientBundle<Config> transportClientBundle = new TransportClientBundle<>();

    @Override
    public void run(final Config configuration,
                    final Environment environment) throws Exception {
        environment.healthChecks().register("Something", new HealthCheck() {
            @Override
            protected Result check() {
                return Result.healthy("Keeps Dropwizard Happy");
            }
        });

        if (configuration.getIndexingConfig().getEnabled()) {
            final Timer timer = new Timer();
            final IndexingTimerTask indexingTimerTask = injector.getInstance(IndexingTimerTask.class);
            timer.schedule(indexingTimerTask,
                    configuration.getIndexingConfig().getSecondsBetweenChecks() * 1000);
        }
    }

    private Module getGuiceModule(final Config configuration) {
        return Modules.combine(new AbstractModule() {
            @Override
            protected void configure() {
                bind(AutoIndexTrackerDao.class).to(AutoIndexTrackerDaoImpl.class);
                bind(IndexJobDao.class).to(IndexJobDaoImpl.class);
                bind(new TypeLiteral<QueryClientCache<QueryResourceHttpClient>>(){})
                        .toInstance(new QueryClientCache<>(configuration, QueryResourceHttpClient::new));
                bind(new TypeLiteral<QueryClientCache<DocRefResourceHttpClient>>(){})
                        .toInstance(new QueryClientCache<>(configuration, DocRefResourceHttpClient::new));
                bind(new TypeLiteral<Consumer<IndexJob>>(){})
                        .annotatedWith(Names.named(IndexingTimerTask.TASK_HANDLER_NAME))
                        .to(IndexJobConsumer.class)
                        .asEagerSingleton(); // singleton so that the test receives same instance as the underlying timer task
                bind(IndexingConfig.class).toInstance(configuration.getIndexingConfig());
                bind(IndexWriter.class).to(IndexWriterImpl.class);
                bind(Config.class).toInstance(configuration);
                bind(ServiceUser.class).annotatedWith(Names.named(AutoIndexConstants.STROOM_SERVICE_USER))
                        .toInstance(new ServiceUser.Builder()
                                .name(configuration.getServiceUser().getName())
                                .jwt(configuration.getServiceUser().getJwt())
                                .build());
                bind(TransportClient.class).toInstance(transportClientBundle.getTransportClient());
            }
        }, auditedQueryBundle.getGuiceModule(configuration));
    }

    @Override
    public void initialize(final Bootstrap<Config> bootstrap) {
        super.initialize(bootstrap);

        auditedQueryBundle =
                new AuditedJooqDocRefBundle<>(
                        (c) -> {
                            this.injector = Guice.createInjector(getGuiceModule(c));
                            return injector;
                        },
                        AutoIndexDocRefServiceImpl.class,
                        AutoIndexDocRefEntity.class,
                        AutoIndexQueryServiceImpl.class);

        // This allows us to use templating in the YAML configuration.
        bootstrap.setConfigurationSourceProvider(new SubstitutingSourceProvider(
                bootstrap.getConfigurationSourceProvider(),
                new EnvironmentVariableSubstitutor(false)));

        bootstrap.addBundle(this.transportClientBundle);
        bootstrap.addBundle(this.auditedQueryBundle);

    }
}
