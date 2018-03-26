package stroom.autoindex;

import com.codahale.metrics.health.HealthCheck;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import com.google.inject.util.Modules;
import io.dropwizard.Application;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import stroom.autoindex.indexing.IndexJob;
import stroom.autoindex.indexing.IndexJobConsumer;
import stroom.autoindex.indexing.IndexJobDao;
import stroom.autoindex.indexing.IndexJobDaoImpl;
import stroom.autoindex.indexing.IndexingTimerTask;
import stroom.autoindex.tracker.AutoIndexTrackerDao;
import stroom.autoindex.tracker.AutoIndexTrackerDaoImpl;
import stroom.query.audit.client.QueryResourceHttpClient;
import stroom.query.jooq.AuditedJooqDocRefBundle;

import java.util.Optional;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;

public class App extends Application<Config> {

    private Injector injector;

    private AuditedJooqDocRefBundle<Config,
                AutoIndexDocRefServiceImpl,
                AutoIndexDocRefEntity,
                AutoIndexQueryServiceImpl> auditedQueryBundle;

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
                final ConcurrentHashMap<String, QueryResourceHttpClient> cache =
                        new ConcurrentHashMap<>();
                final Function<String, Optional<QueryResourceHttpClient>> cacheNamed =
                        (type) -> Optional.ofNullable(configuration.getQueryResourceUrlsByType())
                                .map(m -> m.get(type))
                                .map(url -> cache.computeIfAbsent(url, QueryResourceHttpClient::new));

                bind(new TypeLiteral<Function<String, Optional<QueryResourceHttpClient>>>() {})
                        .annotatedWith(Names.named(AutoIndexQueryServiceImpl.QUERY_HTTP_CLIENT_CACHE))
                        .toInstance(cacheNamed);

                bind(AutoIndexTrackerDao.class).to(AutoIndexTrackerDaoImpl.class);
                bind(IndexJobDao.class).to(IndexJobDaoImpl.class);
                bind(new TypeLiteral<Consumer<IndexJob>>(){})
                        .annotatedWith(Names.named(IndexingTimerTask.TASK_HANDLER_NAME))
                        .to(IndexJobConsumer.class)
                        .asEagerSingleton(); // singleton so that the test receives same instance as the underlying timer task
                bind(IndexingConfig.class).toInstance(configuration.getIndexingConfig());
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

        bootstrap.addBundle(this.auditedQueryBundle);

    }
}
