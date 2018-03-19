package stroom.autoindex;

import com.codahale.metrics.health.HealthCheck;
import io.dropwizard.Application;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.glassfish.hk2.api.TypeLiteral;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import stroom.query.audit.client.QueryResourceHttpClient;
import stroom.query.audit.service.DocRefService;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class App extends Application<Config> {


    private final AuditedJooqDocRefBundle<Config,
            AutoIndexDocRefServiceImpl,
            AutoIndexDocRefEntity,
            AutoIndexQueryServiceImpl> auditedQueryBundle =
            new AuditedJooqDocRefBundle<>(
                    AutoIndexDocRefServiceImpl.class,
                    AutoIndexDocRefEntity.class,
                    AutoIndexQueryServiceImpl.class);

    @Override
    public void run(final Config configuration,
                    final Environment environment) throws Exception {
        environment.healthChecks().register("Something", new HealthCheck() {
            @Override
            protected Result check() {
                return Result.healthy("Keeps Dropwizard Happy");
            }
        });

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

        bootstrap.addBundle(this.auditedQueryBundle);

    }
}
