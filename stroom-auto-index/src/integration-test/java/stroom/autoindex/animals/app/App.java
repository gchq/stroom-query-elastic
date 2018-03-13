package stroom.autoindex.animals.app;

import com.codahale.metrics.health.HealthCheck;
import io.dropwizard.Application;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.glassfish.hk2.api.TypeLiteral;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import stroom.query.audit.AuditedQueryBundle;
import stroom.query.audit.service.DocRefService;

public class App extends Application<Config> {
    private final AuditedQueryBundle<Config,
            AnimalDocRefServiceImpl,
            AnimalDocRefEntity,
            AnimalQueryServiceImpl> auditedQueryBundle =
            new AuditedQueryBundle<>(
                    AnimalDocRefServiceImpl.class,
                    AnimalDocRefEntity.class,
                    AnimalQueryServiceImpl.class);

    @Override
    public void run(final Config configuration,
                    final Environment environment) throws Exception {

        environment.healthChecks().register("Something", new HealthCheck() {
            @Override
            protected Result check() throws Exception {
                return Result.healthy("Keeps Dropwizard Happy");
            }
        });

        // Why is this required???
        environment.jersey().register(new AbstractBinder() {
            @Override
            protected void configure() {
                bind(AnimalDocRefServiceImpl.class).to(new TypeLiteral<DocRefService<AnimalDocRefEntity>>(){});
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
