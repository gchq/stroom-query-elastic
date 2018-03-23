package stroom.autoindex.animals.app;

import com.codahale.metrics.health.HealthCheck;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.dropwizard.Application;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.glassfish.hk2.api.TypeLiteral;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import stroom.query.audit.AuditedQueryBundle;
import stroom.query.audit.service.DocRefService;

public class AnimalApp extends Application<AnimalConfig> {
    private Injector injector;

    private AuditedQueryBundle<AnimalConfig,
            AnimalDocRefServiceImpl,
            AnimalDocRefEntity,
            AnimalQueryServiceImpl> auditedQueryBundle;

    @Override
    public void run(final AnimalConfig configuration,
                    final Environment environment) throws Exception {

        environment.healthChecks().register("Something", new HealthCheck() {
            @Override
            protected Result check() throws Exception {
                return Result.healthy("Keeps Dropwizard Happy");
            }
        });
    }

    @Override
    public void initialize(final Bootstrap<AnimalConfig> bootstrap) {
        super.initialize(bootstrap);

        auditedQueryBundle =
                new AuditedQueryBundle<>(
                        (c) -> {
                            this.injector = Guice.createInjector(auditedQueryBundle.getGuiceModule(c));
                            return this.injector;
                        },
                        AnimalDocRefServiceImpl.class,
                        AnimalDocRefEntity.class,
                        AnimalQueryServiceImpl.class);

        // This allows us to use templating in the YAML configuration.
        bootstrap.setConfigurationSourceProvider(new SubstitutingSourceProvider(
                bootstrap.getConfigurationSourceProvider(),
                new EnvironmentVariableSubstitutor(false)));

        bootstrap.addBundle(this.auditedQueryBundle);

    }
}
