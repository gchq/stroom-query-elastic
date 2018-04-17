package stroom.autoindex.animals.app;

import com.codahale.metrics.health.HealthCheck;
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
import stroom.query.csv.AuditedCsvBundle;
import stroom.query.csv.CsvConfig;
import stroom.query.csv.CsvFieldSupplier;

public class AnimalApp extends Application<CsvConfig> {
    private Injector injector;

    private AuditedCsvBundle auditedQueryBundle;

    public Module getGuiceModule(CsvConfig configuration) {
        return Modules.combine(new AbstractModule() {
            @Override
            protected void configure() {
                bind(CsvFieldSupplier.class).to(AnimalFieldSupplier.class);
            }
        }, auditedQueryBundle.getGuiceModule(configuration));
    }

    @Override
    public void run(final CsvConfig configuration,
                    final Environment environment) throws Exception {

        environment.healthChecks().register("Something", new HealthCheck() {
            @Override
            protected Result check() throws Exception {
                return Result.healthy("Keeps Dropwizard Happy");
            }
        });
    }

    @Override
    public void initialize(final Bootstrap<CsvConfig> bootstrap) {
        super.initialize(bootstrap);

        auditedQueryBundle =
                new AuditedCsvBundle(
                        (c) -> {
                            this.injector = Guice.createInjector(getGuiceModule(c));
                            return this.injector;
                        });

        // This allows us to use templating in the YAML configuration.
        bootstrap.setConfigurationSourceProvider(new SubstitutingSourceProvider(
                bootstrap.getConfigurationSourceProvider(),
                new EnvironmentVariableSubstitutor(false)));

        bootstrap.addBundle(this.auditedQueryBundle);

    }
}
