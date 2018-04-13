package stroom.query.csv;

import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import io.dropwizard.ConfiguredBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import stroom.query.audit.AuditedQueryBundle;

import java.util.function.Function;

public class AuditedCsvBundle
        implements ConfiguredBundle<CsvConfig> {

    private final AuditedQueryBundle<CsvConfig,
            CsvDocRefServiceImpl,
            CsvDocRefEntity,
            CsvQueryServiceImpl> auditedQueryBundle;

    public AuditedCsvBundle(final Function<CsvConfig, Injector> injectorSupplier) {
        auditedQueryBundle = new AuditedQueryBundle<>(injectorSupplier,
                CsvDocRefServiceImpl.class,
                CsvDocRefEntity.class,
                CsvQueryServiceImpl.class);
    }

    public Module getGuiceModule(CsvConfig configuration) {
        return Modules.combine(new AbstractModule() {
            @Override
            protected void configure() {
            }
        }, auditedQueryBundle.getGuiceModule(configuration));
    }

    @Override
    public void run(final CsvConfig configuration,
                    final Environment environment) throws Exception {

    }

    @Override
    public void initialize(final Bootstrap<?> bootstrap) {
        final Bootstrap<CsvConfig> castBootstrap = (Bootstrap<CsvConfig>) bootstrap; // this initialize function should have used the templated config type
        castBootstrap.addBundle(auditedQueryBundle);
    }
}
