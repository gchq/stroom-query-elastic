package stroom.autoindex;

import io.dropwizard.db.DataSourceFactory;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.junit.rules.MethodRule;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * A rule that can be applied to a test to clear database tables before test execution
 */
public class InitialiseJooqDbRule implements MethodRule {

    private final Supplier<DataSourceFactory> dataSourceFactory;
    private final List<String> tablesToClear;

    private InitialiseJooqDbRule(final Builder builder) {
        this.dataSourceFactory = builder.dataSourceFactory;
        this.tablesToClear = builder.tablesToClear;
    }

    @Override
    public Statement apply(final Statement statement,
                           final FrameworkMethod frameworkMethod,
                           final Object o) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                before();
                statement.evaluate();
            }
        };
    }

    private void before() throws Throwable {
        final DataSourceFactory dsf = Optional.ofNullable(dataSourceFactory.get())
                .orElseThrow(() -> new RuntimeException("Could not get Data Source Factory"));

        DSL.using(dsf.getUrl(), dsf.getUser(), dsf.getPassword())
                .transaction(c -> this.tablesToClear.stream()
                        .map(DSL::table)
                        .forEach(t -> DSL.using(c).deleteFrom(t).execute()));
    }

    public DSLContext withDatabase() {
        final DataSourceFactory dsf = Optional.ofNullable(dataSourceFactory.get())
                .orElseThrow(() -> new RuntimeException("Could not get Data Source Factory"));

        return DSL.using(dsf.getUrl(), dsf.getUser(), dsf.getPassword());
    }

    public static Builder withDataSourceFactory(final Supplier<DataSourceFactory> dataSourceFactory) {
        return new Builder(dataSourceFactory);
    }

    public static final class Builder {
        private final Supplier<DataSourceFactory> dataSourceFactory;
        private final List<String> tablesToClear;

        private Builder(final Supplier<DataSourceFactory> dataSourceFactory) {
            this.dataSourceFactory = dataSourceFactory;
            this.tablesToClear = new ArrayList<>();
        }

        public Builder tableToClear(final String tableName) {
            this.tablesToClear.add(tableName);
            return this;
        }

        public InitialiseJooqDbRule build() {
            return new InitialiseJooqDbRule(this);
        }
    }
}
