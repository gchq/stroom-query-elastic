package stroom.autoindex;

import io.dropwizard.Configuration;
import io.dropwizard.testing.junit.DropwizardAppRule;
import org.jooq.impl.DSL;
import org.junit.rules.MethodRule;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;
import stroom.query.jooq.HasDataSourceFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * A rule that can be applied to a test to clear database tables before test execution
 * @param <CONFIG> The Dropwizard Configuration that contains the jOOQ Factory and Data Source Factory required to
 *                connect to the database.
 */
public class DeleteFromTableRule<CONFIG extends Configuration & HasDataSourceFactory> implements MethodRule {

    private final DropwizardAppRule<CONFIG> appRule;
    private final List<String> tablesToClear;

    private DeleteFromTableRule(final Builder<CONFIG> builder) {
        this.appRule = builder.appRule;
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
        DSL.using(appRule.getConfiguration().getDataSourceFactory().getUrl(),
                appRule.getConfiguration().getDataSourceFactory().getUser(),
                appRule.getConfiguration().getDataSourceFactory().getPassword())
                .transaction(c -> this.tablesToClear.stream()
                        .map(DSL::table)
                        .forEach(t -> DSL.using(c).deleteFrom(t).execute()));
    }

    public static <C extends Configuration & HasDataSourceFactory>
    Builder<C> withApp(final DropwizardAppRule<C> appRule) {
        return new Builder<>(appRule);
    }

    public static final class Builder<C extends Configuration & HasDataSourceFactory> {
        private final DropwizardAppRule<C> appRule;
        private final List<String> tablesToClear;

        private Builder(final DropwizardAppRule<C> appRule) {
            this.appRule = appRule;
            this.tablesToClear = new ArrayList<>();
        }

        public Builder<C> table(final String tableName) {
            this.tablesToClear.add(tableName);
            return this;
        }

        public DeleteFromTableRule<C> build() {
            return new DeleteFromTableRule<>(this);
        }
    }
}
