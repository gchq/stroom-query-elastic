package stroom.autoindex;

import com.bendb.dropwizard.jooq.JooqFactory;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import io.dropwizard.db.DataSourceFactory;
import io.dropwizard.flyway.FlywayFactory;
import stroom.query.audit.authorisation.AuthorisationServiceConfig;
import stroom.query.audit.authorisation.HasAuthorisationConfig;
import stroom.query.audit.security.HasTokenConfig;
import stroom.query.audit.security.TokenConfig;
import stroom.query.jooq.HasDataSourceFactory;
import stroom.query.jooq.HasFlywayFactory;
import stroom.query.jooq.HasJooqFactory;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.Map;

public class Config
        extends Configuration
        implements  HasAuthorisationConfig,
                    HasTokenConfig,
                    HasDataSourceFactory,
                    HasFlywayFactory,
                    HasJooqFactory {
    @Valid
    @NotNull
    @JsonProperty("database")
    private DataSourceFactory dataSourceFactory = new DataSourceFactory();

    @Valid
    @NotNull
    @JsonProperty("jooq")
    private JooqFactory jooqFactory = new JooqFactory();

    @Valid
    @NotNull
    @JsonProperty("flyway")
    private FlywayFactory flywayFactory = new FlywayFactory();

    @NotNull
    @JsonProperty("token")
    private TokenConfig tokenConfig;

    @NotNull
    @JsonProperty("authorisationService")
    private AuthorisationServiceConfig authorisationServiceConfig;

    @NotNull
    @JsonProperty("queryResourceUrlsByType")
    private Map<String, String> queryResourceUrlsByType;

    public Map<String, String> getQueryResourceUrlsByType() {
        return queryResourceUrlsByType;
    }

    public final DataSourceFactory getDataSourceFactory() {
        return this.dataSourceFactory;
    }

    public final FlywayFactory getFlywayFactory() {
        return this.flywayFactory;
    }

    public final JooqFactory getJooqFactory() {
        return jooqFactory;
    }

    @Override
    public TokenConfig getTokenConfig() {
        return tokenConfig;
    }

    @Override
    public AuthorisationServiceConfig getAuthorisationServiceConfig() {
        return authorisationServiceConfig;
    }
}
