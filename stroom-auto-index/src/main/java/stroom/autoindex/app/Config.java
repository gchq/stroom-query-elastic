package stroom.autoindex.app;

import com.bendb.dropwizard.jooq.JooqFactory;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import io.dropwizard.db.DataSourceFactory;
import io.dropwizard.flyway.FlywayFactory;
import stroom.query.audit.authorisation.AuthorisationServiceConfig;
import stroom.query.audit.authorisation.HasAuthorisationConfig;
import stroom.query.audit.security.HasTokenConfig;
import stroom.query.audit.security.TokenConfig;
import stroom.query.elastic.config.ElasticConfig;
import stroom.query.elastic.config.HasElasticConfig;
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
                    HasJooqFactory,
                    HasElasticConfig {
    @Valid
    @NotNull
    @JsonProperty("database")
    private DataSourceFactory dataSourceFactory = new DataSourceFactory();

    @Valid
    @NotNull
    @JsonProperty("trackingDatabase")
    private DataSourceFactory trackingDataSourceFactory = new DataSourceFactory();

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
    @JsonProperty("indexing")
    private IndexingConfig indexingConfig;

    @NotNull
    @JsonProperty("queryResourceUrlsByType")
    private Map<String, String> queryResourceUrlsByType;

    @NotNull
    @JsonProperty("serviceUser")
    private ServiceUserConfig serviceUser;

    @Valid
    @NotNull
    @JsonProperty("elastic")
    private ElasticConfig elasticConfig;

    public ElasticConfig getElasticConfig() {
        return elasticConfig;
    }

    public Map<String, String> getQueryResourceUrlsByType() {
        return queryResourceUrlsByType;
    }

    public final DataSourceFactory getDataSourceFactory() {
        return this.dataSourceFactory;
    }

    public DataSourceFactory getTrackingDataSourceFactory() {
        return trackingDataSourceFactory;
    }

    public final FlywayFactory getFlywayFactory() {
        return this.flywayFactory;
    }

    public final JooqFactory getJooqFactory() {
        return jooqFactory;
    }

    public IndexingConfig getIndexingConfig() {
        return indexingConfig;
    }

    public ServiceUserConfig getServiceUser() {
        return serviceUser;
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
