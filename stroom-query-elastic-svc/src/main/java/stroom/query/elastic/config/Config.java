package stroom.query.elastic.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import stroom.authorisation.AuthorisationServiceConfig;
import stroom.authorisation.HasAuthorisationConfig;
import stroom.security.HasTokenConfig;
import stroom.security.TokenConfig;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public class Config extends Configuration
        implements  HasAuthorisationConfig,
                    HasTokenConfig,
                    HasElasticConfig {

    @Valid
    @NotNull
    @JsonProperty("elastic")
    private ElasticConfig elasticConfig;

    @NotNull
    @JsonProperty("token")
    private TokenConfig tokenConfig;

    @NotNull
    @JsonProperty("authorisationService")
    private AuthorisationServiceConfig authorisationServiceConfig;

    @Override
    public TokenConfig getTokenConfig() {
        return tokenConfig;
    }

    @Override
    public AuthorisationServiceConfig getAuthorisationServiceConfig() {
        return authorisationServiceConfig;
    }

    public ElasticConfig getElasticConfig() {
        return elasticConfig;
    }
}
