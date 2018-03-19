package stroom.autoindex;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import stroom.query.audit.authorisation.AuthorisationServiceConfig;
import stroom.query.audit.authorisation.HasAuthorisationConfig;
import stroom.query.audit.security.HasTokenConfig;
import stroom.query.audit.security.TokenConfig;
import stroom.query.elastic.ElasticConfig;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.Map;

public class Config extends Configuration implements HasAuthorisationConfig, HasTokenConfig {
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

    private Map<String, String> queryResourceUrlsByType;

    public Map<String, String> getQueryResourceUrlsByType() {
        return queryResourceUrlsByType;
    }

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
