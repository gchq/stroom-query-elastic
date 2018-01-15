package stroom.query.elastic.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import stroom.query.elastic.ElasticConfig;

import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public class Config extends Configuration {

    @Valid
    @NotNull
    @JsonProperty("elastic")
    private ElasticConfig elasticConfig;

    @Nullable
    @JsonProperty("token")
    private TokenConfig tokenConfig;

    public ElasticConfig getElasticConfig() {
        return elasticConfig;
    }

    public final TokenConfig getTokenConfig() {
        return tokenConfig;
    }
}
