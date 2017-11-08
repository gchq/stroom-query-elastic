package stroom.query.elastic;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public class Config extends Configuration {

    @Valid
    @NotNull
    @JsonProperty("elastic")
    private ElasticConfig elasticConfig;

    public ElasticConfig getElasticConfig() {
        return elasticConfig;
    }
}
