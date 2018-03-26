package stroom.autoindex;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.Valid;

public class IndexingConfig {

    @Valid
    @JsonProperty
    private Boolean enabled;
}
