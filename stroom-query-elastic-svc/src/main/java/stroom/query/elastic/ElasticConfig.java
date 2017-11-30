package stroom.query.elastic;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public class ElasticConfig {
    public static final String ENTRY_DELIMITER = ",";
    public static final String HOST_PORT_DELIMITER = ":";

    @Valid
    @NotNull
    @JsonProperty("hosts")
    private String hosts;

    @Valid
    @NotNull
    @JsonProperty("clusterName")
    private String clusterName;

    public String getClusterName() {
        return clusterName;
    }

    public String getHosts() {
        return hosts;
    }
}
