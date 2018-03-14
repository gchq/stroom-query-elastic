package stroom.query.elastic;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public class ElasticConfig {
    public static final String ENTRY_DELIMITER = ",";
    public static final String HOST_PORT_DELIMITER = ":";

    @Valid
    @NotNull
    @JsonProperty("transportHosts")
    private String transportHosts;

    @Valid
    @NotNull
    @JsonProperty("httpHost")
    private String httpHost;

    @Valid
    @NotNull
    @JsonProperty("clusterName")
    private String clusterName;

    public String getClusterName() {
        return clusterName;
    }

    public String getTransportHosts() {
        return transportHosts;
    }

    public String getHttpHost() {
        return httpHost;
    }
}
