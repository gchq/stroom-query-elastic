package stroom.query.elastic;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.Map;

public class ElasticConfig {

    @Valid
    @NotNull
    @JsonProperty("hosts")
    private Map<String, Integer> hosts;

    @Valid
    @NotNull
    @JsonProperty("clusterName")
    private String clusterName;

    public String getClusterName() {
        return clusterName;
    }

    public Map<String, Integer> getHosts() {
        return hosts;
    }
}
