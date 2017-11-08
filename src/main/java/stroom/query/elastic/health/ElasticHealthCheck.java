package stroom.query.elastic.health;

import com.codahale.metrics.health.HealthCheck;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterIndexHealth;

public class ElasticHealthCheck extends HealthCheck {

    private final TransportClient client;

    public ElasticHealthCheck(TransportClient client) {
        this.client = client;
    }

    @Override
    protected Result check() throws Exception {
        final ClusterHealthResponse healths = client.admin().cluster().prepareHealth().get();
        final String clusterName = healths.getClusterName();
        final int numberOfDataNodes = healths.getNumberOfDataNodes();
        final int numberOfNodes = healths.getNumberOfNodes();

        final StringBuilder healthMsg = new StringBuilder();
        boolean firstSeen = false;
        
        healthMsg.append(String.format("Cluster Name: %s, ", clusterName));
        healthMsg.append(String.format("Number Data Nodes: %d, ", numberOfDataNodes));
        healthMsg.append(String.format("Number of Nodes: %d, ", numberOfNodes));
        
        for (ClusterIndexHealth health : healths.getIndices().values()) {
            if (firstSeen) {
                healthMsg.append(", ");
            } else {
                firstSeen = true;
            }
            
            final String index = health.getIndex();
            final int numberOfShards = health.getNumberOfShards();
            final int numberOfReplicas = health.getNumberOfReplicas();
            final ClusterHealthStatus status = health.getStatus();

            healthMsg.append(String.format("Index: %s (", index));
            healthMsg.append(String.format("Number of Shards: %d, ", numberOfShards));
            healthMsg.append(String.format("Number of Replicas: %d, ", numberOfReplicas));
            healthMsg.append(String.format("Status: %s)", status));
        }

        return Result.healthy(healthMsg.toString());
    }
}
