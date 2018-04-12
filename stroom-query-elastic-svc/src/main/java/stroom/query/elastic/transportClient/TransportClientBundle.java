package stroom.query.elastic.transportClient;

import io.dropwizard.Configuration;
import io.dropwizard.ConfiguredBundle;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.query.elastic.config.ElasticConfig;
import stroom.query.elastic.config.HasElasticConfig;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;

public class TransportClientBundle<T extends Configuration & HasElasticConfig> implements ConfiguredBundle<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(TransportClientBundle.class);

    private TransportClient transportClient;

    public TransportClient getTransportClient() {
        return transportClient;
    }

    public static TransportClient createTransportClient(final HasElasticConfig hasElasticConfig) {
        final ElasticConfig elasticConfig = hasElasticConfig.getElasticConfig();

        final Settings settings = Settings.builder()
                .put("cluster.name", elasticConfig.getClusterName()).build();
        final TransportClient tClient = new PreBuiltTransportClient(settings);

        Arrays.stream(elasticConfig.getTransportHosts().split(ElasticConfig.ENTRY_DELIMITER))
                .map(h -> h.split(ElasticConfig.HOST_PORT_DELIMITER))
                .filter(h -> (h.length == 2))
                .map(h -> new Tuple<>(h[0], Integer.parseInt(h[1])))
                .forEach(tuple -> {
                    final String hostname = tuple.v1();
                    final Integer port = tuple.v2();
                    try {
                        LOGGER.info(String.format("Elastic Connecting to %s:%d", hostname, port));
                        tClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(hostname), port));
                    } catch (UnknownHostException e) {
                        LOGGER.warn(String.format("Could not configure client connection to %s:%d", hostname, port));
                    }
                });

        return tClient;
    }

    @Override
    public void run(final T configuration, final Environment environment) throws Exception {
        transportClient = createTransportClient(configuration);

        environment.lifecycle().manage(new Managed() {
            @Override
            public void start() throws Exception {

            }

            @Override
            public void stop() throws Exception {
                LOGGER.info("Closing Elastic Transport Client");
                transportClient.close();
            }
        });
    }

    @Override
    public void initialize(final Bootstrap bootstrap) {

    }
}
