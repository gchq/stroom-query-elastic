package stroom.query.elastic.transportClient;

import io.dropwizard.Configuration;
import io.dropwizard.ConfiguredBundle;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

public abstract class TransportClientBundle<T extends Configuration> implements ConfiguredBundle<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(TransportClientBundle.class);

    private TransportClient transportClient;

    public TransportClient getTransportClient() {
        return transportClient;
    }

    @Override
    public void run(final T configuration, final Environment environment) throws Exception {
        final Settings settings = Settings.builder()
                .put("cluster.name", getClusterName(configuration)).build();
        transportClient = new PreBuiltTransportClient(settings);

        getHosts(configuration).forEach((hostname, port) -> {
            try {
                LOGGER.info(String.format("Elastic Connecting to %s:%d", hostname, port));
                transportClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(hostname), port));
            } catch (UnknownHostException e) {
                LOGGER.warn(String.format("Could not configure client connection to %s:%d", hostname, port));
            }
        });

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

    protected abstract Map<String, Integer> getHosts(T config);

    protected abstract String getClusterName(T config);
}
