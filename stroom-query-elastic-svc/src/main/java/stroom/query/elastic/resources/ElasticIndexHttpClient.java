package stroom.query.elastic.resources;

import stroom.query.audit.SimpleJsonHttpClient;
import stroom.query.elastic.hibernate.ElasticIndexConfig;
import stroom.util.shared.QueryApiException;

import javax.ws.rs.core.Response;
import java.util.function.Function;

public class ElasticIndexHttpClient implements ElasticIndexResource {
    private final Function<String, String> updateUrl;
    private final SimpleJsonHttpClient<QueryApiException> httpClient;

    public ElasticIndexHttpClient(final String baseUrl) {
        this.updateUrl = (uuid) -> String.format("%s/elasticIndex/v1/%s", baseUrl, uuid);
        this.httpClient = new SimpleJsonHttpClient<>(QueryApiException::new);
    }

    @Override
    public Response update(final String uuid,
                           final ElasticIndexConfig updatedConfig) throws QueryApiException {
        return httpClient
                .put(this.updateUrl.apply(uuid))
                .body(updatedConfig)
                .send();
    }
}
