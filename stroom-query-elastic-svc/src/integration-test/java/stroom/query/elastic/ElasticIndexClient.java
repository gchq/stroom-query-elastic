package stroom.query.elastic;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import stroom.query.elastic.hibernate.ElasticIndexConfig;
import stroom.query.elastic.resources.ElasticIndexGenericResource;

import javax.ws.rs.core.MediaType;
import java.util.function.Function;

public class ElasticIndexClient implements ElasticIndexGenericResource<HttpResponse<String>, UnirestException> {
    private final String baseUrl;
    private final Function<String, String> updateUrl;

    public ElasticIndexClient(final String baseUrl) {
        this.baseUrl = baseUrl;
        this.updateUrl = (uuid) -> String.format("%s/%s", this.baseUrl, uuid);
    }

    @Override
    public HttpResponse<String> update(final String uuid,
                                       final ElasticIndexConfig updatedConfig) throws UnirestException {
        return Unirest
                .put(this.updateUrl.apply(uuid))
                .header("accept", MediaType.APPLICATION_JSON)
                .header("Content-Type", MediaType.APPLICATION_JSON)
                .body(updatedConfig)
                .asString();
    }
}
