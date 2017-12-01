package stroom.query.elastic.resources;

import stroom.query.elastic.hibernate.ElasticIndexConfig;
import stroom.query.elastic.service.ElasticDocRefService;
import stroom.util.shared.QueryApiException;

import javax.inject.Inject;
import javax.ws.rs.core.Response;

public class ElasticIndexResourceImpl implements ElasticIndexResource {

    private final ElasticDocRefService service;

    @Inject
    public ElasticIndexResourceImpl(final ElasticDocRefService service) {
        this.service = service;
    }

    @Override
    public Response update(String uuid, ElasticIndexConfig updatedConfig) throws QueryApiException {
        return service.update(uuid, updatedConfig)
                .map(d -> Response.ok(d).build())
                .orElse(Response.noContent().build());
    }
}
