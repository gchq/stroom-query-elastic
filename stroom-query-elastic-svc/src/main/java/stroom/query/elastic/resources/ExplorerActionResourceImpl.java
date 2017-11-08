package stroom.query.elastic.resources;

import org.apache.http.HttpStatus;
import stroom.query.elastic.hibernate.ElasticIndexConfig;
import stroom.query.elastic.hibernate.ElasticIndexConfigService;

import javax.inject.Inject;
import javax.ws.rs.core.Response;

public class ExplorerActionResourceImpl implements ExplorerActionResource<ElasticIndexConfig> {

    private final ElasticIndexConfigService service;

    @Inject
    public ExplorerActionResourceImpl(final ElasticIndexConfigService service) {
        this.service = service;
    }

    @Override
    public Response createOrUpdate(ElasticIndexConfig data) {
        return service.createOrUpdate(data)
                .map(d -> Response.ok(d).build())
                .orElse(Response.noContent().build());
    }

    @Override
    public Response get(String uuid) {
        return service.get(uuid)
                .map(d -> Response.ok(d).build())
                .orElse(Response.status(HttpStatus.SC_NOT_FOUND).build());
    }

    @Override
    public Response delete(String uuid) {
        service.destroy(uuid);

        return Response.noContent().build();
    }
}
