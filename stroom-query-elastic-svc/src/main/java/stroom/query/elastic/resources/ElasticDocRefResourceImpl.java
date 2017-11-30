package stroom.query.elastic.resources;

import org.apache.http.HttpStatus;
import stroom.query.audit.DocRefException;
import stroom.query.audit.DocRefResource;
import stroom.query.elastic.hibernate.ElasticIndexConfig;
import stroom.query.elastic.service.ElasticDocRefService;

import javax.inject.Inject;
import javax.ws.rs.core.Response;

/**
 * This is the general Doc Ref resource that stroom will use to manage doc refs
 */
public class ElasticDocRefResourceImpl implements DocRefResource {
    private final ElasticDocRefService service;

    @Inject
    public ElasticDocRefResourceImpl(final ElasticDocRefService service) {
        this.service = service;
    }

    @Override
    public Response getAll() throws DocRefException {
        return Response.ok(service.getAll()).build();
    }

    @Override
    public Response get(String uuid) throws DocRefException {
        return service.get(uuid)
                .map(d -> Response.ok(d).build())
                .orElse(Response.status(HttpStatus.SC_NOT_FOUND)
                        .entity(new ElasticIndexConfig())
                        .build());
    }

    @Override
    public Response createDocument(final String uuid,
                                   final String name) throws DocRefException {
        return service.createDocument(uuid, name)
                .map(d -> Response.ok(d).build())
                .orElse(Response.status(HttpStatus.SC_NOT_FOUND)
                        .entity(new ElasticIndexConfig())
                        .build());
    }

    @Override
    public Response copyDocument(final String originalUuid,
                                 final String copyUuid) throws DocRefException {
        return service.copyDocument(originalUuid, copyUuid)
                .map(d -> Response.ok(d).build())
                .orElse(Response.status(HttpStatus.SC_NOT_FOUND)
                        .entity(new ElasticIndexConfig())
                        .build());
    }

    @Override
    public Response documentMoved(final String uuid) throws DocRefException {
        return service.documentMoved(uuid)
                .map(d -> Response.ok(d).build())
                .orElse(Response.status(HttpStatus.SC_NOT_FOUND)
                        .entity(new ElasticIndexConfig())
                        .build());
    }

    @Override
    public Response documentRenamed(final String uuid, final String name) throws DocRefException {
        return service.documentRenamed(uuid, name)
                .map(d -> Response.ok(d).build())
                .orElse(Response.status(HttpStatus.SC_NOT_FOUND)
                        .entity(new ElasticIndexConfig())
                        .build());
    }

    @Override
    public Response deleteDocument(final String uuid) throws DocRefException {
        service.deleteDocument(uuid);

        return Response.noContent().build();
    }
}
