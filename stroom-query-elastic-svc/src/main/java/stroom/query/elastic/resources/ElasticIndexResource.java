package stroom.query.elastic.resources;

import stroom.query.audit.DocRefException;

import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/elasticIndex/v1")
@Produces(MediaType.APPLICATION_JSON)
public interface ElasticIndexResource extends ElasticIndexGenericResource<Response, DocRefException> {
}
