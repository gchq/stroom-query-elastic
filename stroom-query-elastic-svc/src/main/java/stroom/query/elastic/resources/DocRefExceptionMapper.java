package stroom.query.elastic.resources;

import org.elasticsearch.index.IndexNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.query.audit.DocRefException;
import stroom.query.elastic.hibernate.ElasticIndexConfig;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class DocRefExceptionMapper implements ExceptionMapper<DocRefException> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DocRefExceptionMapper.class);

    @Override
    public Response toResponse(final DocRefException e) {
        LOGGER.warn("Exception seen on REST interface", e);

        if (e.getCause() instanceof IndexNotFoundException) {
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(new ElasticIndexConfig())
                    .build();
        }

        return Response.serverError().entity(e.getLocalizedMessage()).build();
    }
}
