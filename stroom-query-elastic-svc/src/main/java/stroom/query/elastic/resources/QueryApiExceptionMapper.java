package stroom.query.elastic.resources;

import org.elasticsearch.index.IndexNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.query.elastic.hibernate.ElasticIndexConfig;
import stroom.util.shared.QueryApiException;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class QueryApiExceptionMapper implements ExceptionMapper<QueryApiException> {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryApiExceptionMapper.class);

    @Override
    public Response toResponse(final QueryApiException e) {
        LOGGER.warn("Exception seen on REST interface", e);

        if (e.getCause() instanceof IndexNotFoundException) {
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(new ElasticIndexConfig())
                    .build();
        }

        return Response.serverError().entity(e.getLocalizedMessage()).build();
    }
}
