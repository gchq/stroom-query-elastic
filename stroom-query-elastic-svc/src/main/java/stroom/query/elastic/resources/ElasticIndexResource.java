package stroom.query.elastic.resources;

import stroom.query.elastic.hibernate.ElasticIndexConfig;
import stroom.util.shared.QueryApiException;

import javax.ws.rs.Consumes;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * This is the implementation specific resource for updating an Elastic Index Config
 */
@Path("/elasticIndex/v1")
@Produces(MediaType.APPLICATION_JSON)
public interface ElasticIndexResource {

    /**
     * Called to create or update
     * @param updatedConfig The updated configuration
     * @return
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/{uuid}")
    Response update(@PathParam("uuid") String uuid, ElasticIndexConfig updatedConfig) throws QueryApiException;
}
