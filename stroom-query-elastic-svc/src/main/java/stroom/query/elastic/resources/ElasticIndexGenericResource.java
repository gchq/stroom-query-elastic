package stroom.query.elastic.resources;

import stroom.query.elastic.hibernate.ElasticIndexConfig;

import javax.ws.rs.Consumes;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.MediaType;

/**
 * This is the implementation specific resource for updating an Elastic Index Config
 */
public interface ElasticIndexGenericResource<RESPONSE, EXCEPTION extends Throwable> {

    /**
     * Called to create or update
     * @param updatedConfig The updated configuration
     * @return
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/{uuid}")
    RESPONSE update(@PathParam("uuid") String uuid, ElasticIndexConfig updatedConfig) throws EXCEPTION;
}
