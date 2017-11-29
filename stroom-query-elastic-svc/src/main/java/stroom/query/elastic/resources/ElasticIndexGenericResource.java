package stroom.query.elastic.resources;

import stroom.query.elastic.hibernate.ElasticIndexConfig;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

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