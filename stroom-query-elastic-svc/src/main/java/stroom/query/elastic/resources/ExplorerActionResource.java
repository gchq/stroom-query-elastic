package stroom.query.elastic.resources;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/explorerAction/v1")
@Produces(MediaType.APPLICATION_JSON)
public interface ExplorerActionResource<E> {

    /**
     * Called to create or update
     * @param data The updated configuration
     * @return
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/{uuid}")
    Response set(@PathParam("uuid") String uuid, E data);

    /**
     * Called to load an existing Document by UUID
     * @param uuid The UUID of the document
     * @return The document configuration
     */
    @GET
    @Path("/{uuid}")
    Response get(@PathParam("uuid") String uuid);

    @DELETE
    @Path("/{uuid}")
    Response remove(@PathParam("uuid") String uuid);
}
