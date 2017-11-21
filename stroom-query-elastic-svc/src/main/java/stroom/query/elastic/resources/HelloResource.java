package stroom.query.elastic.resources;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;

@Path("/hello/v1")
public class HelloResource {

    public static final String WELCOME_TEXT = "Welcome to the Stroom Query Elastic Service";

    @GET
    public Response hello() {
        return Response
                .ok(WELCOME_TEXT)
                .build();
    }
}
