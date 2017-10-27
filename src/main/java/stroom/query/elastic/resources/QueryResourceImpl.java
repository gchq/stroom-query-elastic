package stroom.query.elastic.resources;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.query.api.v2.DocRef;
import stroom.query.api.v2.QueryKey;
import stroom.query.api.v2.SearchRequest;

import javax.ws.rs.core.Response;

public class QueryResourceImpl implements QueryResource {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryResourceImpl.class);

    private final TransportClient client;

    public QueryResourceImpl(final TransportClient client) {
        this.client = client;
    }

    @Override
    public Response getDataSource(DocRef docRef) {
        return Response.ok("Get Data Source").build();
    }

    @Override
    public Response search(SearchRequest request) {
        GetResponse response = client.prepareGet("index", "type", "1")
                .setOperationThreaded(false)
                .get();

        LOGGER.info("Found " + response);

        return Response.ok("Search").build();
    }

    @Override
    public Response destroy(QueryKey queryKey) {
        return Response.ok("Destroy").build();
    }
}
