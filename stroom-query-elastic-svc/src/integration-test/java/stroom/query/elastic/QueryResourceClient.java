package stroom.query.elastic;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import stroom.query.api.v2.DocRef;
import stroom.query.api.v2.QueryKey;
import stroom.query.api.v2.SearchRequest;
import stroom.query.audit.QueryGenericResource;

import javax.ws.rs.core.MediaType;

public class QueryResourceClient implements QueryGenericResource<HttpResponse<String>, UnirestException> {

    private final String queryDataSourceUrl;
    private final String querySearchUrl;
    private final String queryDestroyUrl;

    public QueryResourceClient(final String baseUrl) {
        this.queryDataSourceUrl = String.format("%s/dataSource", baseUrl);
        this.querySearchUrl = String.format("%s/search", baseUrl);
        this.queryDestroyUrl = String.format("%s/destroy", baseUrl);
    }

    public HttpResponse<String> getDataSource(final DocRef docRef) throws UnirestException {
        return Unirest
                .post(queryDataSourceUrl)
                .header("accept", MediaType.APPLICATION_JSON)
                .header("Content-Type", MediaType.APPLICATION_JSON)
                .body(docRef)
                .asString();
    }

    public HttpResponse<String> search(final SearchRequest searchRequest) throws UnirestException {
        return Unirest
                .post(querySearchUrl)
                .header("accept", MediaType.APPLICATION_JSON)
                .header("Content-Type", MediaType.APPLICATION_JSON)
                .body(searchRequest)
                .asString();
    }

    public HttpResponse<String> destroy(final QueryKey queryKey) throws UnirestException {
        return Unirest
                .delete(queryDestroyUrl)
                .header("accept", MediaType.APPLICATION_JSON)
                .header("Content-Type", MediaType.APPLICATION_JSON)
                .body(queryKey)
                .asString();
    }

}
