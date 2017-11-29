package stroom.query.elastic;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import stroom.query.audit.DocRefGenericResource;

import javax.ws.rs.core.MediaType;
import java.util.function.BiFunction;
import java.util.function.Function;

public class DocRefResourceClient implements DocRefGenericResource<HttpResponse<String>, UnirestException> {
    private final String getAllUrl;
    private final Function<String, String> getUrl;
    private final BiFunction<String, String, String> createUrl;
    private final BiFunction<String, String, String> copyUrl;
    private final Function<String, String> moveUrl;
    private final BiFunction<String, String, String> renameUrl;
    private final Function<String, String> deleteUrl;

    public DocRefResourceClient(final String baseUrl) {
        this.getAllUrl = baseUrl;
        this.getUrl = (uuid) -> String.format("%s/%s", baseUrl, uuid);
        this.createUrl = (uuid, name) -> String.format("%s/create/%s/%s", baseUrl, uuid, name);
        this.copyUrl = (originalUuid, copyUuid) -> String.format("%s/copy/%s/%s", baseUrl, originalUuid, copyUuid);
        this.moveUrl = (uuid) -> String.format("%s/move/%s", baseUrl, uuid);
        this.renameUrl = (uuid, name) -> String.format("%s/rename/%s/%s", baseUrl, uuid, name);
        this.deleteUrl = (uuid) -> String.format("%s/delete/%s", baseUrl, uuid);
    }


    @Override
    public HttpResponse<String> getAll() throws UnirestException {
        return Unirest
                .get(this.getAllUrl)
                .header("accept", MediaType.APPLICATION_JSON)
                .asString();
    }

    @Override
    public HttpResponse<String> get(final String uuid) throws UnirestException {
        return Unirest
                .get(this.getUrl.apply(uuid))
                .header("accept", MediaType.APPLICATION_JSON)
                .asString();
    }

    @Override
    public HttpResponse<String> createDocument(final String uuid,
                                               final String name) throws UnirestException {
        return Unirest
                .post(this.createUrl.apply(uuid, name))
                .header("accept", MediaType.APPLICATION_JSON)
                .asString();
    }

    @Override
    public HttpResponse<String> copyDocument(final String originalUuid,
                                             final String copyUuid) throws UnirestException {
        return Unirest
                .post(this.copyUrl.apply(originalUuid, copyUuid))
                .header("accept", MediaType.APPLICATION_JSON)
                .asString();
    }

    @Override
    public HttpResponse<String> documentMoved(final String uuid) throws UnirestException {
        return Unirest
                .put(this.moveUrl.apply(uuid))
                .header("accept", MediaType.APPLICATION_JSON)
                .asString();
    }

    @Override
    public HttpResponse<String> documentRenamed(final String uuid,
                                                final String name) throws UnirestException {
        return Unirest
                .put(this.renameUrl.apply(uuid, name))
                .header("accept", MediaType.APPLICATION_JSON)
                .asString();
    }

    @Override
    public HttpResponse<String> deleteDocument(final String uuid) throws UnirestException {
        return Unirest
                .delete(this.deleteUrl.apply(uuid))
                .header("accept", MediaType.APPLICATION_JSON)
                .asString();
    }
}
