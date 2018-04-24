package stroom.akka;

import java.io.Serializable;

public abstract class JobComplete<REQUEST extends ApiMessage, RESPONSE> implements Serializable {
    private final REQUEST request;
    private final RESPONSE response;
    private final String error;

    protected JobComplete(final REQUEST request,
                        final RESPONSE response,
                        final String error) {
        this.request = request;
        this.response = response;
        this.error = error;
    }

    public REQUEST getRequest() {
        return request;
    }

    public RESPONSE getResponse() {
        return response;
    }

    public String getError() {
        return error;
    }
}