package stroom.akka;

import stroom.security.ServiceUser;

import java.io.Serializable;

public abstract class ApiMessage implements Serializable {
    final String docRefType;
    final ServiceUser user;

    protected ApiMessage(final ServiceUser user,
                         final String docRefType) {
        this.user = user;
        this.docRefType = docRefType;
    }

    public ServiceUser getUser() {
        return user;
    }

    public String getDocRefType() {
        return docRefType;
    }
}
