package stroom.autoindex;

import org.jooq.DSLContext;
import org.jooq.impl.DSL;

public class DSLContextBuilder {
    private final String url;
    private String username;
    private String password;

    public static DSLContextBuilder withUrl(final String url) {
        return new DSLContextBuilder(url);
    }

    private DSLContextBuilder(final String url) {
        this.url = url;
    }

    public DSLContextBuilder username(final String username) {
        this.username = username;
        return this;
    }

    public DSLContextBuilder password(final String password) {
        this.password = password;
        return this;
    }

    public DSLContext build() {
        if ((null != username) && (null != password)) {
            return DSL.using(this.url, this.username, this.password);
        } else {
            return DSL.using(this.url);
        }
    }
}
