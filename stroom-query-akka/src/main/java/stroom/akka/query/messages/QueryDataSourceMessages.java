package stroom.akka.query.messages;

import stroom.datasource.api.v2.DataSource;
import stroom.query.api.v2.DocRef;

public interface QueryDataSourceMessages {

    static JobComplete complete(final DocRef request,
                                final DataSource response) {
        return new JobComplete(request, response, null);
    }

    static JobComplete failed(final DocRef request,
                              final Throwable exception) {
        return new JobComplete(request, null, exception.getLocalizedMessage());
    }

    class JobComplete extends stroom.akka.JobComplete<DocRef, DataSource> {
        private JobComplete(final DocRef request,
                            final DataSource response,
                            final String error) {
            super(request, response, error);
        }
    }
}
