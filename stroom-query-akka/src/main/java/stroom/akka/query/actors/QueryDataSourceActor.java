package stroom.akka.query.actors;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.akka.query.messages.QueryDataSourceMessages;
import stroom.query.api.v2.DocRef;
import stroom.query.audit.service.QueryApiException;
import stroom.query.audit.service.QueryService;
import stroom.query.audit.service.QueryServiceSupplier;
import stroom.security.ServiceUser;

import java.util.concurrent.CompletableFuture;

import static akka.pattern.PatternsCS.pipe;

public class QueryDataSourceActor extends AbstractActor {

    private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    public static Props props(final ServiceUser user,
                              final QueryService service) {
        return Props.create(QueryDataSourceActor.class,
                () -> new QueryDataSourceActor(user, service));
    }

    private final ServiceUser user;
    private final QueryService service;

    public QueryDataSourceActor(final ServiceUser user,
                                final QueryService service) {
        this.user = user;
        this.service = service;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(DocRef.class, this::handleDataSource)
                .build();
    }

    private void handleDataSource(final DocRef docRef) {

        log.debug("Fetching Data Source for {}", docRef);

        final CompletableFuture<QueryDataSourceMessages.JobComplete> result =
                CompletableFuture.supplyAsync(() -> {
                    try {
                        return service.getDataSource(user, docRef)
                                .orElseThrow(() -> new QueryApiException("Could not get response"));
                    } catch (QueryApiException e) {
                        log.error("Failed to run search", e);
                        throw new RuntimeException(e);
                    } catch (RuntimeException e) {
                        log.error("Failed to run search", e);
                        throw e;
                    }
                })
                        .thenApply(d -> QueryDataSourceMessages.complete(docRef, d))
                        .exceptionally(e -> QueryDataSourceMessages.failed(docRef, e));

        pipe(result, getContext().dispatcher()).to(getSender());
    }
}
