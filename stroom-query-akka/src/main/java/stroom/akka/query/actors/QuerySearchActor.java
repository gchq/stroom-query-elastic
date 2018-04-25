package stroom.akka.query.actors;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.akka.query.messages.QuerySearchMessages;
import stroom.query.api.v2.SearchRequest;
import stroom.query.audit.service.QueryApiException;
import stroom.query.audit.service.QueryService;
import stroom.query.audit.service.QueryServiceSupplier;
import stroom.security.ServiceUser;

import java.util.concurrent.CompletableFuture;

import static akka.pattern.PatternsCS.pipe;

public class QuerySearchActor extends AbstractActor {

    private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    public static Props props(final ServiceUser user,
                              final QueryService service) {
        return Props.create(QuerySearchActor.class,
                () -> new QuerySearchActor(user, service));
    }

    private final ServiceUser user;
    private final QueryService service;

    public QuerySearchActor(final ServiceUser user,
                            final QueryService service) {
        this.user = user;
        this.service = service;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(SearchRequest.class, this::handleSearch)
                .build();
    }

    private void handleSearch(final SearchRequest request) {
        log.info("Handling Search Request {}", request);

        final CompletableFuture<QuerySearchMessages.JobComplete> result =
                CompletableFuture.supplyAsync(() -> {
                    try {
                        return service.search(user, request)
                                .orElseThrow(() -> new QueryApiException("Could not get response"));
                    } catch (QueryApiException e) {
                        log.error("Failed to run search", e);
                        throw new RuntimeException(e);
                    } catch (RuntimeException e) {
                        log.error("Failed to run search", e);
                        throw e;
                    }
                })
                        .thenApply(d -> QuerySearchMessages.complete(request, d))
                        .exceptionally(e -> QuerySearchMessages.failed(request, e));

        pipe(result, getContext().dispatcher()).to(getSender());
    }
}
