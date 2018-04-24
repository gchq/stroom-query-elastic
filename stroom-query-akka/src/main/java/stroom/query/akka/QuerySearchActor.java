package stroom.query.akka;

import akka.actor.AbstractActor;
import akka.actor.Props;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.query.api.v2.SearchRequest;
import stroom.security.ServiceUser;
import stroom.query.audit.service.QueryApiException;
import stroom.query.audit.service.QueryService;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static akka.pattern.PatternsCS.pipe;

public class QuerySearchActor extends AbstractActor {
    private static final Logger LOGGER = LoggerFactory.getLogger(QuerySearchActor.class);

    public static Props props(final Function<String, Optional<QueryService>> serviceSupplier) {
        return Props.create(QuerySearchActor.class,
                () -> new QuerySearchActor(serviceSupplier));
    }

    private final Function<String, Optional<QueryService>> serviceSupplier;

    public QuerySearchActor(final Function<String, Optional<QueryService>> serviceSupplier) {
        this.serviceSupplier = serviceSupplier;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder().match(QueryApiMessages.SearchJob.class,
                searchJob -> {
                    final ServiceUser user = searchJob.getUser();
                    final SearchRequest request = searchJob.getRequest();

                    LOGGER.debug("Handling Search Request {}", request);

                    final CompletableFuture<QueryApiMessages.SearchJobComplete> result =
                            CompletableFuture.supplyAsync(() -> {
                                try {
                                    final QueryService queryService = this.serviceSupplier.apply(searchJob.getDocRefType())
                                            .orElseThrow(() -> new RuntimeException("Could not find query service for " + searchJob.getDocRefType()));

                                    return queryService.search(user, request)
                                            .orElseThrow(() -> new QueryApiException("Could not get response"));
                                } catch (QueryApiException e) {
                                    LOGGER.error("Failed to run search", e);
                                    throw new RuntimeException(e);
                                } catch (RuntimeException e) {
                                    LOGGER.error("Failed to run search", e);
                                    throw e;
                                }
                            })
                                    .thenApply(d -> QueryApiMessages.complete(searchJob, d))
                                    .exceptionally(e -> QueryApiMessages.failed(searchJob, e));

                    pipe(result, getContext().dispatcher()).to(getSender());
                })
                .build();
    }
}
