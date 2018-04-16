package stroom.query.akka;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.query.api.v2.SearchRequest;
import stroom.query.audit.security.ServiceUser;
import stroom.query.audit.service.QueryApiException;
import stroom.query.audit.service.QueryService;

import java.util.concurrent.CompletableFuture;

import static akka.pattern.PatternsCS.pipe;

public class SearchBackendActor extends AbstractActor {
    private static final Logger LOGGER = LoggerFactory.getLogger(SearchBackendActor.class);

    public static Props props(final String docRefType,
                              final ServiceUser user,
                              final QueryService queryService,
                              final ActorRef recipient) {
        return Props.create(SearchBackendActor.class,
                () -> new SearchBackendActor(docRefType, user, queryService, recipient));
    }

    private final String docRefType;
    private final ServiceUser user;
    private final QueryService queryService;
    private final ActorRef recipient;

    public SearchBackendActor(final String docRefType,
                              final ServiceUser user,
                              final QueryService queryService,
                              final ActorRef recipient) {
        this.docRefType = docRefType;
        this.user = user;
        this.queryService = queryService;
        this.recipient = recipient;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder().match(SearchMessages.SearchJob.class,
                (j) -> j.getDocRefType().equals(this.docRefType),
                searchJob -> {
                    final SearchRequest request = searchJob.getRequest();

                    LOGGER.debug("Handling Search Request {}", request);

                    final CompletableFuture<SearchMessages.SearchJobComplete> result =
                            CompletableFuture.supplyAsync(() -> {
                                try {
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
                                    .thenApply(d -> new SearchMessages.SearchJobComplete(request, d))
                                    .exceptionally(e -> new SearchMessages.SearchJobComplete(request, e));

                    pipe(result, getContext().dispatcher()).to(this.recipient);
                })
                .build();
    }
}
