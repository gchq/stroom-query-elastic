package stroom.query.akka;

import akka.actor.AbstractActor;
import akka.actor.Props;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.query.api.v2.DocRef;
import stroom.query.audit.service.QueryApiException;
import stroom.query.audit.service.QueryService;
import stroom.security.ServiceUser;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static akka.pattern.PatternsCS.pipe;

public class QueryDataSourceActor extends AbstractActor {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryDataSourceActor.class);

    public static Props props(final Function<String, Optional<QueryService>> serviceSupplier) {
        return Props.create(QueryDataSourceActor.class,
                () -> new QueryDataSourceActor(serviceSupplier));
    }

    private final Function<String, Optional<QueryService>> serviceSupplier;

    public QueryDataSourceActor(final Function<String, Optional<QueryService>> serviceSupplier) {
        this.serviceSupplier = serviceSupplier;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder().match(QueryApiMessages.DataSourceJob.class,
                dataSourceJob -> {
                    final ServiceUser user = dataSourceJob.getUser();
                    final DocRef docRef = dataSourceJob.getDocRef();

                    LOGGER.debug("Fetching Data Source for {}", docRef);

                    final CompletableFuture<QueryApiMessages.DataSourceJobComplete> result =
                            CompletableFuture.supplyAsync(() -> {
                                try {
                                    final QueryService queryService = this.serviceSupplier.apply(docRef.getType())
                                            .orElseThrow(() -> new RuntimeException("Could not find query service for " + docRef.getType()));

                                    return queryService.getDataSource(user, docRef)
                                            .orElseThrow(() -> new QueryApiException("Could not get response"));
                                } catch (QueryApiException e) {
                                    LOGGER.error("Failed to run search", e);
                                    throw new RuntimeException(e);
                                } catch (RuntimeException e) {
                                    LOGGER.error("Failed to run search", e);
                                    throw e;
                                }
                            })
                                    .thenApply(d -> QueryApiMessages.complete(dataSourceJob, d))
                                    .exceptionally(e -> QueryApiMessages.failed(dataSourceJob, e));

                    pipe(result, getContext().dispatcher()).to(getSender());
                })
                .build();
    }
}
