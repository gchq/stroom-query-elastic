package stroom.akka.query.cluster;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.ReceiveTimeout;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.routing.FromConfig;
import scala.concurrent.duration.Duration;
import stroom.query.api.v2.DocRef;
import stroom.query.api.v2.SearchRequest;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class QuerySearchFrontend extends AbstractActor {

    public static Props props(final CompletableFuture<Boolean> isReady) {
        return Props.create(QuerySearchFrontend.class, () -> new QuerySearchFrontend(isReady));
    }

    private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    private final CompletableFuture<Boolean> isReady;
    private ActorRef backend;

    private QuerySearchFrontend(final CompletableFuture<Boolean> isReady) {
        this.isReady = isReady;
    }

    @Override
    public void preStart() throws Exception {
        backend = getContext().actorOf(FromConfig.getInstance().props(),
                "querySearchBackendRouter");
        getContext().setReceiveTimeout(Duration.create(1, TimeUnit.SECONDS));
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(DocRef.class, d -> {
                    log.info("Forwarding DataSource job to Backend");
                    backend.tell(d, getSender());
                })
                .match(SearchRequest.class, r -> {
                    log.info("Forwarding Search job to Backend");
                    backend.tell(r, getSender());
                })
                .match(ReceiveTimeout.class, message -> {
                    log.info("Attempting to ping backend");
                    backend.tell(Boolean.TRUE, getSelf());
                })
                .match(Boolean.class, b -> {
                    log.info("One of the workers has responded");
                    isReady.complete(b);
                })
                .build();
    }
}
