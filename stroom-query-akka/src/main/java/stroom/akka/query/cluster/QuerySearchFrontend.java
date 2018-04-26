package stroom.akka.query.cluster;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.routing.FromConfig;
import stroom.query.api.v2.SearchRequest;

public class QuerySearchFrontend extends AbstractActor {

    private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    private final ActorRef backend = getContext().actorOf(FromConfig.getInstance().props(),
            "querySearchBackendRouter");

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(SearchRequest.class, j -> {
                    log.info("Forwarding Search job to Backend");
                    backend.tell(j, getSender());
                })
                .build();
    }
}
