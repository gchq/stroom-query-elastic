package stroom.akka.query.cluster;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import stroom.akka.query.actors.QueryDataSourceActor;
import stroom.akka.query.actors.QuerySearchActor;
import stroom.query.api.v2.DocRef;
import stroom.query.api.v2.SearchRequest;
import stroom.query.audit.service.QueryService;
import stroom.security.ServiceUser;

public class QuerySearchBackend extends AbstractActor {

    private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    public static Props props(final ServiceUser user,
                              final QueryService service) {
        return Props.create(QuerySearchBackend.class,
                () -> new QuerySearchBackend(user, service));
    }

    private final ServiceUser user;
    private final QueryService service;
    private ActorRef dataSourceActor;
    private ActorRef searchActor;

    public QuerySearchBackend(final ServiceUser user,
                            final QueryService service) {
        this.user = user;
        this.service = service;
    }

    @Override
    public void preStart() throws Exception {
        this.dataSourceActor = getContext().actorOf(QueryDataSourceActor.props(user, service));
        this.searchActor = getContext().actorOf(QuerySearchActor.props(user, service));
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(DocRef.class, d -> {
                    log.info("Backend Handling DataSource Job");
                    dataSourceActor.tell(d, getSender());
                })
                .match(SearchRequest.class, r -> {
                    log.info("Backend Handling Search Job");
                    searchActor.tell(r, getSender());
                })
                .match(Boolean.class, b -> {
                    log.info("Received Ping from Frontend");
                    getSender().tell(b, getSelf());
                }) // echo booleans
                .build();
    }
}
