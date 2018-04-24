package stroom.autoindex.search;

import akka.actor.AbstractActor;
import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.Creator;
import stroom.query.akka.QueryApiMessages;

public class FederatedSearchActor extends AbstractActor {

    public static Props props(final ActorRef searchSplitter,
                              final ActorRef searchActor,
                              final ActorRef searchMerge,
                              final ActorRef errorHandler) {
        return Props.create(new Creator<Actor>() {
            @Override
            public Actor create() throws Exception {
                return new FederatedSearchActor(searchSplitter, searchActor, searchMerge, errorHandler);
            }
        });
    }

    private final ActorRef searchSplitter;
    private final ActorRef searchActor;
    private final ActorRef searchMerger;
    private final ActorRef errorHandler;

    public FederatedSearchActor(final ActorRef searchSplitter,
                                final ActorRef searchActor,
                                final ActorRef searchMerger,
                                final ActorRef errorHandler) {
        this.searchSplitter = searchSplitter;
        this.searchActor = searchActor;
        this.searchMerger = searchMerger;
        this.errorHandler = errorHandler;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(QueryApiMessages.SearchJob.class, searchJob -> {
                    searchSplitter.tell(searchJob, getSelf());
                })
                .match(AutoIndexMessages.SplitSearchJobComplete.class, splitComplete -> {
                    if (null != splitComplete.getSplitSearchRequest()) {
                        splitComplete.getSplitSearchRequest().getRequests()
                                .forEach((docRef, requestsByTracker) -> {
                            requestsByTracker.forEach((tw, searchRequest) -> {
                                searchActor.tell(QueryApiMessages.search(splitComplete.getUser(), docRef.getType(), searchRequest), getSelf());
                            });

                        });
                    } else if (null != splitComplete.getError()) {
                        final String msg = String.format("Could not split search: %s", splitComplete.getError());
                        errorHandler.tell(QueryApiMessages.error(msg), getSelf());
                    }
                })
                .match(QueryApiMessages.SearchJobComplete.class, searchJobComplete -> {

                })
                .build();
    }
}
