package stroom.autoindex.indexing;

import akka.actor.AbstractActor;
import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.Creator;

public class IndexJobActor extends AbstractActor {

    public static Props props(final ActorRef replyTo,
                              final IndexJobHandler indexJobHandler) {
        return Props.create(new Creator<Actor>() {
            @Override
            public Actor create() throws Exception {
                return new IndexJobActor(replyTo, indexJobHandler);
            }
        });
    }

    // When the first message comes in, keep record of who sent it so the eventual
    // result can be returned to them
    private final ActorRef replyTo;
    private final IndexJobHandler indexJobHandler;

    private IndexJobActor(final ActorRef replyTo,
                          final IndexJobHandler indexJobHandler) {
        this.replyTo = replyTo;
        this.indexJobHandler = indexJobHandler;

    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(IndexJobMessages.SearchIndexJob.class, indexJob -> {
                    getContext().actorOf(IndexJobSearchActor.props(this.indexJobHandler))
                            .tell(indexJob, getSelf());
                })
                .match(IndexJobMessages.WriteIndexJob.class, writeJob ->
                        getContext().actorOf(IndexJobWriteActor.props(this.indexJobHandler))
                                .tell(writeJob, getSelf())
                )
                .match(IndexJob.class, indexJob -> {
                    this.replyTo.tell(indexJob, getSelf());

                    // That's us done
                    getContext().stop(getSelf());
                })
                .build();
    }
}
