package stroom.autoindex.indexing;

import akka.actor.AbstractActor;
import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.Creator;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static akka.pattern.PatternsCS.pipe;


public class IndexJobActor extends AbstractActor {
    public static Props props(final Consumer<IndexJob> jobHandler,
                              final ActorRef postJobHandler) {
        return Props.create(new Creator<Actor>() {
            @Override
            public Actor create() throws Exception {
                return new IndexJobActor(jobHandler, postJobHandler);
            }
        });
    }

    private final Consumer<IndexJob> jobHandler;
    private final ActorRef postJobHandler;

    private IndexJobActor(final Consumer<IndexJob> jobHandler,
                          final ActorRef postJobHandler) {
        this.jobHandler = jobHandler;
        this.postJobHandler = postJobHandler;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(IndexJob.class, indexJob -> {

                    CompletableFuture<IndexJob> result =
                            CompletableFuture.supplyAsync(() -> {
                                jobHandler.accept(indexJob);
                                return indexJob;
                            });

                    pipe(result, getContext().dispatcher()).to(this.postJobHandler);
                }).build();
    }
}
