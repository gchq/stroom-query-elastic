package stroom.autoindex.indexing;

import akka.actor.AbstractActor;
import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.Creator;

import java.util.concurrent.CompletableFuture;

import static akka.pattern.PatternsCS.pipe;


public class IndexJobActor extends AbstractActor {
    public static Props props(final IndexJobHandler jobHandler,
                              final ActorRef postJobHandler) {
        return Props.create(new Creator<Actor>() {
            @Override
            public Actor create() throws Exception {
                return new IndexJobActor(jobHandler, postJobHandler);
            }
        });
    }

    private final IndexJobHandler jobHandler;
    private final ActorRef postJobHandler;

    private IndexJobActor(final IndexJobHandler jobHandler,
                          final ActorRef postJobHandler) {
        this.jobHandler = jobHandler;
        this.postJobHandler = postJobHandler;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(IndexJobMessages.SearchIndexJob.class, indexJob -> {

                    CompletableFuture<IndexJobMessages.WriteIndexJob> result =
                            CompletableFuture.supplyAsync(() -> jobHandler.search(indexJob.getIndexJob()))
                            .thenApply(s -> IndexJobMessages.write(indexJob.getIndexJob(), s));

                    pipe(result, getContext().dispatcher()).to(getSelf());
                })
                .match(IndexJobMessages.WriteIndexJob.class, writeJob -> {

                    CompletableFuture<IndexJobMessages.CompleteIndexJob> result =
                            CompletableFuture.supplyAsync(() -> jobHandler.write(writeJob.getIndexJob(), writeJob.getSearchResponse()))
                            .thenApply(IndexJobMessages::complete);

                    pipe(result, getContext().dispatcher()).to(getSelf());
                })
                .match(IndexJobMessages.CompleteIndexJob.class, completeJob -> {

                    CompletableFuture<IndexJob> result =
                            CompletableFuture.supplyAsync(() -> jobHandler.complete(completeJob.getIndexJob()));

                    pipe(result, getContext().dispatcher()).to(this.postJobHandler);
                })

                .build();
    }
}
