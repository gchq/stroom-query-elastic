package stroom.autoindex.indexing;

import akka.actor.AbstractActor;
import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.Creator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

import static akka.pattern.PatternsCS.pipe;


public class IndexJobSearchActor extends AbstractActor {
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexJobSearchActor.class);

    public static Props props(final IndexJobHandler jobHandler) {
        return Props.create(new Creator<Actor>() {
            @Override
            public Actor create() throws Exception {
                return new IndexJobSearchActor(jobHandler);
            }
        });
    }

    private final IndexJobHandler jobHandler;

    private IndexJobSearchActor(final IndexJobHandler jobHandler) {
        this.jobHandler = jobHandler;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(IndexJobMessages.SearchIndexJob.class, indexJob -> {
                    final CompletableFuture<IndexJobMessages.WriteIndexJob> result =
                            CompletableFuture.supplyAsync(() -> jobHandler.search(indexJob.getIndexJob()))
                            .thenApply(s -> IndexJobMessages.write(indexJob.getIndexJob(), s));

                    // Send to self, so it can then be forwarded to original sender and we close ourselves down
                    pipe(result, getContext().dispatcher()).to(getSender());
                })
                .build();
    }
}
