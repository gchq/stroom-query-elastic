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


public class IndexJobWriteActor extends AbstractActor {
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexJobWriteActor.class);

    public static Props props(final IndexJobHandler jobHandler) {
        return Props.create(new Creator<Actor>() {
            @Override
            public Actor create() throws Exception {
                return new IndexJobWriteActor(jobHandler);
            }
        });
    }

    private final IndexJobHandler jobHandler;

    private IndexJobWriteActor(final IndexJobHandler jobHandler) {
        this.jobHandler = jobHandler;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(IndexJobMessages.WriteIndexJob.class, writeJob -> {
                    final CompletableFuture<IndexJob> result =
                            CompletableFuture.supplyAsync(() -> jobHandler.write(writeJob.getIndexJob(), writeJob.getSearchResponse()));

                    pipe(result, getContext().dispatcher()).to(getSender());
                })
                .build();
    }
}
