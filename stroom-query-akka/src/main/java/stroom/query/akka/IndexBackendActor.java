package stroom.query.akka;

import akka.actor.AbstractActor;

public class IndexBackendActor extends AbstractActor {
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .build();
    }
}
