package stroom.query.akka;

import akka.actor.AbstractActor;

public class SearchFrontendActor extends AbstractActor {
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .build();
    }
}
