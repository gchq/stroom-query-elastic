package stroom.autoindex.akka;

import akka.actor.ActorSystem;
import io.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeUnit;

/**
 * Wrapper around the Akka Actor System that Dropwizard can lifecycle manage.
 */
public class ManagedActorSystem implements Managed {
    private static final Logger LOGGER = LoggerFactory.getLogger(ManagedActorSystem.class);

    private final ActorSystem actorSystem;

    public ManagedActorSystem() {
        this.actorSystem = ActorSystem.create();
    }

    public ActorSystem getActorSystem() {
        return actorSystem;
    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public void stop() throws Exception {
        LOGGER.info("Shutting down Actor System");

        actorSystem.terminate();

        try {
            Await.ready(actorSystem.whenTerminated(), Duration.create(10, TimeUnit.SECONDS));
        } catch (final Exception e) {
            LOGGER.info("Could not terminate actor system nicely");
        }
    }
}
