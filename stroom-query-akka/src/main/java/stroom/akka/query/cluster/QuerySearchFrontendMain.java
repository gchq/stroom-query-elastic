package stroom.akka.query.cluster;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.cluster.Cluster;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import akka.actor.ActorSystem;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;
import stroom.akka.query.messages.QuerySearchMessages;
import stroom.query.api.v2.SearchRequest;

import java.util.concurrent.TimeUnit;

public class QuerySearchFrontendMain {
    public static final String CLUSTER_SYSTEM_NAME = "QueryClusterSystem";

    public static QuerySearchFrontendMain create() {
        return new QuerySearchFrontendMain();
    }

    private ActorRef frontendActor;

    public void search(final SearchRequest request) {
        if (null != frontendActor) {
            frontendActor.tell(request, ActorRef.noSender());
        } else {
            throw new RuntimeException("Search is not available");
        }
    }

    public void run() {
        final Config config = ConfigFactory.parseString(
                "akka.cluster.roles = [frontend]").withFallback(
                ConfigFactory.load("search"));

        final ActorSystem system = ActorSystem.create(CLUSTER_SYSTEM_NAME, config);
        system.log().info(
                "Search will be available when 1 backend members in the cluster.");
        Cluster.get(system).registerOnMemberUp(new Runnable() {
            @Override
            public void run() {
                system.log().info("Backend Discovered");
                frontendActor = system.actorOf(Props.create(QuerySearchFrontend.class),
                        "querySearchFrontend");
            }
        });

        Cluster.get(system).registerOnMemberRemoved(new Runnable() {
            @Override
            public void run() {
                // exit JVM when ActorSystem has been terminated
                final Runnable exit = new Runnable() {
                    @Override public void run() {
                        System.exit(0);
                    }
                };
                system.registerOnTermination(exit);

                // shut down ActorSystem
                system.terminate();

                // In case ActorSystem shutdown takes longer than 10 seconds,
                // exit the JVM forcefully anyway.
                // We must spawn a separate thread to not block current thread,
                // since that would have blocked the shutdown of the ActorSystem.
                new Thread() {
                    @Override public void run(){
                        try {
                            Await.ready(system.whenTerminated(), Duration.create(10, TimeUnit.SECONDS));
                        } catch (Exception e) {
                            System.exit(-1);
                        }

                    }
                }.start();
            }
        });
    }


    public static void main(String[] args) {
        // Run this cluster frontend
        create().run();
    }
}
