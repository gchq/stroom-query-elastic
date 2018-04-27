package stroom.akka.query.cluster;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.util.Timeout;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import akka.actor.ActorSystem;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;
import stroom.akka.query.messages.QueryDataSourceMessages;
import stroom.akka.query.messages.QuerySearchMessages;
import stroom.datasource.api.v2.DataSource;
import stroom.query.api.v2.DocRef;
import stroom.query.api.v2.QueryKey;
import stroom.query.api.v2.SearchRequest;
import stroom.query.api.v2.SearchResponse;
import stroom.query.audit.service.QueryApiException;
import stroom.query.audit.service.QueryService;
import stroom.security.ServiceUser;

import static akka.pattern.PatternsCS.ask;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class QuerySearchFrontendMain implements QueryService {
    public static final String CLUSTER_SYSTEM_NAME = "QueryClusterSystem";

    public static QuerySearchFrontendMain create() {
        return new QuerySearchFrontendMain();
    }

    private final CompletableFuture<Boolean> clusterReady;
    private ActorRef frontendActor;

    public QuerySearchFrontendMain() {
        this.clusterReady = new CompletableFuture<>();
    }

    public CompletableFuture<Boolean> run() {
        final Config config = ConfigFactory.parseString(
                "akka.cluster.roles = [frontend]").withFallback(
                ConfigFactory.load("search"));

        final ActorSystem system = ActorSystem.create(CLUSTER_SYSTEM_NAME, config);
        system.log().info(
                "Search will be available when 1 backend members in the cluster.");
        Cluster.get(system).registerOnMemberUp(() -> {
            system.log().info("Backend Discovered");
            frontendActor = system.actorOf(QuerySearchFrontend.props(clusterReady),
                    "querySearchFrontend");
        });

        Cluster.get(system).registerOnMemberRemoved(() -> {
            // exit JVM when ActorSystem has been terminated
            final Runnable exit = () -> System.exit(0);
            system.registerOnTermination(exit);

            // shut down ActorSystem
            system.terminate();

            // In case ActorSystem shutdown takes longer than 10 seconds,
            // exit the JVM forcefully anyway.
            // We must spawn a separate thread to not block current thread,
            // since that would have blocked the shutdown of the ActorSystem.
            new Thread(() -> {
                try {
                    Await.ready(system.whenTerminated(), Duration.create(10, TimeUnit.SECONDS));
                } catch (Exception e) {
                    System.exit(-1);
                }

            }).start();
        });

        return this.clusterReady;
    }

    public static void main(String[] args) {
        // Run this cluster frontend
        create().run();
    }

    @Override
    public String getType() {
        return null;
    }

    @Override
    public Optional<DataSource> getDataSource(final ServiceUser user,
                                              final DocRef docRef) throws QueryApiException {
        if (null == frontendActor) {
            throw new RuntimeException("Frontend is not available");
        }

        final Timeout timeout = Timeout.durationToTimeout(FiniteDuration.apply(5, TimeUnit.SECONDS));
        try {
            final QueryDataSourceMessages.JobComplete complete =
                    ask(frontendActor, docRef, timeout)
                            .thenApply((QueryDataSourceMessages.JobComplete.class::cast))
                            .toCompletableFuture()
                            .get();

            return Optional.ofNullable(complete.getResponse());
        } catch (InterruptedException | ExecutionException e) {
            throw new QueryApiException(e);
        }
    }

    @Override
    public Optional<SearchResponse> search(final ServiceUser user,
                                           final SearchRequest request) throws QueryApiException {
        if (null == frontendActor) {
            throw new RuntimeException("Frontend is not available");
        }

        final Timeout timeout = Timeout.durationToTimeout(FiniteDuration.apply(5, TimeUnit.SECONDS));
        try {
            final QuerySearchMessages.JobComplete complete =
                    ask(frontendActor, request, timeout)
                            .thenApply((QuerySearchMessages.JobComplete.class::cast))
                            .toCompletableFuture()
                            .get();

            return Optional.ofNullable(complete.getResponse());
        } catch (InterruptedException | ExecutionException e) {
            throw new QueryApiException(e);
        }
    }

    @Override
    public Boolean destroy(final ServiceUser user,
                           final QueryKey queryKey) throws QueryApiException {
        return null;
    }

    @Override
    public Optional<DocRef> getDocRefForQueryKey(final ServiceUser user,
                                                 final QueryKey queryKey) throws QueryApiException {
        return null;
    }
}
