package stroom.akka.query;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.javadsl.TestKit;
import akka.util.Timeout;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.FiniteDuration;
import stroom.akka.query.actors.QuerySearchActor;
import stroom.akka.query.messages.QuerySearchMessages;
import stroom.query.api.v2.SearchRequest;
import stroom.query.api.v2.SearchResponse;
import stroom.query.audit.service.QueryApiException;
import stroom.query.audit.service.QueryService;
import stroom.security.ServiceUser;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static akka.pattern.PatternsCS.ask;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class QuerySearchActorTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(QuerySearchActorTest.class);

    static ActorSystem system;

    @BeforeClass
    public static void setup() {
        system = ActorSystem.create();
    }

    @AfterClass
    public static void teardown() {
        TestKit.shutdownActorSystem(system);
        system = null;
    }

    @Test
    public void testSearchActorValid() throws QueryApiException, ExecutionException, InterruptedException {
        // Given
        final ServiceUser user = new ServiceUser.Builder()
                .name("Me")
                .jwt(UUID.randomUUID().toString())
                .build();
        final QueryService queryService = Mockito.mock(QueryService.class);
        Mockito.doReturn(Optional.of(new SearchResponse.FlatResultBuilder().build()))
                .when(queryService)
                .search(Mockito.any(), Mockito.any());
        final TestKit testProbe = new TestKit(system);
        final ActorRef searchActor = system.actorOf(QuerySearchActor.props(user, queryService));

        // When
        final SearchRequest job = new SearchRequest.Builder().build();
        searchActor.tell(job, testProbe.getRef());

        // Then
        final QuerySearchMessages.JobComplete jobComplete1 = testProbe.expectMsgClass(QuerySearchMessages.JobComplete.class);
        assertNotNull(jobComplete1.getResponse());
        assertNull(jobComplete1.getError());

        // Now try same thing using ask
        final Timeout timeout = Timeout.durationToTimeout(FiniteDuration.apply(5, TimeUnit.SECONDS));
        CompletableFuture<QuerySearchMessages.JobComplete> syncJobComplete =
                ask(searchActor, job, timeout)
                        .thenApply((QuerySearchMessages.JobComplete.class::cast))
                        .toCompletableFuture();

        final QuerySearchMessages.JobComplete jobComplete2 = syncJobComplete.get();

        LOGGER.info("Job Complete as Synchronous Process {}", jobComplete2);
        assertNotNull(jobComplete2.getResponse());
        assertNull(jobComplete2.getError());
    }

    @Test
    public void testSearchActorEmptySearchRequest() throws QueryApiException {
        // Given
        final ServiceUser user = new ServiceUser.Builder()
                .name("Me")
                .jwt(UUID.randomUUID().toString())
                .build();
        final QueryService queryService = Mockito.mock(QueryService.class);
        Mockito.doReturn(Optional.empty())
                .when(queryService)
                .search(Mockito.any(), Mockito.any());
        final TestKit testProbe = new TestKit(system);
        final ActorRef searchActor = system.actorOf(QuerySearchActor.props(user, queryService));

        // When
        searchActor.tell(new SearchRequest.Builder().build(), testProbe.getRef());

        // Then
        final QuerySearchMessages.JobComplete jobComplete = testProbe.expectMsgClass(QuerySearchMessages.JobComplete.class);
        assertNull(jobComplete.getResponse());
        assertNotNull(jobComplete.getError());

        LOGGER.info("Error Seen correctly {}", jobComplete.getError());
    }

    @Test
    public void testMultipleJobs() throws QueryApiException {
        // Given
        final ServiceUser user = new ServiceUser.Builder()
                .name("Me")
                .jwt(UUID.randomUUID().toString())
                .build();
        final QueryService queryService = Mockito.mock(QueryService.class);
        Mockito.doReturn(Optional.of(new SearchResponse.FlatResultBuilder().build()))
                .when(queryService)
                .search(Mockito.any(), Mockito.any());
        final TestKit testProbe = new TestKit(system);
        final ActorRef searchActor = system.actorOf(QuerySearchActor.props(user, queryService));
        final int numberOfJobs = 3;

        final long startTime = System.currentTimeMillis();

        // When
        IntStream.range(0, numberOfJobs).forEach(i -> {
            searchActor.tell(new SearchRequest.Builder()
                    .key(Integer.toString(i))
                    .build(), testProbe.getRef());
        });

        // Then
        final List<Object> responses = testProbe.receiveN(numberOfJobs);

        responses.stream()
                .filter(i -> i instanceof QuerySearchMessages.JobComplete)
                .map(i -> (QuerySearchMessages.JobComplete) i)
                .forEach(jobComplete -> {
                    LOGGER.info("Job Complete Received {}", jobComplete);
                    assertNotNull(jobComplete.getResponse());
                    assertNull(jobComplete.getError());
                });

        final long endTime = System.currentTimeMillis();

        final Duration testTime = Duration.of(endTime - startTime, ChronoUnit.MILLIS);
        LOGGER.info("Test took {} seconds", testTime.get(ChronoUnit.SECONDS));
    }
}