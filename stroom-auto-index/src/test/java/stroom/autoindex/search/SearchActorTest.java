package stroom.autoindex.search;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.javadsl.TestKit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.query.api.v2.SearchRequest;
import stroom.query.api.v2.SearchResponse;
import stroom.query.audit.client.RemoteClientCache;
import stroom.query.audit.security.ServiceUser;
import stroom.query.audit.service.QueryApiException;
import stroom.query.audit.service.QueryService;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class SearchActorTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(SearchActorTest.class);

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
    public void testSearchActorValid() throws QueryApiException {
        // Given
        final ServiceUser user = new ServiceUser.Builder()
                .name("Me")
                .jwt(UUID.randomUUID().toString())
                .build();
        final String type1 = "typeOne";
        final QueryService queryService = Mockito.mock(QueryService.class);
        final RemoteClientCache<QueryService> queryServices =
                new RemoteClientCache<>(d -> d, (t, u) -> t.equals(type1) ? queryService : null);
        Mockito.doReturn(Optional.of(new SearchResponse.FlatResultBuilder().build()))
                .when(queryService)
                .search(Mockito.any(), Mockito.any());
        final TestKit testProbe = new TestKit(system);
        final ActorRef searchActor = system.actorOf(SearchActor.props(queryServices));

        // When
        searchActor.tell(SearchMessages.search(user, type1, new SearchRequest.Builder().build()), testProbe.getRef());

        // Then
        final SearchMessages.SearchJobComplete jobComplete = testProbe.expectMsgClass(SearchMessages.SearchJobComplete.class);
        assertNotNull(jobComplete.getResponse());
        assertNull(jobComplete.getError());
    }

    @Test
    public void testSearchActorEmptySearchRequest() throws QueryApiException {
        // Given
        final ServiceUser user = new ServiceUser.Builder()
                .name("Me")
                .jwt(UUID.randomUUID().toString())
                .build();
        final String type1 = "typeOne";
        final QueryService queryService = Mockito.mock(QueryService.class);
        Mockito.doReturn(Optional.empty())
                .when(queryService)
                .search(Mockito.any(), Mockito.any());
        final RemoteClientCache<QueryService> queryServices =
                new RemoteClientCache<>(d -> d, (t, u) -> t.equals(type1) ? queryService : null);
        final TestKit testProbe = new TestKit(system);
        final ActorRef searchActor = system.actorOf(SearchActor.props(queryServices));

        // When
        searchActor.tell(SearchMessages.search(user, type1, new SearchRequest.Builder().build()), testProbe.getRef());

        // Then
        final SearchMessages.SearchJobComplete jobComplete = testProbe.expectMsgClass(SearchMessages.SearchJobComplete.class);
        assertNull(jobComplete.getResponse());
        assertNotNull(jobComplete.getError());

        LOGGER.info("Error Seen correctly {}", jobComplete.getError());
    }

    @Test
    public void testSearchInvalidType() throws QueryApiException {
        // Given
        final ServiceUser user = new ServiceUser.Builder()
                .name("Me")
                .jwt(UUID.randomUUID().toString())
                .build();
        final String type1 = "typeOne";
        final String type2 = "typeTwo";
        final QueryService queryService = Mockito.mock(QueryService.class);
        Mockito.doReturn(Optional.of(new SearchResponse.FlatResultBuilder().build()))
                .when(queryService)
                .search(Mockito.any(), Mockito.any());
        final RemoteClientCache<QueryService> queryServices =
                new RemoteClientCache<>(d -> d, (t, u) -> t.equals(type1) ? queryService : null);
        final TestKit testProbe = new TestKit(system);
        final ActorRef searchActor1 = system.actorOf(SearchActor.props(queryServices));

        // When
        searchActor1.tell(SearchMessages.search(user, type2, new SearchRequest.Builder().build()), testProbe.getRef());

        // Then
        final SearchMessages.SearchJobComplete jobComplete = testProbe.expectMsgClass(SearchMessages.SearchJobComplete.class);
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
        final String type1 = "typeOne";
        final QueryService queryService = Mockito.mock(QueryService.class);
        Mockito.doReturn(Optional.of(new SearchResponse.FlatResultBuilder().build()))
                .when(queryService)
                .search(Mockito.any(), Mockito.any());
        final RemoteClientCache<QueryService> queryServices =
                new RemoteClientCache<>(d -> d, (t, u) -> t.equals(type1) ? queryService : null);
        final TestKit testProbe = new TestKit(system);
        final ActorRef searchActor = system.actorOf(SearchActor.props(queryServices));
        final int numberOfJobs = 3;

        final long startTime = System.currentTimeMillis();

        // When
        IntStream.range(0, numberOfJobs).forEach(i -> {
            searchActor.tell(SearchMessages.search(user, type1, new SearchRequest.Builder()
                    .key(Integer.toString(i))
                    .build()), testProbe.getRef());
        });

        // Then
        final List<Object> responses = testProbe.receiveN(numberOfJobs);

        responses.stream()
                .filter(i -> i instanceof SearchMessages.SearchJobComplete)
                .map(i -> (SearchMessages.SearchJobComplete) i)
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
