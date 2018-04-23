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
import stroom.datasource.api.v2.DataSource;
import stroom.query.api.v2.DocRef;
import stroom.query.audit.client.RemoteClientCache;
import stroom.query.audit.service.QueryApiException;
import stroom.query.audit.service.QueryService;
import stroom.security.ServiceUser;

import java.util.Optional;
import java.util.UUID;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class QueryDataSourceActorTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryDataSourceActorTest.class);

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
    public void testDataSourceActorValid() throws QueryApiException {
        // Given
        final ServiceUser user = new ServiceUser.Builder()
                .name("Me")
                .jwt(UUID.randomUUID().toString())
                .build();
        final String type1 = "typeOne";
        final DocRef docRef1 = new DocRef.Builder()
                .uuid(UUID.randomUUID().toString())
                .type(type1)
                .name(UUID.randomUUID().toString())
                .build();
        final QueryService queryService = Mockito.mock(QueryService.class);
        final RemoteClientCache<QueryService> queryServices =
                new RemoteClientCache<>(d -> d, (t, u) -> t.equals(type1) ? queryService : null);
        Mockito.doReturn(Optional.of(new DataSource.Builder().build()))
                .when(queryService)
                .getDataSource(Mockito.any(), Mockito.any());
        final TestKit testProbe = new TestKit(system);
        final ActorRef searchActor = system.actorOf(QueryDataSourceActor.props(queryServices));

        // When
        searchActor.tell(QueryApiMessages.dataSource(user, docRef1), testProbe.getRef());

        // Then
        final QueryApiMessages.DataSourceJobComplete jobComplete = testProbe.expectMsgClass(QueryApiMessages.DataSourceJobComplete.class);
        assertNotNull(jobComplete.getResponse());
        assertNull(jobComplete.getError());
    }

    @Test
    public void testDataSourceActorEmptySearchRequest() throws QueryApiException {
        // Given
        final ServiceUser user = new ServiceUser.Builder()
                .name("Me")
                .jwt(UUID.randomUUID().toString())
                .build();
        final String type1 = "typeOne";
        final DocRef docRef1 = new DocRef.Builder()
                .uuid(UUID.randomUUID().toString())
                .type(type1)
                .name(UUID.randomUUID().toString())
                .build();
        final QueryService queryService = Mockito.mock(QueryService.class);
        Mockito.doReturn(Optional.empty())
                .when(queryService)
                .getDataSource(Mockito.any(), Mockito.any());
        final RemoteClientCache<QueryService> queryServices =
                new RemoteClientCache<>(d -> d, (t, u) -> t.equals(type1) ? queryService : null);
        final TestKit testProbe = new TestKit(system);
        final ActorRef searchActor = system.actorOf(QueryDataSourceActor.props(queryServices));

        // When
        searchActor.tell(QueryApiMessages.dataSource(user, docRef1), testProbe.getRef());

        // Then
        final QueryApiMessages.DataSourceJobComplete jobComplete = testProbe.expectMsgClass(QueryApiMessages.DataSourceJobComplete.class);
        assertNull(jobComplete.getResponse());
        assertNotNull(jobComplete.getError());

        LOGGER.info("Error Seen correctly {}", jobComplete.getError());
    }

    @Test
    public void testDataSourceInvalidType() throws QueryApiException {
        // Given
        final ServiceUser user = new ServiceUser.Builder()
                .name("Me")
                .jwt(UUID.randomUUID().toString())
                .build();
        final String type1 = "typeOne";
        final String type2 = "typeTwo";
        final DocRef docRef2 = new DocRef.Builder()
                .uuid(UUID.randomUUID().toString())
                .type(type2)
                .name(UUID.randomUUID().toString())
                .build();
        final QueryService queryService = Mockito.mock(QueryService.class);
        Mockito.doReturn(Optional.of(new DataSource.Builder().build()))
                .when(queryService)
                .getDataSource(Mockito.any(), Mockito.any());
        final RemoteClientCache<QueryService> queryServices =
                new RemoteClientCache<>(d -> d, (t, u) -> t.equals(type1) ? queryService : null);
        final TestKit testProbe = new TestKit(system);
        final ActorRef searchActor1 = system.actorOf(QueryDataSourceActor.props(queryServices));

        // When
        searchActor1.tell(QueryApiMessages.dataSource(user, docRef2), testProbe.getRef());

        // Then
        final QueryApiMessages.DataSourceJobComplete jobComplete = testProbe.expectMsgClass(QueryApiMessages.DataSourceJobComplete.class);
        assertNull(jobComplete.getResponse());
        assertNotNull(jobComplete.getError());

        LOGGER.info("Error Seen correctly {}", jobComplete.getError());
    }
}
