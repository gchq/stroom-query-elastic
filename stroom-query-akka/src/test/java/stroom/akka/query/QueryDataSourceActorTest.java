package stroom.akka.query;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.javadsl.TestKit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.akka.query.actors.QueryDataSourceActor;
import stroom.akka.query.messages.QueryDataSourceMessages;
import stroom.datasource.api.v2.DataSource;
import stroom.query.api.v2.DocRef;
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
        Mockito.doReturn(Optional.of(new DataSource.Builder().build()))
                .when(queryService)
                .getDataSource(Mockito.any(), Mockito.any());
        final TestKit testProbe = new TestKit(system);
        final ActorRef searchActor = system.actorOf(QueryDataSourceActor.props(user, queryService));

        // When
        searchActor.tell(docRef1, testProbe.getRef());

        // Then
        final QueryDataSourceMessages.JobComplete jobComplete = testProbe.expectMsgClass(QueryDataSourceMessages.JobComplete.class);
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
        final TestKit testProbe = new TestKit(system);
        final ActorRef searchActor = system.actorOf(QueryDataSourceActor.props(user, queryService));

        // When
        searchActor.tell(docRef1, testProbe.getRef());

        // Then
        final QueryDataSourceMessages.JobComplete jobComplete = testProbe.expectMsgClass(QueryDataSourceMessages.JobComplete.class);
        assertNull(jobComplete.getResponse());
        assertNotNull(jobComplete.getError());

        LOGGER.info("Error Seen correctly {}", jobComplete.getError());
    }
}
