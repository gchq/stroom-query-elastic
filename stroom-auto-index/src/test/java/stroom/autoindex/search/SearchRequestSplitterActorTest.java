package stroom.autoindex.search;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.javadsl.TestKit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import stroom.autoindex.search.SearchMessages;
import stroom.autoindex.search.SearchRequestSplitterActor;
import stroom.autoindex.service.AutoIndexDocRefEntity;
import stroom.query.api.v2.SearchRequest;
import stroom.query.audit.security.ServiceUser;
import stroom.query.audit.service.DocRefService;
import stroom.tracking.TimelineTrackerService;

import java.util.UUID;

@RunWith(MockitoJUnitRunner.class)
public class SearchRequestSplitterActorTest {
    private static ActorSystem actorSystem;

    @Mock
    private DocRefService<AutoIndexDocRefEntity> docRefService;

    @Mock
    private TimelineTrackerService timelineTrackerService;

    @BeforeClass
    public static void beforeClass() {
        actorSystem = ActorSystem.create();
    }

    @AfterClass
    public static void afterClass() {
        TestKit.shutdownActorSystem(actorSystem);
        actorSystem = null;
    }

    @Test
    public void testSuccess() {
        // Given
        final ServiceUser user = new ServiceUser.Builder()
                .name("testGuy")
                .jwt(UUID.randomUUID().toString())
                .build();
        final TestKit testProbe = new TestKit(actorSystem);
        final ActorRef splitter = actorSystem.actorOf(SearchRequestSplitterActor.props(docRefService, timelineTrackerService));
        final SearchMessages.SearchJob searchJob =
                SearchMessages.search(user, AutoIndexDocRefEntity.TYPE, new SearchRequest.Builder().build());

        // When
        splitter.tell(searchJob, testProbe.getRef());

        // Then
        final SearchMessages.SplitSearchJobComplete jobComplete =
                testProbe.expectMsgClass(SearchMessages.SplitSearchJobComplete.class);
    }
}
