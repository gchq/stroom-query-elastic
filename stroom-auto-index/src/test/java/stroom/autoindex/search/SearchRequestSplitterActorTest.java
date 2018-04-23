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
import stroom.autoindex.service.AutoIndexDocRefEntity;
import stroom.query.api.v2.DocRef;
import stroom.query.api.v2.SearchRequest;
import stroom.security.ServiceUser;
import stroom.query.audit.service.DocRefService;
import stroom.query.audit.service.QueryApiException;
import stroom.tracking.TimelineTracker;
import stroom.tracking.TimelineTrackerService;
import stroom.tracking.TrackerWindow;

import java.util.Optional;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.when;
import static stroom.autoindex.search.SearchRequestSplitterTest.TIMELINE_FIELD_NAME;
import static stroom.autoindex.search.SearchRequestSplitterTest.assertSplitPart;

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
    public void testSuccess() throws QueryApiException {
        // Given
        final Long timelineWindow = 100L; // size of the window when future jobs are created
        final Long windowsInTest = 10L; // number of windows we want for a whole timeline
        final Long timelineStart = 1000L; // The start value
        final Long filledInWindows = 3L; // The number of windows we will have filled in already (from the end)
        final Long timelineEnd = timelineStart + (windowsInTest * timelineWindow); // Calculated end value
        final Long filledInStart = timelineEnd - (filledInWindows * timelineWindow); // Start value of filled in window

        final ServiceUser user = new ServiceUser.Builder()
                .name("testGuy")
                .jwt(UUID.randomUUID().toString())
                .build();
        final DocRef autoIndexDocRef = new DocRef.Builder()
                .uuid(UUID.randomUUID().toString())
                .type(AutoIndexDocRefEntity.TYPE)
                .build();
        final AutoIndexDocRefEntity autoIndexDocRefEntity =
                new AutoIndexDocRefEntity.Builder()
                        .docRef(autoIndexDocRef)
                        .indexDocRef(new DocRef.Builder()
                                .type(UUID.randomUUID().toString())
                                .uuid(UUID.randomUUID().toString())
                                .build())
                        .rawDocRef(new DocRef.Builder()
                                .type(UUID.randomUUID().toString())
                                .uuid(UUID.randomUUID().toString())
                                .build())
                        .timeFieldName(TIMELINE_FIELD_NAME)
                        .indexWindow(100L)
                        .build();
        final TimelineTracker timelineTracker = TimelineTracker.forDocRef(autoIndexDocRefEntity.getUuid())
                .withBounds(TrackerWindow.from(timelineStart).to(timelineEnd))
                .withWindow(TrackerWindow.from(filledInStart).to(timelineEnd));
        final TestKit testProbe = new TestKit(actorSystem);
        final ActorRef splitter = actorSystem.actorOf(SearchRequestSplitterActor.props(docRefService, timelineTrackerService));
        final SearchRequest searchRequest = SearchRequestSplitterTest.getTestSearchRequest(autoIndexDocRef);
        final SearchMessages.SearchJob searchJob =
                SearchMessages.search(user, AutoIndexDocRefEntity.TYPE, searchRequest);

        // mocking the underlying services
        when(docRefService.get(user, autoIndexDocRef.getUuid()))
                .thenReturn(Optional.of(autoIndexDocRefEntity));
        when(timelineTrackerService.get(autoIndexDocRef.getUuid()))
                .thenReturn(timelineTracker);

        // When
        splitter.tell(searchJob, testProbe.getRef());

        // Then
        final SearchMessages.SplitSearchJobComplete jobComplete =
                testProbe.expectMsgClass(SearchMessages.SplitSearchJobComplete.class);
        assertNull(jobComplete.getError());
        assertEquals(autoIndexDocRef, jobComplete.getDocRef());
        assertEquals(searchJob.getRequest(), jobComplete.getOriginalSearchRequest());
        assertEquals(user, jobComplete.getUser());

        // Detailed check of the split request
        final SplitSearchRequest splitSearchRequest = jobComplete.getSplitSearchRequest();
        assertSplitPart(searchRequest,
                splitSearchRequest,
                TrackerWindow.from(timelineStart).to(filledInStart),
                autoIndexDocRefEntity.getRawDocRef());
        assertSplitPart(searchRequest,
                splitSearchRequest,
                TrackerWindow.from(filledInStart).to(timelineEnd),
                autoIndexDocRefEntity.getIndexDocRef());
    }
}
