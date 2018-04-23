package stroom.autoindex.search;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.TestProbe;
import akka.testkit.javadsl.TestKit;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import org.elasticsearch.client.transport.TransportClient;
import org.jooq.DSLContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.authorisation.DocumentPermission;
import stroom.autoindex.AbstractAutoIndexIntegrationTest;
import stroom.autoindex.animals.AnimalsQueryResourceIT;
import stroom.autoindex.animals.app.AnimalSighting;
import stroom.autoindex.app.Config;
import stroom.autoindex.service.AutoIndexDocRefEntity;
import stroom.autoindex.service.AutoIndexDocRefServiceImpl;
import stroom.query.api.v2.ExpressionOperator;
import stroom.query.api.v2.ExpressionTerm;
import stroom.query.api.v2.OffsetRange;
import stroom.query.api.v2.SearchRequest;
import stroom.query.audit.service.DocRefService;
import stroom.query.elastic.transportClient.TransportClientBundle;
import stroom.security.ServiceUser;
import stroom.tracking.*;

import javax.inject.Named;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static stroom.autoindex.AutoIndexConstants.STROOM_SERVICE_USER;
import static stroom.autoindex.TestConstants.TEST_SERVICE_USER;
import static stroom.autoindex.search.SearchRequestSplitterTest.assertSplitPart;

public class SearchRequestSplitterActorIT extends AbstractAutoIndexIntegrationTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(SearchRequestSplitterActorIT.class);

    private static ActorSystem actorSystem;

    private static DocRefService<AutoIndexDocRefEntity> docRefService;
    private static TimelineTrackerService timelineTrackerService;
    private static TestProbe testProbe;

    @BeforeClass
    public static void beforeClass() {
        actorSystem = ActorSystem.create();

        testProbe = new TestProbe(actorSystem);

        final Injector injector = Guice.createInjector(new AbstractModule() {
            @Override
            protected void configure() {
                bind(DSLContext.class).toInstance(initialiseJooqDbRule.withDatabase());
                bind(TimelineTrackerDao.class).to(TimelineTrackerDaoJooqImpl.class);
                bind(TimelineTrackerService.class).to(TimelineTrackerServiceImpl.class);
                bind(DocRefService.class).to(AutoIndexDocRefServiceImpl.class);
                bind(Config.class).toInstance(autoIndexAppRule.getConfiguration());
                bind(TransportClient.class)
                        .toInstance(TransportClientBundle.createTransportClient(autoIndexAppRule.getConfiguration()));
            }

            @Provides
            @Named(STROOM_SERVICE_USER)
            public ServiceUser serviceUser() {
                return serviceUser;
            }
        });

        docRefService = injector.getInstance(DocRefService.class);
        timelineTrackerService = injector.getInstance(TimelineTrackerService.class);
    }

    @AfterClass
    public static void afterClass() {
        TestKit.shutdownActorSystem(actorSystem);
        actorSystem = null;
    }

    @Test
    public void testSimple() {
        // Create a proper auto indexed set of documents
        final EntityWithDocRef<AutoIndexDocRefEntity> autoIndex = createAutoIndex();

        // Give our fixed test service user access to the doc refs
        // The wired Index Job DAO will use this user via Guice injection
        authRule.permitAuthenticatedUser(TEST_SERVICE_USER)
                .docRef(autoIndex.getDocRef())
                .docRef(autoIndex.getEntity().getRawDocRef())
                .docRef(autoIndex.getEntity().getIndexDocRef())
                .permission(DocumentPermission.READ)
                .permission(DocumentPermission.UPDATE)
                .done();
        final ServiceUser testUser = authRule.authenticatedUser(TEST_SERVICE_USER);

        // Fix these timeline bounds
        final Long timelineWindow = 100L; // size of the window when future jobs are created
        final Long windowsInTest = 10L; // number of windows we want for a whole timeline
        final Long timelineStart = 1000L; // The start value
        final Long filledInWindows = 3L; // The number of windows we will have filled in already (from the end)
        final Long timelineEnd = timelineStart + (windowsInTest * timelineWindow); // Calculated end value
        final Long filledInStart = timelineEnd - (filledInWindows * timelineWindow); // Start value of filled in window

        // Register the filled in window with the tracker service
        timelineTrackerService.setTimelineBounds(autoIndex.getDocRef().getUuid(),
                TrackerWindow.from(timelineStart).to(timelineEnd));
        timelineTrackerService.addWindow(autoIndex.getDocRef().getUuid(),
                TrackerWindow.from(filledInStart).to(timelineEnd));

        // Construct the actor to test
        final ActorRef splitter = actorSystem.actorOf(SearchRequestSplitterActor.props(docRefService, timelineTrackerService));

        // Now compose a query that covers all time
        final OffsetRange offset = new OffsetRange.Builder()
                .length(100L)
                .offset(0L)
                .build();
        final String testObserver = "alpha";
        final LocalDateTime testMinDate = LocalDateTime.of(2017, 1, 1, 0, 0, 0);
        final ExpressionOperator expressionOperator = new ExpressionOperator.Builder(ExpressionOperator.Op.AND)
                .addTerm(AnimalSighting.OBSERVER, ExpressionTerm.Condition.CONTAINS, testObserver)
                .addTerm(AnimalSighting.TIME,
                        ExpressionTerm.Condition.GREATER_THAN,
                        testMinDate.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                )
                .build();
        final SearchRequest searchRequest = AnimalsQueryResourceIT
                .getTestSearchRequest(autoIndex.getDocRef(), expressionOperator, offset);

        // Send the message to the splitter
        splitter.tell(SearchMessages.search(testUser, AutoIndexDocRefEntity.TYPE, searchRequest), testProbe.ref());

        // Then
        final SearchMessages.SplitSearchJobComplete jobComplete =
                testProbe.expectMsgClass(SearchMessages.SplitSearchJobComplete.class);
        assertNull(jobComplete.getError());
        assertEquals(autoIndex.getDocRef(), jobComplete.getDocRef());
        assertEquals(searchRequest, jobComplete.getOriginalSearchRequest());
        assertEquals(testUser, jobComplete.getUser());

        // Detailed check of the split request
        final SplitSearchRequest splitSearchRequest = jobComplete.getSplitSearchRequest();

        try {
            assertSplitPart(searchRequest,
                    splitSearchRequest,
                    TrackerWindow.from(timelineStart).to(filledInStart),
                    autoIndex.getEntity().getRawDocRef());
            assertSplitPart(searchRequest,
                    splitSearchRequest,
                    TrackerWindow.from(filledInStart).to(timelineEnd),
                    autoIndex.getEntity().getIndexDocRef());
        } catch (final Exception e) {
            LOGGER.info("Failed to process {}", splitSearchRequest);
            throw e;
        }

    }
}
