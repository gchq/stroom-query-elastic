package stroom.autoindex;

import akka.actor.ActorSystem;
import akka.testkit.javadsl.TestKit;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;
import com.google.inject.util.Modules;
import org.elasticsearch.client.transport.TransportClient;
import org.jooq.DSLContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.autoindex.animals.AnimalTestData;
import stroom.autoindex.animals.AnimalsQueryResourceIT;
import stroom.autoindex.animals.app.AnimalSighting;
import stroom.autoindex.app.Config;
import stroom.autoindex.app.IndexingConfig;
import stroom.autoindex.indexing.*;
import stroom.autoindex.service.AutoIndexDocRefEntity;
import stroom.autoindex.service.AutoIndexDocRefServiceImpl;
import stroom.autoindex.service.AutoIndexQueryServiceImpl;
import stroom.query.api.v2.*;
import stroom.query.audit.authorisation.DocumentPermission;
import stroom.query.audit.client.RemoteClientModule;
import stroom.query.audit.security.ServiceUser;
import stroom.query.audit.service.DocRefService;
import stroom.query.audit.service.QueryApiException;
import stroom.query.csv.CsvDocRefEntity;
import stroom.query.elastic.model.ElasticIndexDocRefEntity;
import stroom.query.elastic.transportClient.TransportClientBundle;
import stroom.tracking.TimelineTrackerDao;
import stroom.tracking.TimelineTrackerDaoJooqImpl;
import stroom.tracking.TimelineTrackerService;
import stroom.tracking.TimelineTrackerServiceImpl;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Set;

import static org.junit.Assert.assertTrue;
import static stroom.autoindex.AutoIndexConstants.TASK_HANDLER_NAME;
import static stroom.autoindex.TestConstants.TEST_SERVICE_USER;
import static stroom.autoindex.animals.AnimalsQueryResourceIT.getAnimalSightingsFromResponse;

/**
 * This will run a test that ensures some portion of the data has been indexed
 * and that the query gets split both ways
 */
public class AutoIndexQueryForkIT extends AbstractAutoIndexIntegrationTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexJobHandlerImplIT.class);

    private static ActorSystem actorSystem;

    private static IndexJobDao indexJobDao;

    private static IndexingConfig indexingConfig = IndexingConfig
            .asEnabled()
            .withNumberOfTasksPerRun(3);

    /**
     * We are only really testing that the integration of the window and job management causes the right tasks to be fired off.
     */
    private static IndexJobHandlerImpl indexJobHandler;

    /**
     * We will use this to manually tell the system that we already have data that runs from 'now' back to
     * the end date of our test data.
     */
    private static TimelineTrackerService timelineTrackerService;

    /**
     * Run tests on a locally created service
     */
    private static AutoIndexQueryServiceImpl autoIndexQueryService;

    @BeforeClass
    public static void beforeClass() {

        actorSystem = ActorSystem.create();

        final Injector testInjector = Guice.createInjector(Modules.combine(new AbstractModule() {
            @Override
            protected void configure() {
                bind(DSLContext.class).toInstance(initialiseJooqDbRule.withDatabase());
                bind(TimelineTrackerDao.class).to(TimelineTrackerDaoJooqImpl.class);
                bind(TimelineTrackerService.class).to(TimelineTrackerServiceImpl.class);
                bind(IndexJobDao.class).to(IndexJobDaoImpl.class);
                bind(IndexWriter.class).to(IndexWriterImpl.class);
                bind(DocRefService.class).to(AutoIndexDocRefServiceImpl.class);
                bind(IndexJobHandler.class)
                        .annotatedWith(Names.named(TASK_HANDLER_NAME))
                        .to(IndexJobHandlerImpl.class)
                        .asEagerSingleton(); // singleton so that the test receives same instance as the underlying timer task
                bind(IndexingConfig.class).toInstance(indexingConfig);
                bind(Config.class).toInstance(autoIndexAppRule.getConfiguration());
                bind(ServiceUser.class)
                        .annotatedWith(Names.named(AutoIndexConstants.STROOM_SERVICE_USER))
                        .toInstance(serviceUser);
                bind(TransportClient.class)
                        .toInstance(TransportClientBundle.createTransportClient(autoIndexAppRule.getConfiguration()));
            }
        }), new RemoteClientModule(autoIndexAppRule.getConfiguration().getQueryResourceUrlsByType())
                .addType(ElasticIndexDocRefEntity.TYPE, ElasticIndexDocRefEntity.class)
                .addType(CsvDocRefEntity.TYPE, CsvDocRefEntity.class)
        );

        final Key<IndexJobHandler> taskHandlerKey = Key.get(IndexJobHandler.class, Names.named(TASK_HANDLER_NAME));
        final Object testIndexJobConsumerObj = testInjector.getInstance(taskHandlerKey);
        assertTrue(testIndexJobConsumerObj instanceof IndexJobHandlerImpl);
        indexJobHandler = (IndexJobHandlerImpl) testIndexJobConsumerObj;
        indexJobDao = testInjector.getInstance(IndexJobDao.class);
        timelineTrackerService = testInjector.getInstance(TimelineTrackerService.class);
        autoIndexQueryService = testInjector.getInstance(AutoIndexQueryServiceImpl.class);
    }

    @AfterClass
    public static void afterClass() {
        TestKit.shutdownActorSystem(actorSystem);
        actorSystem = null;
    }

    @Test
    public void testForkBasedOnSingleRun() throws QueryApiException {
        // Create a valid auto index
        final EntityWithDocRef<AutoIndexDocRefEntity> autoIndex = createAutoIndex();
        final String docRefUuid = autoIndex.getDocRef().getUuid();

        timelineTrackerService.setTimelineBounds(docRefUuid, AnimalTestData.TIMELINE_BOUNDS);

        // Give our fixed test service user access to the doc refs
        // The wired Index Job DAO will use this user via Guice injection
        authRule.permitAuthenticatedUser(TEST_SERVICE_USER)
                .docRef(autoIndex.getDocRef())
                .docRef(autoIndex.getEntity().getRawDocRef())
                .docRef(autoIndex.getEntity().getIndexDocRef())
                .permission(DocumentPermission.READ)
                .permission(DocumentPermission.UPDATE)
                .done();

        // Manually force the indexing to occur
        final IndexJob indexJob = indexJobDao.getOrCreate(docRefUuid)
                .orElseThrow(() -> new AssertionError("Index Job Should exist"));
        final SearchResponse indexingSearchResponse = indexJobHandler.search(indexJob);
        indexJobHandler.write(indexJob, indexingSearchResponse);

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

        // Conduct the search
        final SearchRequest searchRequest = AnimalsQueryResourceIT
                .getTestSearchRequest(autoIndex.getDocRef(), expressionOperator, offset);

        final SearchResponse searchResponse = autoIndexQueryService.search(authRule.adminUser(), searchRequest)
                .orElseThrow(() -> new AssertionError("Service failed to provide search response"));

        final Set<AnimalSighting> resultsSet = getAnimalSightingsFromResponse(searchResponse);

        assertTrue("No results seen", searchResponse.getResults().size() > 0);
    }
}
