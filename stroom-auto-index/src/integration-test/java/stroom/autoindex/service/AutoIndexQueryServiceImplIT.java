package stroom.autoindex.service;

import akka.actor.ActorSystem;
import akka.testkit.javadsl.TestKit;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.util.Modules;
import org.elasticsearch.client.transport.TransportClient;
import org.jooq.DSLContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.authorisation.DocumentPermission;
import stroom.autoindex.AbstractAutoIndexIntegrationTest;
import stroom.autoindex.app.Config;
import stroom.autoindex.app.IndexingConfig;
import stroom.autoindex.indexing.*;
import stroom.query.api.v2.*;
import stroom.query.audit.client.NotFoundException;
import stroom.query.audit.service.DocRefService;
import stroom.query.audit.service.QueryService;
import stroom.query.audit.service.QueryServiceSupplier;
import stroom.query.csv.CsvDocRefEntity;
import stroom.query.elastic.model.ElasticIndexDocRefEntity;
import stroom.query.elastic.transportClient.TransportClientBundle;
import stroom.query.testing.RemoteClientTestingModule;
import stroom.security.ServiceUser;
import stroom.test.AnimalSighting;
import stroom.test.AnimalTestData;
import stroom.tracking.TimelineTrackerDao;
import stroom.tracking.TimelineTrackerDaoJooqImpl;
import stroom.tracking.TimelineTrackerService;
import stroom.tracking.TimelineTrackerServiceImpl;

import javax.inject.Named;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Set;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static stroom.autoindex.AutoIndexConstants.STROOM_SERVICE_USER;
import static stroom.autoindex.TestConstants.TEST_SERVICE_USER;
import static stroom.test.AnimalTestData.getAnimalSightingsFromResponse;

public class AutoIndexQueryServiceImplIT extends AbstractAutoIndexIntegrationTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(AutoIndexQueryServiceImplIT.class);

    private static ActorSystem actorSystem;

    private static IndexJobDao indexJobDao;

    private static IndexingConfig indexingConfig = IndexingConfig
            .asEnabled()
            .withNumberOfTasksPerRun(3);

    /**
     * We are only really testing that the integration of the window and job management causes the right tasks to be fired off.
     */
    private static IndexJobHandler indexJobHandler;

    /**
     * We will use this to manually tell the system that we already have data that runs from 'now' back to
     * the end date of our test data.
     */
    private static TimelineTrackerService timelineTrackerService;

    /**
     * This is the instance of the service under test
     */
    private static AutoIndexQueryServiceImpl service;

    /**
     * The cache of query resources, should serve up Client Spy's
     */
    private static QueryServiceSupplier queryServiceSupplier;

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
                bind(ActorSystem.class).toInstance(actorSystem);
                bind(IndexJobHandler.class)
                        .to(IndexJobHandlerImpl.class)
                        .asEagerSingleton(); // singleton so that the test receives same instance as the underlying timer task
                bind(IndexingConfig.class).toInstance(indexingConfig);
                bind(Config.class).toInstance(autoIndexAppRule.getConfiguration());
                bind(TransportClient.class)
                        .toInstance(TransportClientBundle.createTransportClient(autoIndexAppRule.getConfiguration()));
            }

            @Provides
            @Named(STROOM_SERVICE_USER)
            public ServiceUser serviceUser() {
                return serviceUser;
            }
        }),
                new RemoteClientTestingModule(autoIndexAppRule.getConfiguration().getQueryResourceUrlsByType())
                        .addType(ElasticIndexDocRefEntity.TYPE, ElasticIndexDocRefEntity.class)
        );

        indexJobHandler = testInjector.getInstance(IndexJobHandler.class);
        indexJobDao = testInjector.getInstance(IndexJobDao.class);
        timelineTrackerService = testInjector.getInstance(TimelineTrackerService.class);
        service = testInjector.getInstance(AutoIndexQueryServiceImpl.class);
        queryServiceSupplier = testInjector.getInstance(QueryServiceSupplier.class);
    }

    @AfterClass
    public static void afterClass() {
        TestKit.shutdownActorSystem(actorSystem);
        actorSystem = null;
    }

    @Test
    public void testServiceForksQuery() throws Exception {
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

        // Get hold of the various client spies for the query resource
        final QueryService rawQueryClient =
                queryServiceSupplier.apply(autoIndex.getEntity().getRawDocRef().getType())
                        .orElseThrow(() -> new RuntimeException("Could not get query resource client spy (raw)"));

        final QueryService indexQueryClient =
                queryServiceSupplier.apply(autoIndex.getEntity().getIndexDocRef().getType())
                        .orElseThrow(() -> new RuntimeException("Could not get query resource client spy (index)"));

        // Manually force the indexing to occur
        final IndexJob indexJob = indexJobDao.getOrCreate(docRefUuid)
                .orElseThrow(() -> new AssertionError("Index Job Should exist"));
        final SearchResponse indexingSearchResponse = indexJobHandler.search(indexJob);
        indexJobHandler.write(indexJob, indexingSearchResponse);

        // The raw search will have been called to populate the index
        Mockito.verify(rawQueryClient).search(Mockito.any(), Mockito.any());
        reset(rawQueryClient);

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
        final SearchRequest searchRequest = AnimalTestData
                .getTestSearchRequest(autoIndex.getDocRef(), expressionOperator, offset);

        final SearchResponse searchResponse =
                service.search(authRule.adminUser(), searchRequest)
                        .orElseThrow(() -> new RuntimeException("No search response given"));

        final Set<AnimalSighting> resultsSet = getAnimalSightingsFromResponse(searchResponse);

        assertTrue("No results seen", searchResponse.getResults().size() > 0);

        // Check that the search was forked to both underlying data sources correctly
        Mockito.verify(rawQueryClient).search(Mockito.any(), Mockito.any());
        Mockito.verify(indexQueryClient).search(Mockito.any(), Mockito.any());
    }
}
