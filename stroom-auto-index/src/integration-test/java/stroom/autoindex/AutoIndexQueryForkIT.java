package stroom.autoindex;

import com.google.inject.*;
import com.google.inject.name.Names;
import org.eclipse.jetty.http.HttpStatus;
import org.elasticsearch.client.transport.TransportClient;
import org.jooq.DSLContext;
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
import stroom.query.api.v2.*;
import stroom.query.audit.authorisation.DocumentPermission;
import stroom.query.audit.client.DocRefResourceHttpClient;
import stroom.query.audit.client.QueryResourceHttpClient;
import stroom.query.audit.rest.DocRefResource;
import stroom.query.audit.rest.QueryResource;
import stroom.query.audit.security.ServiceUser;
import stroom.query.elastic.transportClient.TransportClientBundle;
import stroom.tracking.TimelineTrackerDao;
import stroom.tracking.TimelineTrackerDaoJooqImpl;
import stroom.tracking.TimelineTrackerService;
import stroom.tracking.TimelineTrackerServiceImpl;

import javax.ws.rs.core.Response;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Set;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static stroom.autoindex.AutoIndexConstants.TASK_HANDLER_NAME;
import static stroom.autoindex.TestConstants.TEST_SERVICE_USER;
import static stroom.autoindex.animals.AnimalsQueryResourceIT.getAnimalSightingsFromResponse;

/**
 * This will run a test that ensures some portion of the data has been indexed
 * and that the query gets split both ways
 */
public class AutoIndexQueryForkIT extends AbstractAutoIndexIntegrationTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexJobConsumerIT.class);

    private static IndexJobDao indexJobDao;

    private static IndexingConfig indexingConfig = IndexingConfig
            .asEnabled()
            .withNumberOfTasksPerRun(3);

    /**
     * We are only really testing that the integration of the window and job management causes the right tasks to be fired off.
     */
    private static IndexJobConsumer indexJobConsumer;

    /**
     * We will use this to manually tell the system that we already have data that runs from 'now' back to
     * the end date of our test data.
     */
    private static TimelineTrackerService timelineTrackerService;

    @BeforeClass
    public static void beforeClass() {

        final Injector testInjector = Guice.createInjector(new AbstractModule() {
            @Override
            protected void configure() {
                bind(DSLContext.class).toInstance(initialiseJooqDbRule.withDatabase());
                bind(TimelineTrackerDao.class).to(TimelineTrackerDaoJooqImpl.class);
                bind(TimelineTrackerService.class).to(TimelineTrackerServiceImpl.class);
                bind(IndexJobDao.class).to(IndexJobDaoImpl.class);
                bind(IndexWriter.class).to(IndexWriterImpl.class);
                bind(new TypeLiteral<Consumer<IndexJob>>(){})
                        .annotatedWith(Names.named(TASK_HANDLER_NAME))
                        .to(IndexJobConsumer.class)
                        .asEagerSingleton(); // singleton so that the test receives same instance as the underlying timer task
                bind(IndexingConfig.class).toInstance(indexingConfig);
                bind(Config.class).toInstance(autoIndexAppRule.getConfiguration());
                bind(ServiceUser.class)
                        .annotatedWith(Names.named(AutoIndexConstants.STROOM_SERVICE_USER))
                        .toInstance(serviceUser);
                bind(TransportClient.class)
                        .toInstance(TransportClientBundle.createTransportClient(autoIndexAppRule.getConfiguration()));
                bind(new TypeLiteral<QueryClientCache<QueryResource>>(){})
                        .toInstance(new QueryClientCache<>(autoIndexAppRule.getConfiguration(), QueryResourceHttpClient::new));
                bind(new TypeLiteral<QueryClientCache<DocRefResource>>(){})
                        .toInstance(new QueryClientCache<>(autoIndexAppRule.getConfiguration(), DocRefResourceHttpClient::new));
            }
        });

        final Key<Consumer<IndexJob>> taskHandlerKey = Key.get(new TypeLiteral<Consumer<IndexJob>>(){}, Names.named(TASK_HANDLER_NAME));
        final Object testIndexJobConsumerObj = testInjector.getInstance(taskHandlerKey);
        assertTrue(testIndexJobConsumerObj instanceof IndexJobConsumer);
        indexJobConsumer = (IndexJobConsumer) testIndexJobConsumerObj;
        indexJobDao = testInjector.getInstance(IndexJobDao.class);
        timelineTrackerService = testInjector.getInstance(TimelineTrackerService.class);
    }

    @Test
    public void testForkBasedOnSingleRun() {
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
        indexJobConsumer.accept(indexJob);

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

        final Response response = autoIndexQueryClient.search(authRule.adminUser(), searchRequest);
        assertEquals(HttpStatus.OK_200, response.getStatus());

        final SearchResponse searchResponse = response.readEntity(SearchResponse.class);

        final Set<AnimalSighting> resultsSet = getAnimalSightingsFromResponse(searchResponse);

        assertTrue("No results seen", searchResponse.getResults().size() > 0);
    }
}
