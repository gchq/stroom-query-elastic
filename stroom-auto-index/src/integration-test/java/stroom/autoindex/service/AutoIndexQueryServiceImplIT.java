package stroom.autoindex.service;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import org.elasticsearch.client.transport.TransportClient;
import org.jooq.DSLContext;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.autoindex.AbstractAutoIndexIntegrationTest;
import stroom.autoindex.AutoIndexConstants;
import stroom.autoindex.QueryClientCache;
import stroom.autoindex.animals.AnimalTestData;
import stroom.autoindex.animals.AnimalsQueryResourceIT;
import stroom.autoindex.animals.app.AnimalSighting;
import stroom.autoindex.app.Config;
import stroom.autoindex.app.IndexingConfig;
import stroom.autoindex.indexing.IndexJob;
import stroom.autoindex.indexing.IndexJobConsumer;
import stroom.autoindex.indexing.IndexJobDao;
import stroom.autoindex.indexing.IndexJobDaoImpl;
import stroom.autoindex.indexing.IndexWriter;
import stroom.autoindex.indexing.IndexWriterImpl;
import stroom.autoindex.indexing.IndexingTimerTask;
import stroom.autoindex.tracker.AutoIndexTrackerDao;
import stroom.autoindex.tracker.AutoIndexTrackerDaoImpl;
import stroom.query.api.v2.ExpressionOperator;
import stroom.query.api.v2.ExpressionTerm;
import stroom.query.api.v2.OffsetRange;
import stroom.query.api.v2.SearchRequest;
import stroom.query.api.v2.SearchResponse;
import stroom.query.audit.authorisation.DocumentPermission;
import stroom.query.audit.client.DocRefResourceHttpClient;
import stroom.query.audit.client.QueryResourceHttpClient;
import stroom.query.audit.rest.DocRefResource;
import stroom.query.audit.rest.QueryResource;
import stroom.query.audit.security.ServiceUser;
import stroom.query.audit.service.DocRefService;
import stroom.query.elastic.transportClient.TransportClientBundle;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static stroom.autoindex.AutoIndexConstants.TASK_HANDLER_NAME;
import static stroom.autoindex.TestConstants.TEST_SERVICE_USER;
import static stroom.autoindex.animals.AnimalsQueryResourceIT.getAnimalSightingsFromResponse;

public class AutoIndexQueryServiceImplIT extends AbstractAutoIndexIntegrationTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(AutoIndexQueryServiceImplIT.class);

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
    private static AutoIndexTrackerDao trackerDao;

    /**
     * This is the instance of the service under test
     */
    private static AutoIndexQueryServiceImpl service;

    /**
     * The cache of query resources, should serve up Client Spy's
     */
    private static QueryClientCache<QueryResource> queryClientCache;

    @BeforeClass
    public static void beforeClass() {

        final Injector testInjector = Guice.createInjector(new AbstractModule() {
            @Override
            protected void configure() {
                bind(DSLContext.class).toInstance(initialiseJooqDbRule.withDatabase());
                bind(AutoIndexTrackerDao.class).to(AutoIndexTrackerDaoImpl.class);
                bind(IndexJobDao.class).to(IndexJobDaoImpl.class);
                bind(IndexWriter.class).to(IndexWriterImpl.class);
                bind(DocRefService.class).to(AutoIndexDocRefServiceImpl.class);
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
                        .toInstance(new QueryClientCache<>(autoIndexAppRule.getConfiguration(), u -> QueryResourceClientSpy.wrapping(new QueryResourceHttpClient(u))));
                bind(new TypeLiteral<QueryClientCache<DocRefResource>>(){})
                        .toInstance(new QueryClientCache<>(autoIndexAppRule.getConfiguration(), DocRefResourceHttpClient::new));
            }
        });

        final Key<Consumer<IndexJob>> taskHandlerKey = Key.get(new TypeLiteral<Consumer<IndexJob>>(){}, Names.named(TASK_HANDLER_NAME));
        final Object testIndexJobConsumerObj = testInjector.getInstance(taskHandlerKey);
        assertTrue(testIndexJobConsumerObj instanceof IndexJobConsumer);
        indexJobConsumer = (IndexJobConsumer) testIndexJobConsumerObj;
        indexJobDao = testInjector.getInstance(IndexJobDao.class);
        trackerDao = testInjector.getInstance(AutoIndexTrackerDao.class);
        service = testInjector.getInstance(AutoIndexQueryServiceImpl.class);
        queryClientCache = testInjector.getInstance(Key.get(new TypeLiteral<QueryClientCache<QueryResource>>(){}));
    }

    @Test
    public void testServiceForksQuery() throws Exception, RuntimeException {
        // Create a valid auto index
        final EntityWithDocRef<AutoIndexDocRefEntity> autoIndex = createAutoIndex();

        trackerDao.setTimelineBounds(autoIndex.getDocRef().getUuid(), AnimalTestData.TIMELINE_BOUNDS);

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
        final IndexJob indexJob = indexJobDao.getOrCreate(autoIndex.getEntity())
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

        // Get hold of the various client spies for the query resource
        final QueryResourceClientSpy<QueryResourceHttpClient> rawQueryClient =
                queryClientCache.apply(autoIndex.getEntity().getRawDocRef().getType())
                .filter(c -> c instanceof QueryResourceClientSpy)
                .map(c -> (QueryResourceClientSpy<QueryResourceHttpClient>) c)
                .orElseThrow(() -> new RuntimeException("Could not get query resource client spy (raw)"));

        final QueryResourceClientSpy<QueryResourceHttpClient> indexQueryClient =
                queryClientCache.apply(autoIndex.getEntity().getIndexDocRef().getType())
                .filter(c -> c instanceof QueryResourceClientSpy)
                .map(c -> (QueryResourceClientSpy<QueryResourceHttpClient>) c)
                .orElseThrow(() -> new RuntimeException("Could not get query resource client spy (index)"));

        // Clear any existing intercepted searches
        Stream.of(rawQueryClient, indexQueryClient)
                .forEach(QueryResourceClientSpy::clearCalls);

        // Conduct the search
        final SearchRequest searchRequest = AnimalsQueryResourceIT
                .getTestSearchRequest(autoIndex.getDocRef(), expressionOperator, offset);

        final SearchResponse searchResponse =
                service.search(authRule.adminUser(), searchRequest)
                        .orElseThrow(() -> new RuntimeException("No search response given"));

        final Set<AnimalSighting> resultsSet = getAnimalSightingsFromResponse(searchResponse);

        assertTrue("No results seen", searchResponse.getResults().size() > 0);

        // Check that the search was forked to both underlying data sources correctly
        final List<QueryResourceClientSpy.SearchCall> rawSearchCalls = rawQueryClient.getSearchCalls();
        final List<QueryResourceClientSpy.SearchCall> indexSearchCalls = indexQueryClient.getSearchCalls();

        assertEquals(1, rawSearchCalls.size());
        assertEquals(1, indexSearchCalls.size());

        QueryResourceClientSpy.SearchCall rawSearchCall = rawSearchCalls.get(0);
        QueryResourceClientSpy.SearchCall indexSearchCall = indexSearchCalls.get(0);

        LOGGER.info("Raw Search Call {}", rawSearchCall);
        LOGGER.info("Index Search Call {}", indexSearchCall);
    }
}
