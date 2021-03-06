package stroom.autoindex.indexing;

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
import stroom.autoindex.app.Config;
import stroom.autoindex.app.IndexingConfig;
import stroom.autoindex.service.AutoIndexDocRefEntity;
import stroom.autoindex.tracker.TimelineTrackerDao;
import stroom.autoindex.tracker.TimelineTrackerDaoJooqImpl;
import stroom.autoindex.tracker.TimelineTrackerService;
import stroom.autoindex.tracker.TimelineTrackerServiceImpl;
import stroom.query.audit.authorisation.DocumentPermission;
import stroom.query.audit.client.DocRefResourceHttpClient;
import stroom.query.audit.client.QueryResourceHttpClient;
import stroom.query.audit.rest.DocRefResource;
import stroom.query.audit.rest.QueryResource;
import stroom.query.audit.security.ServiceUser;
import stroom.query.elastic.transportClient.TransportClientBundle;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static stroom.autoindex.AutoIndexConstants.TASK_HANDLER_NAME;
import static stroom.autoindex.TestConstants.TEST_SERVICE_USER;

/**
 * This tests the {@link IndexJobConsumer} with a real {@link IndexJobDao}
 *
 * The jobs are being requested within the test and fired manually at the consumer.
 * In the running application, this process would be executed by the {@link IndexingTimerTask}
 */
public class IndexJobConsumerIT extends AbstractAutoIndexIntegrationTest {
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
    public void testSingleRun() {
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

        final IndexJob indexJob = indexJobDao.getOrCreate(docRefUuid)
                .orElseThrow(() -> new AssertionError("Index Job Should exist"));

        indexJobConsumer.accept(indexJob);
    }

    @Test
    public void testRunThroughAllValidTime() {
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

        // Attempt to run the number of jobs that would completely cover the time range
        final List<IndexJob> jobs = AnimalTestData.getExpectedTrackerWindows().stream()
                .map(tw -> indexJobDao.getOrCreate(docRefUuid))
                .filter(Optional::isPresent).map(Optional::get)
                .peek(indexJobConsumer)
                .collect(Collectors.toList());

        LOGGER.debug("Index Jobs Processed {}", jobs.size());

        // Check that all the expected time windows were processed
        assertEquals(AnimalTestData.getExpectedTrackerWindows(),
                jobs.stream()
                        .map(IndexJob::getTrackerWindow)
                        .collect(Collectors.toList()));
    }
}
