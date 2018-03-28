package stroom.autoindex.indexing;

import com.google.inject.*;
import com.google.inject.name.Names;
import org.jooq.DSLContext;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.autoindex.AbstractAutoIndexIntegrationTest;
import stroom.autoindex.AutoIndexConstants;
import stroom.autoindex.app.IndexingConfig;
import stroom.autoindex.service.AutoIndexDocRefEntity;
import stroom.autoindex.service.AutoIndexDocRefServiceImpl;
import stroom.autoindex.tracker.AutoIndexTracker;
import stroom.autoindex.tracker.AutoIndexTrackerDao;
import stroom.autoindex.tracker.AutoIndexTrackerDaoImpl;
import stroom.autoindex.tracker.TrackerWindow;
import stroom.query.audit.security.ServiceUser;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This suite of tests exercises the {@link IndexingTimerTask} and it's integration with
 * the doc ref service {@link AutoIndexDocRefServiceImpl} and
 * the job DAO service {@link IndexJobDaoImpl} which is in turn dependant on the real {@link AutoIndexTrackerDaoImpl}
 */
public class IndexingTimerTaskIT extends AbstractAutoIndexIntegrationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(IndexingTimerTaskIT.class);

    private static IndexingTimerTask indexingTimerTask;

    private static AutoIndexTrackerDao autoIndexTrackerDao;

    private static IndexingConfig indexingConfig = IndexingConfig
            .asEnabled()
            .withNumberOfTasksPerRun(3);

    /**
     * We are only really testing that the integration of the window and job management causes the right tasks to be fired off.
     */
    private static TestIndexJobConsumer testIndexJobConsumer;

    @BeforeClass
    public static void beforeClass() {

        final Injector testInjector = Guice.createInjector(new AbstractModule() {
            @Override
            protected void configure() {
                bind(DSLContext.class).toInstance(initialiseJooqDbRule.withDatabase());
                bind(AutoIndexTrackerDao.class).to(AutoIndexTrackerDaoImpl.class);
                bind(IndexJobDao.class).to(IndexJobDaoImpl.class);
                bind(new TypeLiteral<Consumer<IndexJob>>(){})
                        .annotatedWith(Names.named(IndexingTimerTask.TASK_HANDLER_NAME))
                        .to(TestIndexJobConsumer.class)
                        .asEagerSingleton(); // singleton so that the test receives same instance as the underlying timer task
                bind(IndexingConfig.class).toInstance(indexingConfig);
                bind(ServiceUser.class)
                        .annotatedWith(Names.named(AutoIndexConstants.STROOM_SERVICE_USER))
                        .toInstance(serviceUser);
            }
        });

        final Key<Consumer<IndexJob>> taskHandlerKey = Key.get(new TypeLiteral<Consumer<IndexJob>>(){}, Names.named(IndexingTimerTask.TASK_HANDLER_NAME));
        final Object testIndexJobConsumerObj = testInjector.getInstance(taskHandlerKey);
        assertTrue(testIndexJobConsumerObj instanceof TestIndexJobConsumer);
        testIndexJobConsumer = (TestIndexJobConsumer) testIndexJobConsumerObj;
        indexingTimerTask = testInjector.getInstance(IndexingTimerTask.class);
        autoIndexTrackerDao = testInjector.getInstance(AutoIndexTrackerDao.class);
    }

    @Before
    public void beforeTest() {
        testIndexJobConsumer.clear();
    }

    @Test
    public void testRunsSingleJob() {
        // Create a valid auto index
        final EntityWithDocRef<AutoIndexDocRefEntity> autoIndex = createAutoIndex();

        indexingTimerTask.run();

        assertEquals(
                Collections.singletonList(autoIndex.getEntity()),
                testIndexJobConsumer.extractJobs().stream()
                        .map(IndexJob::getAutoIndexDocRefEntity)
                        .collect(Collectors.toList()));
    }

    @Test
    public void testRunsMultipleIterations() {
        final int numberCyclesToGetThroughAllIndexesOnce = 5;
        final int numberOfOuterCycles = 3;

        // Create enough indexes to require numberCyclesToGetThroughAllIndexesOnce
        // iterations of the timer task run, to get through all the indexes
        final Set<AutoIndexDocRefEntity> autoIndexes = IntStream.range(0, indexingConfig.getNumberOfTasksPerRun() * numberCyclesToGetThroughAllIndexesOnce)
                .mapToObj(i -> createAutoIndex())
                .map(EntityWithDocRef::getEntity)
                .collect(Collectors.toSet());

        // Keep a record of the index jobs per doc ref
        final ConcurrentHashMap<String, List<IndexJob>> lastIndexJobByDocRefUuid = new ConcurrentHashMap<>();

        // We then have an outer cycle which attempts to get through all the indexes in 'numberOuterCycles' number of times
        IntStream.range(0, numberOfOuterCycles).forEach(cycle -> {

            final Set<AutoIndexDocRefEntity> jobsToRun = IntStream.range(0, numberCyclesToGetThroughAllIndexesOnce)
                    .mapToObj(i -> {
                        indexingTimerTask.run();
                        return testIndexJobConsumer.extractJobs();
                    })
                    .peek(l -> assertEquals(indexingConfig.getNumberOfTasksPerRun(), l.size()))
                    .flatMap(List::stream)
                    .peek(j -> lastIndexJobByDocRefUuid
                            .computeIfAbsent(j.getAutoIndexDocRefEntity().getUuid(),
                                    u -> new ArrayList<>())
                            .add(j)
                    )
                    .map(IndexJob::getAutoIndexDocRefEntity)
                    .collect(Collectors.toSet());

            assertEquals(autoIndexes.size(), jobsToRun.size());
            assertEquals(autoIndexes, jobsToRun);
        });

        // Check that the jobs go backwards in time, without gaps, for each doc ref UUID
        final AtomicInteger jobsCounted = new AtomicInteger(0);
        final AtomicInteger jobsCompared = new AtomicInteger(0);
        lastIndexJobByDocRefUuid.forEach((uuid, jobs) -> {
            Optional<Long> lastFromOpt = Optional.empty();

            LOGGER.debug("Job's for UUID {}", uuid);

            for (final IndexJob job : jobs) {
                LOGGER.debug(job.toString());

                lastFromOpt.ifPresent(lastFromDate -> {
                    assertEquals(lastFromDate, job.getTrackerWindow().getTo());
                    jobsCompared.incrementAndGet();
                });

                lastFromOpt = Optional.of(job.getTrackerWindow().getFrom());
                jobsCounted.incrementAndGet();
            }

            // Check the tracker information matches what we expect
            final Long from = jobs.get(jobs.size() - 1).getTrackerWindow().getFrom();
            final Long to = jobs.get(0).getTrackerWindow().getTo();

            final AutoIndexTracker tracker = autoIndexTrackerDao.get(uuid);
            assertEquals(
                    Collections.singletonList(TrackerWindow.from(from).to(to)),
                    tracker.getWindows());
        });

        // Double check that the comparisons happened in the volumes expected
        assertEquals(indexingConfig.getNumberOfTasksPerRun() * numberCyclesToGetThroughAllIndexesOnce * numberOfOuterCycles,
                jobsCounted.intValue());
        assertEquals((indexingConfig.getNumberOfTasksPerRun() * numberCyclesToGetThroughAllIndexesOnce) * (numberOfOuterCycles - 1),
                jobsCompared.intValue());
    }
}
