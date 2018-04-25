package stroom.autoindex.indexing;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.javadsl.TestKit;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.util.Modules;
import org.jooq.DSLContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.autoindex.AbstractAutoIndexIntegrationTest;
import stroom.autoindex.app.IndexingConfig;
import stroom.autoindex.service.AutoIndexDocRefEntity;
import stroom.autoindex.service.AutoIndexDocRefServiceImpl;
import stroom.security.ServiceUser;
import stroom.test.AnimalTestData;
import stroom.tracking.*;

import javax.inject.Named;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static stroom.autoindex.AutoIndexConstants.STROOM_SERVICE_USER;
import static stroom.autoindex.AutoIndexConstants.TASK_HANDLER_PARENT;

/**
 * This suite of tests exercises the {@link IndexingTimerTask} and it's integration with
 * the doc ref service {@link AutoIndexDocRefServiceImpl} and
 * the job DAO service {@link IndexJobDaoImpl} which is in turn dependant on the real {@link TimelineTrackerDaoJooqImpl}
 */
public class IndexingTimerTaskIT extends AbstractAutoIndexIntegrationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(IndexingTimerTaskIT.class);

    private static IndexingTimerTask indexingTimerTask;

    private static TimelineTrackerService timelineTrackerService;

    private static IndexingConfig indexingConfig = IndexingConfig
            .asEnabled()
            .withNumberOfTasksPerRun(3);

    private static ActorSystem actorSystem;

    private static TestKit indexJobPostHandler;

    @BeforeClass
    public static void beforeClass() {

        actorSystem = ActorSystem.create();

        indexJobPostHandler = new TestKit(actorSystem);

        final Injector testInjector = Guice.createInjector(Modules.combine(new AbstractModule() {
            @Override
            protected void configure() {
                bind(DSLContext.class).toInstance(initialiseJooqDbRule.withDatabase());
                bind(TimelineTrackerDao.class).to(TimelineTrackerDaoJooqImpl.class);
                bind(TimelineTrackerService.class).to(TimelineTrackerServiceImpl.class);
                bind(IndexJobDao.class).to(IndexJobDaoImpl.class);
                bind(IndexingConfig.class).toInstance(indexingConfig);
                bind(IndexJobHandler.class).to(TestIndexJobConsumer.class);
                bind(ActorSystem.class).toInstance(actorSystem);
            }

            @Provides
            @Named(STROOM_SERVICE_USER)
            public ServiceUser serviceUser() {
                return serviceUser;
            }

            @Provides
            @Named(TASK_HANDLER_PARENT)
            public ActorRef indexJobPostHandler() {
                final ActorRef ar = indexJobPostHandler.getRef();
                LOGGER.info("Task Handler Parent {}", ar);
                return ar;
            }
        }));

        indexingTimerTask = testInjector.getInstance(IndexingTimerTask.class);
        timelineTrackerService = testInjector.getInstance(TimelineTrackerService.class);
    }

    @AfterClass
    public static void afterClass() {
        TestKit.shutdownActorSystem(actorSystem);
        actorSystem = null;
    }

    @Test
    public void testRunsSingleIndex() {
        // Create a valid auto index
        final EntityWithDocRef<AutoIndexDocRefEntity> autoIndex = createAutoIndex();

        // Timeline bounds must be set
        timelineTrackerService.setTimelineBounds(autoIndex.getDocRef().getUuid(),
                AnimalTestData.TIMELINE_BOUNDS);

        indexingTimerTask.run();

        final IndexJob indexJob = indexJobPostHandler.expectMsgClass(IndexJob.class);

        assertEquals(autoIndex.getEntity(), indexJob.getAutoIndexDocRefEntity());
    }

    @Test
    public void testRunsMultipleIndexes() {
        final int numberCyclesToGetThroughAllIndexesOnce = 5;
        final int numberOfCyclesPerIndex = 3;

        // Create enough indexes to require numberCyclesToGetThroughAllIndexesOnce
        // iterations of the timer task run, to get through all the indexes
        final Set<AutoIndexDocRefEntity> autoIndexes =
                IntStream.range(0, indexingConfig.getNumberOfTasksPerRun() * numberCyclesToGetThroughAllIndexesOnce)
                .mapToObj(i -> createAutoIndex())
                .peek(i -> {
                    // Timeline bounds must be set
                    timelineTrackerService.setTimelineBounds(i.getDocRef().getUuid(),
                            AnimalTestData.TIMELINE_BOUNDS);
                })
                .map(EntityWithDocRef::getEntity)
                .collect(Collectors.toSet());

        // Keep a record of the index jobs per doc ref
        final ConcurrentHashMap<String, List<IndexJob>> jobsByDocRefUuid = new ConcurrentHashMap<>();

        // We then have an outer cycle which attempts to get through all the indexes in 'numberOuterCycles' number of times
        IntStream.range(0, numberOfCyclesPerIndex).forEach(cycle -> {

            LOGGER.debug("Running Cycle {}", cycle);

            final Set<AutoIndexDocRefEntity> jobsToRun = IntStream.range(0, numberCyclesToGetThroughAllIndexesOnce)
                    .mapToObj(i -> {
                        indexingTimerTask.run();
                        return IntStream.range(0, indexingConfig.getNumberOfTasksPerRun())
                                .mapToObj(j -> indexJobPostHandler.expectMsgClass(IndexJob.class));
                    })
                    .flatMap(Function.identity())
                    .peek(j -> jobsByDocRefUuid
                            .computeIfAbsent(j.getAutoIndexDocRefEntity().getUuid(),
                                    u -> new ArrayList<>())
                            .add(j)
                    )
                    .map(IndexJob::getAutoIndexDocRefEntity)
                    .collect(Collectors.toSet());

            assertEquals(autoIndexes.size(), jobsToRun.size());
            autoIndexes.forEach(ai -> assertTrue(String.format("Jobs to Run does not contain %s", ai), jobsToRun.contains(ai)));
        });

        // Check that the jobs go backwards in time, without gaps, for each doc ref UUID
        final AtomicInteger jobsCounted = new AtomicInteger(0);
        final AtomicInteger jobsCompared = new AtomicInteger(0);
        jobsByDocRefUuid.forEach((uuid, jobs) -> {
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

            final TimelineTracker tracker = timelineTrackerService.get(uuid);
            assertEquals(
                    Collections.singletonList(TrackerWindow.from(from).to(to)),
                    tracker.getWindows());
        });

        // Double check that the comparisons happened in the volumes expected
        assertEquals(indexingConfig.getNumberOfTasksPerRun() * numberCyclesToGetThroughAllIndexesOnce * numberOfCyclesPerIndex,
                jobsCounted.intValue());
        assertEquals((indexingConfig.getNumberOfTasksPerRun() * numberCyclesToGetThroughAllIndexesOnce) * (numberOfCyclesPerIndex - 1),
                jobsCompared.intValue());
    }
}
