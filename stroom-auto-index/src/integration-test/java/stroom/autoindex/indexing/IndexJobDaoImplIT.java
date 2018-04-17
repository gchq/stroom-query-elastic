package stroom.autoindex.indexing;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.name.Names;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.junit.BeforeClass;
import org.junit.Test;
import stroom.autoindex.AbstractAutoIndexIntegrationTest;
import stroom.autoindex.AutoIndexConstants;
import stroom.autoindex.animals.AnimalTestData;
import stroom.autoindex.service.AutoIndexDocRefEntity;
import stroom.query.audit.security.ServiceUser;
import stroom.tracking.*;

import java.util.Collections;
import java.util.UUID;

import static org.junit.Assert.*;

public class IndexJobDaoImplIT extends AbstractAutoIndexIntegrationTest {

    /**
     * Create our own Index Tracker DAO for direct testing
     */
    private static IndexJobDaoImpl indexJobDao;

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
                bind(ServiceUser.class)
                        .annotatedWith(Names.named(AutoIndexConstants.STROOM_SERVICE_USER))
                        .toInstance(serviceUser);
            }
        });

        indexJobDao = testInjector.getInstance(IndexJobDaoImpl.class);
        timelineTrackerService = testInjector.getInstance(TimelineTrackerService.class);
    }

    @Test
    public void testGetOrCreateValid() {
        // Create a valid auto index
        final EntityWithDocRef<AutoIndexDocRefEntity> autoIndex = createAutoIndex();
        final String docRefUuid = autoIndex.getDocRef().getUuid();

        timelineTrackerService.setTimelineBounds(docRefUuid,
                AnimalTestData.TIMELINE_BOUNDS);

        // Create an index job
        final IndexJob indexJob = indexJobDao.getOrCreate(docRefUuid)
                .orElseThrow(() -> new AssertionError("Index Job Should exist"));
        assertNotNull(indexJob);

        // Make a repeat request, should get the same index job back
        final IndexJob sameIndexJob = indexJobDao.getOrCreate(docRefUuid)
                .orElseThrow(() -> new AssertionError("Index Job Should exist"));
        assertEquals(indexJob.getJobId(), sameIndexJob.getJobId());

        final long indexJobsInTable = initialiseJooqDbRule.withDatabase()
                .transactionResult(c -> (long) DSL.using(c)
                        .select()
                        .from(IndexJobDaoImpl.JOB_TABLE)
                        .fetch()
                        .size());
        assertEquals(1, indexJobsInTable);
    }

    @Test
    public void testStartTask() {
        // Create a valid auto index
        final EntityWithDocRef<AutoIndexDocRefEntity> autoIndex = createAutoIndex();
        final String docRefUuid = autoIndex.getDocRef().getUuid();

        // Timeline bounds must be set
        timelineTrackerService.setTimelineBounds(docRefUuid,
                AnimalTestData.TIMELINE_BOUNDS);

        // Create an index job
        final IndexJob indexJob = indexJobDao.getOrCreate(docRefUuid)
                .orElseThrow(() -> new AssertionError("Index Job Should exist"));
        assertEquals(0, indexJob.getStartedTimeMillis());

        // Take note of the time, then mark the job as started
        final long timeBeforeMarking = System.currentTimeMillis();
        indexJobDao.markAsStarted(indexJob.getJobId());

        // Now re-fetch the job and check that the started time is there
        final IndexJob jobAfterStarted = indexJobDao.get(indexJob.getJobId())
                .orElseThrow(() -> new AssertionError("Index Job Should exist"));
        assertTrue(jobAfterStarted.getStartedTimeMillis() >= timeBeforeMarking);
    }

    @Test
    public void testCompleteTask() {
        // Create a valid auto index
        final EntityWithDocRef<AutoIndexDocRefEntity> autoIndex = createAutoIndex();
        final String docRefUuid = autoIndex.getDocRef().getUuid();

        // Timeline bounds must be set
        timelineTrackerService.setTimelineBounds(docRefUuid,
                AnimalTestData.TIMELINE_BOUNDS);

        // Create an index job
        final IndexJob indexJob = indexJobDao.getOrCreate(docRefUuid)
                .orElseThrow(() -> new AssertionError("Index Job Should exist"));
        assertEquals(0, indexJob.getStartedTimeMillis());

        // Mark as started, as it has to be started before it can be completed
        indexJobDao.markAsStarted(indexJob.getJobId());

        // Mark the job as complete
        indexJobDao.markAsComplete(indexJob.getJobId());

        // Check that the job has the completed time set
        final IndexJob indexJobAfterComplete = indexJobDao.get(indexJob.getJobId())
                .orElseThrow(() -> new AssertionError("Index Job Should exist"));
        assertTrue(indexJobAfterComplete.getCompletedTimeMillis() > 0);

        // Retrieve the auto index entity and check the tracker window were updated
        final TimelineTracker tracker = timelineTrackerService.get(docRefUuid);
        assertEquals(Collections.singletonList(indexJob.getTrackerWindow()), tracker.getWindows());
    }

    @Test(expected = RuntimeException.class)
    public void testStartNonExistentTask() {
        indexJobDao.markAsStarted(UUID.randomUUID().toString());
    }

    @Test(expected = RuntimeException.class)
    public void testCompleteNonExistentTask() {
        indexJobDao.markAsComplete(UUID.randomUUID().toString());
    }
}
