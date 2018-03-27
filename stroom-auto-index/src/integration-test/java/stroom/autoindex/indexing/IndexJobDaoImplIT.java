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
import stroom.autoindex.service.AutoIndexDocRefEntity;
import stroom.autoindex.tracker.AutoIndexTracker;
import stroom.autoindex.tracker.AutoIndexTrackerDao;
import stroom.autoindex.tracker.AutoIndexTrackerDaoImpl;
import stroom.query.audit.security.ServiceUser;

import java.util.Collections;
import java.util.Optional;
import java.util.UUID;

import static org.junit.Assert.*;
import static stroom.autoindex.TestConstants.TEST_SERVICE_USER;

public class IndexJobDaoImplIT extends AbstractAutoIndexIntegrationTest {

    /**
     * Create our own Index Tracker DAO for direct testing
     */
    private static IndexJobDaoImpl indexJobDao;

    private static AutoIndexTrackerDao autoIndexTrackerDao;

    @BeforeClass
    public static void beforeClass() {
        final Injector testInjector = Guice.createInjector(new AbstractModule() {
            @Override
            protected void configure() {
                bind(DSLContext.class).toInstance(initialiseJooqDbRule.withDatabase());
                bind(AutoIndexTrackerDao.class).to(AutoIndexTrackerDaoImpl.class);
                bind(IndexJobDao.class).to(IndexJobDaoImpl.class);
                bind(ServiceUser.class)
                        .annotatedWith(Names.named(AutoIndexConstants.STROOM_SERVICE_USER))
                        .toInstance(serviceUser);
            }
        });

        indexJobDao = testInjector.getInstance(IndexJobDaoImpl.class);
        autoIndexTrackerDao = testInjector.getInstance(AutoIndexTrackerDao.class);
    }

    @Test
    public void testGetOrCreateValid() {
        // Create a valid auto index
        final EntityWithDocRef<AutoIndexDocRefEntity> autoIndex = createAutoIndex();

        // Create an index job
        final IndexJob indexJob = indexJobDao.getOrCreate(autoIndex.getEntity());
        assertNotNull(indexJob);

        // Make a repeat request, should get the same index job back
        final IndexJob sameIndexJob = indexJobDao.getOrCreate(autoIndex.getEntity());
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

        // Create an index job
        final IndexJob indexJob = indexJobDao.getOrCreate(autoIndex.getEntity());
        assertEquals(0, indexJob.getStartedTimeMillis());

        // Take note of the time, then mark the job as started
        final long timeBeforeMarking = System.currentTimeMillis();
        indexJobDao.markAsStarted(indexJob.getJobId());

        // Now re-fetch the job and check that the started time is there
        final IndexJob jobAfterStarted = indexJobDao.getOrCreate(autoIndex.getEntity());
        assertTrue(jobAfterStarted.getStartedTimeMillis() >= timeBeforeMarking);
    }

    @Test
    public void testCompleteTask() {
        // Create a valid auto index
        final EntityWithDocRef<AutoIndexDocRefEntity> autoIndex = createAutoIndex();

        // Create an index job
        final IndexJob indexJob = indexJobDao.getOrCreate(autoIndex.getEntity());
        assertEquals(0, indexJob.getStartedTimeMillis());

        // Mark the job as complete
        indexJobDao.markAsComplete(indexJob.getJobId());

        // Check that the job can no longer be found
        final Optional<IndexJob> indexJobAfterComplete = indexJobDao.get(indexJob.getJobId());
        assertFalse(indexJobAfterComplete.isPresent());

        // Retrieve the auto index entity and check the tracker window were updated
        final AutoIndexTracker tracker = autoIndexTrackerDao.get(autoIndex.getDocRef().getUuid());
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
