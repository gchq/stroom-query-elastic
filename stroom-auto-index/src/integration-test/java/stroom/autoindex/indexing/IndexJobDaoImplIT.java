package stroom.autoindex.indexing;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.junit.BeforeClass;
import org.junit.Test;
import stroom.autoindex.AbstractAutoIndexIntegrationTest;
import stroom.autoindex.AutoIndexDocRefEntity;
import stroom.autoindex.DSLContextBuilder;
import stroom.autoindex.tracker.AutoIndexTrackerDao;
import stroom.autoindex.tracker.AutoIndexTrackerDaoImpl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class IndexJobDaoImplIT extends AbstractAutoIndexIntegrationTest {

    /**
     * Create our own Index Tracker DAO for direct testing
     */
    private static IndexJobDaoImpl indexJobDao;

    @BeforeClass
    public static void beforeClass() {
        final Injector testInjector = Guice.createInjector(new AbstractModule() {
            @Override
            protected void configure() {
                bind(DSLContext.class).toInstance(initialiseJooqDbRule.withDatabase());
                bind(AutoIndexTrackerDao.class).to(AutoIndexTrackerDaoImpl.class);
            }
        });

        indexJobDao = testInjector.getInstance(IndexJobDaoImpl.class);
    }

    @Test
    public void testGetOrCreateValid() throws Exception {
        // Create a valid auto index
        final EntityWithDocRef<AutoIndexDocRefEntity> autoIndex = createAutoIndex();

        // Create an index job
        final IndexJob indexJob = indexJobDao.getOrCreate(autoIndex.getDocRef().getUuid());
        assertNotNull(indexJob);

        // Make a repeat request, should get the same index job back
        final IndexJob sameIndexJob = indexJobDao.getOrCreate(autoIndex.getDocRef().getUuid());
        assertEquals(indexJob.getJobId(), sameIndexJob.getJobId());

        final long indexJobsInTable = initialiseJooqDbRule.withDatabase()
                .transactionResult(c -> (long) DSL.using(c)
                        .select()
                        .from(IndexJobDaoImpl.JOB_TABLE)
                        .fetch()
                        .size());
        assertEquals(1, indexJobsInTable);
    }
}
