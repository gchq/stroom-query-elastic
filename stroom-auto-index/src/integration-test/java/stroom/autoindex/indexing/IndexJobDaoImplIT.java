package stroom.autoindex.indexing;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.jooq.DSLContext;
import org.junit.BeforeClass;
import org.junit.Test;
import stroom.autoindex.AbstractAutoIndexIntegrationTest;
import stroom.autoindex.AutoIndexDocRefEntity;
import stroom.autoindex.DSLContextBuilder;
import stroom.autoindex.tracker.AutoIndexTrackerDao;
import stroom.autoindex.tracker.AutoIndexTrackerDaoImpl;

import static org.junit.Assert.assertNotNull;

public class IndexJobDaoImplIT extends AbstractAutoIndexIntegrationTest {

    /**
     * Create our own Index Tracker DAO for direct testing
     */
    private static IndexJobDaoImpl indexJobDao;

    /**
     * Injector for creating instances of the server outside of the running application.
     */
    private static Injector injector;

    @BeforeClass
    public static void beforeClass() {
        injector = Guice.createInjector(new AbstractModule() {
            @Override
            protected void configure() {
                bind(DSLContext.class).toInstance(DSLContextBuilder.withUrl(autoIndexAppRule.getConfiguration().getDataSourceFactory().getUrl())
                        .username(autoIndexAppRule.getConfiguration().getDataSourceFactory().getUser())
                        .password(autoIndexAppRule.getConfiguration().getDataSourceFactory().getPassword())
                        .build());

                bind(AutoIndexTrackerDao.class).to(AutoIndexTrackerDaoImpl.class);
            }
        });

        indexJobDao = injector.getInstance(IndexJobDaoImpl.class);
    }

    @Test
    public void testGetOrCreateValid() throws Exception {
        final EntityWithDocRef<AutoIndexDocRefEntity> autoIndex = createAutoIndex();

        final IndexJob indexJob = indexJobDao.getOrCreate(autoIndex.getDocRef().getUuid());

        assertNotNull(indexJob);
    }
}
