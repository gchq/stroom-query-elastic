package stroom.autoindex.tracker;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.jooq.DSLContext;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import stroom.autoindex.App;
import stroom.autoindex.AutoIndexDocRefEntity;
import stroom.autoindex.Config;
import stroom.autoindex.DSLContextBuilder;
import stroom.autoindex.DeleteFromTableRule;
import stroom.autoindex.TestConstants;
import stroom.autoindex.TimeUtils;
import stroom.autoindex.indexing.IndexJob;
import stroom.query.testing.DropwizardAppWithClientsRule;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.dropwizard.testing.ResourceHelpers.resourceFilePath;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class AutoIndexTrackerDaoIT {

    /**
     * Having the app rule will ensure the database gets migrated, it also gives us access to the configuration
     */
    @ClassRule
    public static final DropwizardAppWithClientsRule<Config> appRule =
            new DropwizardAppWithClientsRule<>(App.class, resourceFilePath(TestConstants.AUTO_INDEX_APP_CONFIG));


    @Rule
    public final DeleteFromTableRule<Config> clearDbRule = DeleteFromTableRule.withApp(appRule)
            .table(AutoIndexDocRefEntity.TABLE_NAME)
            .table(AutoIndexTracker.TABLE_NAME)
            .table(IndexJob.TABLE_NAME)
            .build();

    /**
     * Create our own Index Tracker DAO for direct testing
     */
    private static AutoIndexTrackerDao autoIndexTrackerDao;

    /**
     * Injector for creating instances of the server outside of the running application.
     */
    private static Injector injector;

    @BeforeClass
    public static void beforeClass() {
        injector = Guice.createInjector(new AbstractModule() {
            @Override
            protected void configure() {
                bind(DSLContext.class).toInstance(DSLContextBuilder.withUrl(appRule.getConfiguration().getDataSourceFactory().getUrl())
                        .username(appRule.getConfiguration().getDataSourceFactory().getUser())
                        .password(appRule.getConfiguration().getDataSourceFactory().getPassword())
                        .build());
            }
        });

        autoIndexTrackerDao = injector.getInstance(AutoIndexTrackerDaoImpl.class);
    }

    @Test
    public void testGetEmptyTracker() {
        // Given
        final String docRefUuid = UUID.randomUUID().toString();

        // When
        final AutoIndexTracker tracker = autoIndexTrackerDao.get(docRefUuid);

        // Then
        assertNotNull(tracker);
        assertEquals(docRefUuid, tracker.getDocRefUuid());
    }

    @Test
    public void testAddWindow() {
        // Given
        final String docRefUuid = UUID.randomUUID().toString();
        final LocalDateTime now = TimeUtils.nowUtcSeconds();
        final LocalDateTime oneMonthAgo = now.minusMonths(1);
        final LocalDateTime twoMonthsAgo = oneMonthAgo.minusMonths(1);
        final LocalDateTime threeMonthsAgo = oneMonthAgo.minusMonths(1);

        // Two non-contiguous windows
        final TrackerWindow oneMonthAgoToNow = TrackerWindow.from(oneMonthAgo).to(now);
        final TrackerWindow threeMonthsAgoToTwoMonthsAgo = TrackerWindow.from(threeMonthsAgo).to(twoMonthsAgo);

        // When
        final AutoIndexTracker tracker1 = autoIndexTrackerDao.addWindow(docRefUuid, oneMonthAgoToNow);
        final AutoIndexTracker tracker2 = autoIndexTrackerDao.addWindow(docRefUuid, threeMonthsAgoToTwoMonthsAgo);
        final AutoIndexTracker tracker3 = autoIndexTrackerDao.get(docRefUuid);

        // Then
        assertEquals(Collections.singletonList(oneMonthAgoToNow),
                tracker1.getWindows());
        assertEquals(Stream.of(threeMonthsAgoToTwoMonthsAgo, oneMonthAgoToNow)
                        .collect(Collectors.toList()),
                tracker2.getWindows());

        assertEquals(tracker2, tracker3);
    }

    @Test
    public void testAddWindowsToMerge() {
        // Given
        final String docRefUuid = UUID.randomUUID().toString();
        final LocalDateTime now = TimeUtils.nowUtcSeconds();
        final LocalDateTime oneMonthAgo = now.minusMonths(1);
        final LocalDateTime twoMonthsAgo = oneMonthAgo.minusMonths(1);

        // Two windows that should join up
        final TrackerWindow oneMonthAgoToNow = TrackerWindow.from(oneMonthAgo).to(now);
        final TrackerWindow twoMonthsAgoToOneMonthAgo = TrackerWindow.from(twoMonthsAgo).to(oneMonthAgo);

        // When
        autoIndexTrackerDao.addWindow(docRefUuid, oneMonthAgoToNow);
        autoIndexTrackerDao.addWindow(docRefUuid, twoMonthsAgoToOneMonthAgo);
        final AutoIndexTracker tracker = autoIndexTrackerDao.get(docRefUuid);

        // Then
        // Should up with one big window that covers it all
        assertEquals(Collections.singletonList(TrackerWindow.from(twoMonthsAgo).to(now)),
                tracker.getWindows());
    }

    @Test
    public void testClearWindow() {
        // Given
        final String docRefUuid = UUID.randomUUID().toString();
        final LocalDateTime now = TimeUtils.nowUtcSeconds();
        final LocalDateTime oneMonthAgo = now.minusMonths(1);
        final LocalDateTime twoMonthsAgo = oneMonthAgo.minusMonths(1);
        final LocalDateTime threeMonthsAgo = oneMonthAgo.minusMonths(1);

        // Two non-contiguous windows
        final TrackerWindow oneMonthAgoToNow = TrackerWindow.from(oneMonthAgo).to(now);
        final TrackerWindow threeMonthsAgoToTwoMonthsAgo = TrackerWindow.from(threeMonthsAgo).to(twoMonthsAgo);

        // When
        autoIndexTrackerDao.addWindow(docRefUuid, oneMonthAgoToNow);
        autoIndexTrackerDao.addWindow(docRefUuid, threeMonthsAgoToTwoMonthsAgo);
        autoIndexTrackerDao.clearWindows(docRefUuid);
        final AutoIndexTracker tracker = autoIndexTrackerDao.get(docRefUuid);

        // Then
        assertEquals(0L, tracker.getWindows().size());
        assertEquals(AutoIndexTracker.forDocRef(docRefUuid), tracker);
    }
}
