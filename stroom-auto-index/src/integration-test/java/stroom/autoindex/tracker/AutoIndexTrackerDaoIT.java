package stroom.autoindex.tracker;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.jooq.DSLContext;
import org.junit.BeforeClass;
import org.junit.Test;
import stroom.autoindex.AbstractAutoIndexIntegrationTest;
import stroom.autoindex.TimeUtils;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class AutoIndexTrackerDaoIT extends AbstractAutoIndexIntegrationTest {

    /**
     * Create our own Index Tracker DAO for direct testing
     */
    private static AutoIndexTrackerDao autoIndexTrackerDao;

    @BeforeClass
    public static void beforeClass() {
        final Injector injector = Guice.createInjector(new AbstractModule() {
            @Override
            protected void configure() {
                bind(DSLContext.class).toInstance(initialiseJooqDbRule.withDatabase());
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
