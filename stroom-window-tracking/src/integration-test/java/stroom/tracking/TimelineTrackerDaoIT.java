package stroom.tracking;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.name.Names;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TimelineTrackerDaoIT {

    /**
     * Create our own Index Tracker DAO for direct testing
     */
    private static TimelineTrackerDao<Configuration> timelineTrackerDao;

    @BeforeClass
    public static void beforeClass() {
        final Injector injector = Guice.createInjector(new AbstractModule() {
            @Override
            protected void configure() {
                bind(DSLContext.class).toInstance(TestDSLContext.createTestJooq());
            }
        });

        timelineTrackerDao = injector.getInstance(TimelineTrackerDaoJooqImpl.class);
    }

    @Test
    public void testGetEmptyTracker() {
        // Given
        final String docRefUuid = UUID.randomUUID().toString();

        // When
        final TimelineTracker tracker = timelineTrackerDao
                .transactionResult((d, c) -> d.get(c, docRefUuid));

        // Then
        assertNotNull(tracker);
        assertEquals(docRefUuid, tracker.getDocRefUuid());
    }

    @Test
    public void testAddNonContiguousWindows() {
        // Given
        final String docRefUuid = UUID.randomUUID().toString();
        final Long now = 20L;
        final Long oneMonthAgo = now - 5;
        final Long twoMonthsAgo = oneMonthAgo - 5;
        final Long threeMonthsAgo = twoMonthsAgo - 5;

        // Two non-contiguous windows
        final TrackerWindow oneMonthAgoToNow = TrackerWindow.from(oneMonthAgo).to(now);
        final TrackerWindow threeMonthsAgoToTwoMonthsAgo = TrackerWindow.from(threeMonthsAgo).to(twoMonthsAgo);

        // When
        final TimelineTracker tracker1 = timelineTrackerDao.transactionResult((d, c) -> {
            d.insertTracker(c, docRefUuid, oneMonthAgoToNow);
            return d.get(c, docRefUuid);
        });
        final TimelineTracker tracker2 = timelineTrackerDao.transactionResult((d, c) -> {
            d.insertTracker(c, docRefUuid, threeMonthsAgoToTwoMonthsAgo);
            return d.get(c, docRefUuid);
        });
        final TimelineTracker tracker3 = timelineTrackerDao.transactionResult((d, c) -> d.get(c, docRefUuid));

        // Then
        assertEquals(Collections.singletonList(oneMonthAgoToNow),
                tracker1.getWindows());
        assertEquals(Arrays.asList(threeMonthsAgoToTwoMonthsAgo, oneMonthAgoToNow),
                tracker2.getWindows());

        assertEquals(tracker2, tracker3);
    }

    @Test
    public void testAddContiguousWindows() {
        // Given
        final String docRefUuid = UUID.randomUUID().toString();
        final Long now = 35L;
        final Long oneMonthAgo = now - 10;
        final Long twoMonthsAgo = oneMonthAgo - 10;

        // Two windows that should join up
        final TrackerWindow oneMonthAgoToNow = TrackerWindow.from(oneMonthAgo).to(now);
        final TrackerWindow twoMonthsAgoToOneMonthAgo = TrackerWindow.from(twoMonthsAgo).to(oneMonthAgo);

        // When
        final TimelineTracker tracker1 = timelineTrackerDao.transactionResult((d, c) -> {
            d.insertTracker(c, docRefUuid, oneMonthAgoToNow);
            return d.get(c, docRefUuid);
        });
        final TimelineTracker tracker2 = timelineTrackerDao.transactionResult((d, c) -> {
            d.insertTracker(c, docRefUuid, twoMonthsAgoToOneMonthAgo);
            return d.get(c, docRefUuid);
        });
        final TimelineTracker tracker = timelineTrackerDao.transactionResult((d, c) -> d.get(c, docRefUuid));

        // Then
        // Should up with one big window that covers it all
        assertEquals(
                Arrays.asList(
                        TrackerWindow.from(twoMonthsAgo).to(oneMonthAgo),
                        TrackerWindow.from(oneMonthAgo).to(now)
                ),
                tracker.getWindows());
    }

    @Test
    public void testDeleteWindow() {
        // Given
        final String docRefUuid = UUID.randomUUID().toString();
        final Long now = 45L;
        final Long oneMonthAgo = now - 10;
        final Long twoMonthsAgo = oneMonthAgo - 10;
        final Long threeMonthsAgo = twoMonthsAgo - 10;

        final TrackerWindow oneMonthAgoToNow = TrackerWindow.from(oneMonthAgo).to(now);
        final TrackerWindow threeMonthsAgoToTwoMonthsAgo = TrackerWindow.from(threeMonthsAgo).to(twoMonthsAgo);

        // When
        timelineTrackerDao.transaction((d, c) ->
                d.insertTracker(c, docRefUuid, oneMonthAgoToNow)
        );
        timelineTrackerDao.transaction((d, c) ->
                d.insertTracker(c, docRefUuid, threeMonthsAgoToTwoMonthsAgo)
        );
        timelineTrackerDao.transaction((d, c) -> {
            d.deleteTracker(c, docRefUuid, oneMonthAgoToNow);
            d.deleteTracker(c, docRefUuid, threeMonthsAgoToTwoMonthsAgo);
        });
        final TimelineTracker tracker = timelineTrackerDao.transactionResult((d, c) -> d.get(c, docRefUuid));

        // Then
        assertEquals(0L, tracker.getWindows().size());
        assertEquals(TimelineTracker.forDocRef(docRefUuid), tracker);
    }
}
