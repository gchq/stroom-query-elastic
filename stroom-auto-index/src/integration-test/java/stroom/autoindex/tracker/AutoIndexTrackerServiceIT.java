package stroom.autoindex.tracker;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import org.jooq.DSLContext;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.autoindex.AbstractAutoIndexIntegrationTest;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class AutoIndexTrackerServiceIT extends AbstractAutoIndexIntegrationTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(AutoIndexTrackerServiceIT.class);

    /**
     * Create our own Index Tracker Services for direct testing
     */
    private static List<AutoIndexTrackerService> autoIndexTrackerServices;

    @BeforeClass
    public static void beforeClass() {
        autoIndexTrackerServices =
                Stream.of(AutoIndexTrackerDaoJooqImpl.class, AutoIndexTrackerDaoTestImpl.class)
                .map(c -> Guice.createInjector(new AbstractModule() {
                    @Override
                    protected void configure() {
                        bind(AutoIndexTrackerService.class).to(AutoIndexTrackerServiceImpl.class);
                        bind(AutoIndexTrackerDao.class).to(c);
                        bind(DSLContext.class).toInstance(initialiseJooqDbRule.withDatabase());
                    }
                }))
        .map(i -> i.getInstance(AutoIndexTrackerService.class))
        .collect(Collectors.toList());
    }

    @Test
    public void testGetEmptyTracker() {
        autoIndexTrackerServices.forEach(autoIndexTrackerService -> {
            LOGGER.debug("Using Service {}", autoIndexTrackerService);

            // Given
            final String docRefUuid = UUID.randomUUID().toString();

            // When
            final AutoIndexTracker tracker = autoIndexTrackerService.get(docRefUuid);

            // Then
            assertNotNull(tracker);
            assertEquals(docRefUuid, tracker.getDocRefUuid());
        });
    }

    @Test
    public void testAddNonContiguousWindows() {
        autoIndexTrackerServices.forEach(autoIndexTrackerService -> {
            LOGGER.debug("Using Service {}", autoIndexTrackerService);

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
            final AutoIndexTracker tracker1 = autoIndexTrackerService.addWindow(docRefUuid, oneMonthAgoToNow);
            final AutoIndexTracker tracker2 = autoIndexTrackerService.addWindow(docRefUuid, threeMonthsAgoToTwoMonthsAgo);
            final AutoIndexTracker tracker3 = autoIndexTrackerService.get(docRefUuid);

            // Then
            assertEquals(Collections.singletonList(oneMonthAgoToNow),
                    tracker1.getWindows());
            assertEquals(Arrays.asList(threeMonthsAgoToTwoMonthsAgo, oneMonthAgoToNow),
                    tracker2.getWindows());

            assertEquals(tracker2, tracker3);
        });
    }

    @Test
    public void testAddContiguousWindows() {
        autoIndexTrackerServices.forEach(autoIndexTrackerService -> {
            LOGGER.debug("Using Service {}", autoIndexTrackerService);

            // Given
            final String docRefUuid = UUID.randomUUID().toString();
            final Long now = 35L;
            final Long oneMonthAgo = now - 10;
            final Long twoMonthsAgo = oneMonthAgo - 10;

            // Two windows that should join up
            final TrackerWindow oneMonthAgoToNow = TrackerWindow.from(oneMonthAgo).to(now);
            final TrackerWindow twoMonthsAgoToOneMonthAgo = TrackerWindow.from(twoMonthsAgo).to(oneMonthAgo);

            // When
            autoIndexTrackerService.addWindow(docRefUuid, oneMonthAgoToNow);
            autoIndexTrackerService.addWindow(docRefUuid, twoMonthsAgoToOneMonthAgo);
            final AutoIndexTracker tracker = autoIndexTrackerService.get(docRefUuid);

            // Then
            // Should up with one big window that covers it all
            assertEquals(Collections.singletonList(TrackerWindow.from(twoMonthsAgo).to(now)),
                    tracker.getWindows());
        });
    }

    @Test
    public void testAddBelowBounds() {
        // Given
        final String docRefUuid = UUID.randomUUID().toString();
        final Long now = 56L;
        final Long oneWeekAgo = now - 10;
        final Long twoWeeksAgo = oneWeekAgo - 10;

    }

    @Test
    public void testClearWindow() {
        autoIndexTrackerServices.forEach(autoIndexTrackerService -> {
            LOGGER.debug("Using Service {}", autoIndexTrackerService);

            // Given
            final String docRefUuid = UUID.randomUUID().toString();
            final Long now = 45L;
            final Long oneMonthAgo = now - 10;
            final Long twoMonthsAgo = oneMonthAgo - 10;
            final Long threeMonthsAgo = twoMonthsAgo - 10;

            // Two non-contiguous windows
            final TrackerWindow oneMonthAgoToNow = TrackerWindow.from(oneMonthAgo).to(now);
            final TrackerWindow threeMonthsAgoToTwoMonthsAgo = TrackerWindow.from(threeMonthsAgo).to(twoMonthsAgo);

            // When
            autoIndexTrackerService.addWindow(docRefUuid, oneMonthAgoToNow);
            autoIndexTrackerService.addWindow(docRefUuid, threeMonthsAgoToTwoMonthsAgo);
            autoIndexTrackerService.clearWindows(docRefUuid);
            final AutoIndexTracker tracker = autoIndexTrackerService.get(docRefUuid);

            // Then
            assertEquals(0L, tracker.getWindows().size());
            assertEquals(
                    AutoIndexTracker.forDocRef(docRefUuid)
                            .withBounds(TrackerWindow.from(threeMonthsAgo).to(now)),
                    tracker);
        });
    }
}
