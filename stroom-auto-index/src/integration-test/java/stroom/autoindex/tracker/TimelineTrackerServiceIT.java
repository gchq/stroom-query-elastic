package stroom.autoindex.tracker;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import org.jooq.DSLContext;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.autoindex.AbstractAutoIndexIntegrationTest;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TimelineTrackerServiceIT extends AbstractAutoIndexIntegrationTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(TimelineTrackerServiceIT.class);

    /**
     * Create our own Index Tracker Services for direct testing
     */
    private static List<TimelineTrackerService> timelineTrackerServices;

    @BeforeClass
    public static void beforeClass() {
        timelineTrackerServices =
                Stream.of(TimelineTrackerDaoJooqImpl.class, TimelineTrackerDaoTestImpl.class)
                .map(c -> Guice.createInjector(new AbstractModule() {
                    @Override
                    protected void configure() {
                        bind(TimelineTrackerService.class).to(TimelineTrackerServiceImpl.class);
                        bind(TimelineTrackerDao.class).to(c);
                        bind(DSLContext.class).toInstance(initialiseJooqDbRule.withDatabase());
                    }
                }))
        .map(i -> i.getInstance(TimelineTrackerService.class))
        .collect(Collectors.toList());
    }

    @Test
    public void testGetEmptyTracker() {
        timelineTrackerServices.forEach(autoIndexTrackerService -> {
            LOGGER.debug("Using Service {}", autoIndexTrackerService);

            // Given
            final String docRefUuid = UUID.randomUUID().toString();

            // When
            final TimelineTracker tracker = autoIndexTrackerService.get(docRefUuid);

            // Then
            assertEquals(TimelineTracker.forDocRef(docRefUuid), tracker);
        });
    }

    @Test
    public void testAddNonContiguousWindows() {
        timelineTrackerServices.forEach(autoIndexTrackerService -> {
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
            final TimelineTracker tracker1 = autoIndexTrackerService.addWindow(docRefUuid, oneMonthAgoToNow);
            final TimelineTracker tracker2 = autoIndexTrackerService.addWindow(docRefUuid, threeMonthsAgoToTwoMonthsAgo);
            final TimelineTracker tracker3 = autoIndexTrackerService.get(docRefUuid);

            // Then
            assertEquals(
                    TimelineTracker.forDocRef(docRefUuid)
                            .withBounds(oneMonthAgoToNow)
                            .withWindow(oneMonthAgoToNow),
                    tracker1);
            assertEquals(
                    TimelineTracker.forDocRef(docRefUuid)
                            .withBounds(TrackerWindow.from(threeMonthsAgo).to(now))
                            .withWindow(threeMonthsAgoToTwoMonthsAgo)
                            .withWindow(oneMonthAgoToNow),
                    tracker2);

            assertEquals(tracker2, tracker3);
        });
    }

    @Test
    public void testAddContiguousWindows() {
        timelineTrackerServices.forEach(autoIndexTrackerService -> {
            // Given
            final String docRefUuid = UUID.randomUUID().toString();
            final Long now = 56L;
            final Long oneWeekAgo = now - 10;
            final Long twoWeeksAgo = oneWeekAgo - 10;

            final TrackerWindow oneWeekAgoToNow = TrackerWindow.from(oneWeekAgo).to(now);
            final TrackerWindow twoWeeksAgoToOneWeekAgo = TrackerWindow.from(twoWeeksAgo).to(oneWeekAgo);

            // When
            autoIndexTrackerService.addWindow(docRefUuid, oneWeekAgoToNow);
            autoIndexTrackerService.addWindow(docRefUuid, twoWeeksAgoToOneWeekAgo);
            final TimelineTracker tracker = autoIndexTrackerService.get(docRefUuid);

            // Then
            assertEquals(
                    TimelineTracker.forDocRef(docRefUuid)
                            .withBounds(TrackerWindow.from(twoWeeksAgo).to(now))
                            .withWindow(TrackerWindow.from(twoWeeksAgo).to(now)),
                    tracker);
        });
    }

    @Test
    public void testClearWindow() {
        timelineTrackerServices.forEach(autoIndexTrackerService -> {
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
            final TimelineTracker tracker = autoIndexTrackerService.get(docRefUuid);

            // Then
            assertEquals(
                    TimelineTracker.forDocRef(docRefUuid)
                            .withBounds(TrackerWindow.from(threeMonthsAgo).to(now)),
                    tracker);
        });
    }
}
