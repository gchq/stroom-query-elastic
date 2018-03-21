package stroom.autoindex.tracker;

import org.junit.Test;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class NextWindowSelectorTest {

    @Test
    public void testFromEmptyByHours() {
        // Given
        final LocalDateTime now = LocalDateTime.of(2017, 3, 28, 16, 12);
        final TemporalAmount windowSize = Duration.of(1, ChronoUnit.HOURS);

        // When
        final TrackerWindow nextWindow = NextWindowSelector.fromNow(now)
                .windowSize(windowSize)
                .suggestNextWindow();

        // Then
        assertEquals(TrackerWindow
                .from(LocalDateTime.of(2017, 3, 28, 15, 0))
                .to(LocalDateTime.of(2017, 3, 28, 16, 0)), nextWindow);
    }

    @Test
    public void testWithOneFilledInByDays() {
        // Given
        final LocalDateTime now = LocalDateTime.of(2017, 3, 28, 16, 12);
        final TemporalAmount windowSize = Duration.of(1, ChronoUnit.DAYS);
        final TrackerWindow existingWindow = TrackerWindow
                .from(LocalDateTime.of(2017, 3, 27, 0, 0))
                .to(LocalDateTime.of(2017, 3, 28, 0, 0));

        // When
        final TrackerWindow nextWindow = NextWindowSelector.fromNow(now)
                .windowSize(windowSize)
                .existingWindows(existingWindow)
                .suggestNextWindow();

        // Then
        assertEquals(TrackerWindow
                .from(LocalDateTime.of(2017, 3, 26, 0, 0))
                .to(LocalDateTime.of(2017, 3, 27, 0, 0)), nextWindow);
    }

    @Test
    public void testWithAnAwkwardGapIn30Minutes() {
        // Given
        final LocalDateTime now = LocalDateTime.of(2015, 11, 5, 16, 23);
        final TemporalAmount windowSize = Duration.of(30, ChronoUnit.MINUTES);
        final List<TrackerWindow> existingWindows = Arrays.asList(
                TrackerWindow.from(LocalDateTime.of(2015, 11, 5, 15, 30))
                        .to(LocalDateTime.of(2015, 11, 5, 16, 0)),
                TrackerWindow.from(LocalDateTime.of(2015, 11, 5, 14, 46))
                        .to(LocalDateTime.of(2015, 11, 5, 15, 18)),
                TrackerWindow.from(LocalDateTime.of(2015, 11, 5, 13, 0))
                        .to(LocalDateTime.of(2015, 11, 5, 14, 0))
        );

        // When
        final List<TrackerWindow> windows = NextWindowSelector.fromNow(now)
                .windowSize(windowSize)
                .existingWindows(existingWindows)
                .suggestNextWindows(5)
                .collect(Collectors.toList());

        // Then
        assertEquals(Arrays.asList(
                TrackerWindow
                        .from(LocalDateTime.of(2015, 11, 5, 15, 18))
                        .to(LocalDateTime.of(2015, 11, 5, 15, 30)),
                TrackerWindow
                        .from(LocalDateTime.of(2015, 11, 5, 14, 30))
                        .to(LocalDateTime.of(2015, 11, 5, 14, 46)),
                TrackerWindow
                        .from(LocalDateTime.of(2015, 11, 5, 14, 0))
                        .to(LocalDateTime.of(2015, 11, 5, 14, 30)),
                TrackerWindow
                        .from(LocalDateTime.of(2015, 11, 5, 12, 30))
                        .to(LocalDateTime.of(2015, 11, 5, 13, 0)),
                TrackerWindow
                        .from(LocalDateTime.of(2015, 11, 5, 12, 0))
                        .to(LocalDateTime.of(2015, 11, 5, 12, 30))
                ),
                windows);
    }

    @Test
    public void testWithOneLargeGapByFourHours() {
        // Given
        final LocalDateTime now = LocalDateTime.of(2018, 8, 17, 14, 0);
        final TemporalAmount windowSize = Duration.of(4, ChronoUnit.HOURS);
        final List<TrackerWindow> existingWindows = Arrays.asList(
                TrackerWindow.from(LocalDateTime.of(2018, 8, 16, 4, 0))
                        .to(LocalDateTime.of(2018, 8, 16, 12, 0)),
                TrackerWindow.from(LocalDateTime.of(2018, 8, 17, 0, 0))
                        .to(LocalDateTime.of(2018, 8, 17, 12, 0))
        );

        // When
        final List<TrackerWindow> windows = NextWindowSelector.fromNow(now)
                .windowSize(windowSize)
                .existingWindows(existingWindows)
                .suggestNextWindows(5)
                .collect(Collectors.toList());

        // Then
        assertEquals(Arrays.asList(
                TrackerWindow
                        .from(LocalDateTime.of(2018, 8, 16, 20, 0))
                        .to(LocalDateTime.of(2018, 8, 17, 0, 0)),
                TrackerWindow
                        .from(LocalDateTime.of(2018, 8, 16, 16, 0))
                        .to(LocalDateTime.of(2018, 8, 16, 20, 0)),
                TrackerWindow
                        .from(LocalDateTime.of(2018, 8, 16, 12, 0))
                        .to(LocalDateTime.of(2018, 8, 16, 16, 0)),
                TrackerWindow
                        .from(LocalDateTime.of(2018, 8, 16, 0, 0))
                        .to(LocalDateTime.of(2018, 8, 16, 4, 0)),
                TrackerWindow
                        .from(LocalDateTime.of(2018, 8, 15, 20, 0))
                        .to(LocalDateTime.of(2018, 8, 16, 0, 0))
                ),
                windows);
    }
}
