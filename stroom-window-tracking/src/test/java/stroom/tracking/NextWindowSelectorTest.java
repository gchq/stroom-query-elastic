package stroom.tracking;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class NextWindowSelectorTest {

    @Test
    public void testFromEmpty() {
        // Given
        final Long windowSize = 30L;
        final TrackerWindow bounds = TrackerWindow.from(0L).to(6089L);

        // When
        final Optional<TrackerWindow> nextWindow = NextWindowSelector.withBounds(bounds)
                .windowSize(windowSize)
                .suggestNextWindow();

        // Then
        assertTrue(nextWindow.isPresent());
        assertEquals(TrackerWindow
                .from(6030L)
                .to(6060L), nextWindow.get());
    }

    @Test
    public void testOneWithSingleExisting() {
        // Given
        final Long windowSize = 20L;
        final TrackerWindow bounds = TrackerWindow.from(0L).to(4056L);
        final TrackerWindow existingWindow = TrackerWindow
                .from(4020L)
                .to(4040L);

        // When
        final Optional<TrackerWindow> nextWindow = NextWindowSelector.withBounds(bounds)
                .windowSize(windowSize)
                .existingWindows(existingWindow)
                .suggestNextWindow();

        // Then
        assertTrue(nextWindow.isPresent());
        assertEquals(TrackerWindow
                .from(4000L)
                .to(4020L), nextWindow.get());
    }

    @Test
    public void testWithAnAwkwardGap() {
        // Given
        final Long windowSize = 4L;
        final TrackerWindow bounds = TrackerWindow.from(0L).to(23L);
        final List<TrackerWindow> existingWindows = Arrays.asList(
                TrackerWindow.from(17L).to(22L),
                TrackerWindow.from(13L).to(15L),
                TrackerWindow.from(7L).to(11L)
        );

        // When
        final List<TrackerWindow> windows = NextWindowSelector.withBounds(bounds)
                .windowSize(windowSize)
                .existingWindows(existingWindows)
                .suggestNextWindows(6)
                .collect(Collectors.toList());

        // Then
        assertEquals(Arrays.asList(
                TrackerWindow
                        .from(16L)
                        .to(17L),
                TrackerWindow
                        .from(15L)
                        .to(16L),
                TrackerWindow
                        .from(12L)
                        .to(13L),
                TrackerWindow
                        .from(11L)
                        .to(12L),
                TrackerWindow
                        .from(4L)
                        .to(7L),
                TrackerWindow
                        .from(0L)
                        .to(4L)
                ),
                windows);
    }

    @Test
    public void testWithOneLargeGap() {
        // Given
        final Long windowSize = 10L;
        final TrackerWindow bounds = TrackerWindow.from(0L).to(65L);
        final List<TrackerWindow> existingWindows = Arrays.asList(
                TrackerWindow.from(50L).to(60L),
                TrackerWindow.from(0L).to(10L)
        );

        // When
        final List<TrackerWindow> windows = NextWindowSelector.withBounds(bounds)
                .windowSize(windowSize)
                .existingWindows(existingWindows)
                .suggestNextWindows(4)
                .collect(Collectors.toList());

        // Then
        assertEquals(Arrays.asList(
                TrackerWindow.from(40L).to(50L),
                TrackerWindow.from(30L).to(40L),
                TrackerWindow.from(20L).to(30L),
                TrackerWindow.from(10L).to(20L)
                ),
                windows);
    }

    @Test
    public void testExhaustTimelineBounds() {
        // Given
        final Long windowSize = 10L;
        final TrackerWindow bounds = TrackerWindow.from(100L).to(200L);
        final TrackerWindow existingWindow = TrackerWindow.from(120L).to(200L);

        // When
        final List<TrackerWindow> windows = NextWindowSelector.withBounds(bounds)
                .windowSize(windowSize)
                .existingWindows(existingWindow)
                .suggestNextWindows(5)
                .collect(Collectors.toList());

        // Then
        assertEquals(Arrays.asList(
                TrackerWindow.from(110L).to(120L),
                TrackerWindow.from(100L).to(110L)
                ),
                windows);

    }
}
