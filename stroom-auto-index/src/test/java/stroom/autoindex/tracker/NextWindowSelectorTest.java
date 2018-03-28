package stroom.autoindex.tracker;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class NextWindowSelectorTest {

    @Test
    public void testFromEmpty() {
        // Given
        final TrackerWindow bounds = TrackerWindow.from(0L).to(6089L);

        // When
        final TrackerWindow nextWindow = NextWindowSelector.withBounds(bounds)
                .windowSize(30L)
                .suggestNextWindow();

        // Then
        assertEquals(TrackerWindow
                .from(6030L)
                .to(6060L), nextWindow);
    }

    @Test
    public void testOneWithSingleExisting() {
        // Given
        final TrackerWindow bounds = TrackerWindow.from(0L).to(4056L);
        final TrackerWindow existingWindow = TrackerWindow
                .from(4020L)
                .to(4040L);

        // When
        final TrackerWindow nextWindow = NextWindowSelector.withBounds(bounds)
                .windowSize(20)
                .existingWindows(existingWindow)
                .suggestNextWindow();

        // Then
        assertEquals(TrackerWindow
                .from(4000L)
                .to(4020L), nextWindow);
    }

    @Test
    public void testWithAnAwkwardGap() {
        // Given
        final TrackerWindow bounds = TrackerWindow.from(0L).to(23L);
        final List<TrackerWindow> existingWindows = Arrays.asList(
                TrackerWindow.from(17L).to(22L),
                TrackerWindow.from(13L).to(15L),
                TrackerWindow.from(7L).to(11L)
        );

        // When
        final List<TrackerWindow> windows = NextWindowSelector.withBounds(bounds)
                .windowSize(4)
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
        final TrackerWindow bounds = TrackerWindow.from(0L).to(65L);
        final List<TrackerWindow> existingWindows = Arrays.asList(
                TrackerWindow.from(50L).to(60L),
                TrackerWindow.from(0L).to(10L)
        );

        // When
        final List<TrackerWindow> windows = NextWindowSelector.withBounds(bounds)
                .windowSize(10)
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
}
