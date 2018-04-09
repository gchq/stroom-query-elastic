package stroom.autoindex.tracker;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class TrackerInverterTest {

    @Test
    public void testSingleInvertAtEnd() {
        // Given
        final String docRefUuid = UUID.randomUUID().toString();
        final TrackerWindow timelineBounds = TrackerWindow.from(20L).to(200L);
        final AutoIndexTracker tracker = AutoIndexTracker
                .forDocRef(docRefUuid)
                .withBounds(timelineBounds)
                .withWindow(TrackerWindow.from(180L).to(200L));

        // When
        final AutoIndexTracker inverted = TrackerInverter.withTracker(tracker).invert();

        // Then
        assertEquals(docRefUuid, inverted.getDocRefUuid());
        assertEquals(timelineBounds, inverted.getTimelineBounds());
        assertEquals(
                Collections.singletonList(TrackerWindow.from(20L).to(180L)),
                inverted.getWindows());
    }
    @Test
    public void testSingleInvertAtStart() {
        // Given
        final String docRefUuid = UUID.randomUUID().toString();
        final TrackerWindow timelineBounds = TrackerWindow.from(20L).to(200L);
        final AutoIndexTracker tracker = AutoIndexTracker
                .forDocRef(docRefUuid)
                .withBounds(timelineBounds)
                .withWindow(TrackerWindow.from(20L).to(40L));

        // When
        final AutoIndexTracker inverted = TrackerInverter.withTracker(tracker).invert();

        // Then
        assertEquals(docRefUuid, inverted.getDocRefUuid());
        assertEquals(timelineBounds, inverted.getTimelineBounds());
        assertEquals(
                Collections.singletonList(TrackerWindow.from(40L).to(200L)),
                inverted.getWindows());
    }

    @Test
    public void testMultipleInvertWithEnd() {
        // Given
        final String docRefUuid = UUID.randomUUID().toString();
        final TrackerWindow timelineBounds = TrackerWindow.from(20L).to(200L);
        final AutoIndexTracker tracker = AutoIndexTracker
                .forDocRef(docRefUuid)
                .withBounds(timelineBounds)
                .withWindow(TrackerWindow.from(120L).to(140L))
                .withWindow(TrackerWindow.from(180L).to(200L));

        // When
        final AutoIndexTracker inverted = TrackerInverter.withTracker(tracker).invert();

        // Then
        assertEquals(docRefUuid, inverted.getDocRefUuid());
        assertEquals(timelineBounds, inverted.getTimelineBounds());
        assertEquals(
                Arrays.asList(
                        TrackerWindow.from(20L).to(120L),
                        TrackerWindow.from(140L).to(180L)
                ),
                inverted.getWindows());

    }

    @Test
    public void testMultipleInvertWithStart() {
        // Given
        final String docRefUuid = UUID.randomUUID().toString();
        final TrackerWindow timelineBounds = TrackerWindow.from(20L).to(200L);
        final AutoIndexTracker tracker = AutoIndexTracker
                .forDocRef(docRefUuid)
                .withBounds(timelineBounds)
                .withWindow(TrackerWindow.from(20L).to(100L))
                .withWindow(TrackerWindow.from(140L).to(160L));

        // When
        final AutoIndexTracker inverted = TrackerInverter.withTracker(tracker).invert();

        // Then
        assertEquals(docRefUuid, inverted.getDocRefUuid());
        assertEquals(timelineBounds, inverted.getTimelineBounds());
        assertEquals(
                Arrays.asList(
                        TrackerWindow.from(100L).to(140L),
                        TrackerWindow.from(160L).to(200L)
                ),
                inverted.getWindows());

    }

    @Test
    public void testMultipleInvertWithStartAndEnd() {
        // Given
        final String docRefUuid = UUID.randomUUID().toString();
        final TrackerWindow timelineBounds = TrackerWindow.from(20L).to(200L);
        final AutoIndexTracker tracker = AutoIndexTracker
                .forDocRef(docRefUuid)
                .withBounds(timelineBounds)
                .withWindow(TrackerWindow.from(20L).to(100L))
                .withWindow(TrackerWindow.from(140L).to(160L))
                .withWindow(TrackerWindow.from(190L).to(2000L));

        // When
        final AutoIndexTracker inverted = TrackerInverter.withTracker(tracker).invert();

        // Then
        assertEquals(docRefUuid, inverted.getDocRefUuid());
        assertEquals(timelineBounds, inverted.getTimelineBounds());
        assertEquals(
                Arrays.asList(
                        TrackerWindow.from(100L).to(140L),
                        TrackerWindow.from(160L).to(190L)
                ),
                inverted.getWindows());

    }
}
