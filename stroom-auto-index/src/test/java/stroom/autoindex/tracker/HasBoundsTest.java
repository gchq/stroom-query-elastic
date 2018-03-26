package stroom.autoindex.tracker;

import org.junit.Test;

import java.time.LocalDateTime;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HasBoundsTest {

    @Test
    public void testInteger() {
        // Given
        final IntegerWindow window = IntegerWindow.from(5).to(8);

        // When
        final boolean lower = window.isInside(3, Integer::compare);
        final boolean onBottomEdge = window.isInside(5, Integer::compare);
        final boolean within = window.isInside(6, Integer::compare);
        final boolean onTopEdge = window.isInside(8, Integer::compare);
        final boolean higher = window.isInside(10, Integer::compare);

        // Then
        assertFalse(lower);
        assertTrue(onBottomEdge);
        assertTrue(within);
        assertTrue(onTopEdge);
        assertFalse(higher);
    }

    @Test
    public void testIntegerInsideState() {
        // Given
        final IntegerWindow window = IntegerWindow.from(5).to(8);

        // When
        final HasBounds.Inside lower = window.checkInside(3, Integer::compare);
        final HasBounds.Inside onBottomEdge = window.checkInside(5, Integer::compare);
        final HasBounds.Inside within = window.checkInside(6, Integer::compare);
        final HasBounds.Inside onTopEdge = window.checkInside(8, Integer::compare);
        final HasBounds.Inside higher = window.checkInside(10, Integer::compare);

        // Then
        assertEquals(HasBounds.Inside.LOWER, lower);
        assertEquals(HasBounds.Inside.BOTTOM_EDGE, onBottomEdge);
        assertEquals(HasBounds.Inside.WITHIN, within);
        assertEquals(HasBounds.Inside.TOP_EDGE, onTopEdge);
        assertEquals(HasBounds.Inside.HIGHER, higher);
    }

    @Test
    public void testDateTime() {
        // Given
        final TrackerWindow window = TrackerWindow
                .from(LocalDateTime.of(2017, 3, 20, 16, 12))
                .to(LocalDateTime.of(2017, 3, 24, 16, 12));

        // When
        final boolean lower = window.isInside(LocalDateTime.of(2017, 3, 15, 13, 0), LocalDateTime::compareTo);
        final boolean onBottomEdge = window.isInside(LocalDateTime.of(2017, 3, 20, 16, 12), LocalDateTime::compareTo);
        final boolean within = window.isInside(LocalDateTime.of(2017, 3, 22, 13, 0), LocalDateTime::compareTo);
        final boolean onTopEdge = window.isInside(LocalDateTime.of(2017, 3, 24, 16, 12), LocalDateTime::compareTo);
        final boolean higher = window.isInside(LocalDateTime.of(2017, 4, 15, 13, 0), LocalDateTime::compareTo);

        // Then
        assertFalse(lower);
        assertTrue(onBottomEdge);
        assertTrue(within);
        assertTrue(onTopEdge);
        assertFalse(higher);
    }

    @Test
    public void testDateTimeInsideState() {
        // Given
        final TrackerWindow window = TrackerWindow
                .from(LocalDateTime.of(2017, 3, 20, 16, 12))
                .to(LocalDateTime.of(2017, 3, 24, 16, 12));

        // When
        final HasBounds.Inside lower = window.checkInside(LocalDateTime.of(2017, 3, 15, 13, 0), LocalDateTime::compareTo);
        final HasBounds.Inside onBottomEdge = window.checkInside(LocalDateTime.of(2017, 3, 20, 16, 12), LocalDateTime::compareTo);
        final HasBounds.Inside within = window.checkInside(LocalDateTime.of(2017, 3, 22, 13, 0), LocalDateTime::compareTo);
        final HasBounds.Inside onTopEdge = window.checkInside(LocalDateTime.of(2017, 3, 24, 16, 12), LocalDateTime::compareTo);
        final HasBounds.Inside higher = window.checkInside(LocalDateTime.of(2017, 4, 15, 13, 0), LocalDateTime::compareTo);

        // Then
        assertEquals(HasBounds.Inside.LOWER, lower);
        assertEquals(HasBounds.Inside.BOTTOM_EDGE, onBottomEdge);
        assertEquals(HasBounds.Inside.WITHIN, within);
        assertEquals(HasBounds.Inside.TOP_EDGE, onTopEdge);
        assertEquals(HasBounds.Inside.HIGHER, higher);
    }
}
