package stroom.autoindex.tracker;

import org.junit.Test;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class WindowMergerTest {

    // Classes under test
    private final WindowMerger<Integer, IntegerWindow> integerMerger =
            WindowMerger.<Integer, IntegerWindow>withValueGenerator((from, to) -> IntegerWindow.from(from).to(to))
                    .comparator(Integer::compare)
                    .build();

    private final WindowMerger<LocalDateTime, TrackerWindow> dateTimeMerger =
            WindowMerger.<LocalDateTime, TrackerWindow>withValueGenerator((from, to) -> TrackerWindow.from(from).to(to))
                    .comparator(LocalDateTime::compareTo)
                    .build();

    @Test
    public void testOverlapStartInteger() {
        final Optional<WindowMerger.OverlapStyle> overlapStyle =
                integerMerger.determineOverlap(IntegerWindow.from(2).to(5), IntegerWindow.from(1).to(4));
        assertTrue(overlapStyle.isPresent());
        assertEquals(WindowMerger.OverlapStyle.OVERLAP_START, overlapStyle.get());
    }

    @Test
    public void testMergeOverlapStartInteger() {
        final Set<IntegerWindow> deletions = new HashSet<>();

        final Optional<IntegerWindow> toCreateOpt = integerMerger
                .merge(IntegerWindow.from(2).to(5))
                .with(IntegerWindow.from(1).to(4),
                        IntegerWindow.from(9).to(12))
                .deleteWith(deletions::add)
                .execute();

        assertTrue(toCreateOpt.isPresent());

        // Should detect overlap and move the end
        assertEquals(toCreateOpt.get(), IntegerWindow.from(1).to(5));

        // This window that overlapped should be deleted, to be replaced by the new one
        assertEquals(Collections.singleton(IntegerWindow.from(1).to(4)), deletions);
    }

    @Test
    public void testOverlapStartDateTime() {
        final Optional<WindowMerger.OverlapStyle> overlapStyle =
                dateTimeMerger.determineOverlap(
                        TrackerWindow.from(LocalDateTime.of(2017, 3, 20, 0, 0)).to(LocalDateTime.of(2017, 3, 29, 0, 0)),
                        TrackerWindow.from(LocalDateTime.of(2017, 3, 15, 0, 0)).to(LocalDateTime.of(2017, 3, 25, 0, 0)));
        assertTrue(overlapStyle.isPresent());
        assertEquals(WindowMerger.OverlapStyle.OVERLAP_START, overlapStyle.get());
    }

    @Test
    public void testMergeOverlapStartDateTime() {
        final Set<TrackerWindow> deletions = new HashSet<>();

        final Optional<TrackerWindow> toCreateOpt = dateTimeMerger
                .merge(TrackerWindow.from(LocalDateTime.of(2017, 3, 20, 0, 0)).to(LocalDateTime.of(2017, 3, 29, 0, 0)))
                .with(TrackerWindow.from(LocalDateTime.of(2016, 3, 15, 0, 0)).to(LocalDateTime.of(2016, 3, 25, 0, 0)),
                        TrackerWindow.from(LocalDateTime.of(2017, 3, 15, 0, 0)).to(LocalDateTime.of(2017, 3, 25, 0, 0)))
                .deleteWith(deletions::add)
                .execute();

        assertTrue(toCreateOpt.isPresent());

        // Should detect overlap and move the end
        assertEquals(TrackerWindow
                        .from(LocalDateTime.of(2017, 3, 15, 0, 0))
                        .to(LocalDateTime.of(2017, 3, 29, 0, 0)), toCreateOpt.get());

        // This window that overlapped should be deleted, to be replaced by the new one
        assertEquals(Collections.singleton(TrackerWindow
                        .from(LocalDateTime.of(2017, 3, 15, 0, 0))
                        .to(LocalDateTime.of(2017, 3, 25, 0, 0))),
                deletions);
    }

    @Test
    public void testOverlapEndInteger() {
        final Optional<WindowMerger.OverlapStyle> overlapStyle =
                integerMerger.determineOverlap(IntegerWindow.from(9).to(12), IntegerWindow.from(10).to(13));
        assertTrue(overlapStyle.isPresent());
        assertEquals(WindowMerger.OverlapStyle.OVERLAP_END, overlapStyle.get());
    }

    @Test
    public void testMergeOverlapEndInteger() {
        final Set<IntegerWindow> deletions = new HashSet<>();

        final Optional<IntegerWindow> toCreateOpt = integerMerger
                .merge(IntegerWindow.from(9).to(12))
                .with(IntegerWindow.from(2).to(5),
                        IntegerWindow.from(10).to(13))
                .deleteWith(deletions::add)
                .execute();

        assertTrue(toCreateOpt.isPresent());

        // Should detect overlap and move the start
        assertEquals(IntegerWindow.from(9).to(13), toCreateOpt.get());

        // This window that overlapped should be deleted, to be replaced by the new one
        assertEquals(Collections.singleton(IntegerWindow.from(10).to(13)), deletions);
    }

    @Test
    public void testOverlapEndDateTime() {
        final Optional<WindowMerger.OverlapStyle> overlapStyle =
                dateTimeMerger.determineOverlap(
                        TrackerWindow.from(LocalDateTime.of(2017, 3, 15, 0, 0)).to(LocalDateTime.of(2017, 3, 25, 0, 0)),
                        TrackerWindow.from(LocalDateTime.of(2017, 3, 20, 0, 0)).to(LocalDateTime.of(2017, 3, 29, 0, 0)));
        assertTrue(overlapStyle.isPresent());
        assertEquals(WindowMerger.OverlapStyle.OVERLAP_END, overlapStyle.get());
    }

    @Test
    public void testMergeOverlapEndDateTime() {
        final Set<TrackerWindow> deletions = new HashSet<>();

        final Optional<TrackerWindow> toCreateOpt = dateTimeMerger
                .merge(TrackerWindow.from(LocalDateTime.of(2017, 3, 15, 0, 0)).to(LocalDateTime.of(2017, 3, 25, 0, 0)))
                .with(TrackerWindow.from(LocalDateTime.of(2016, 3, 20, 0, 0)).to(LocalDateTime.of(2016, 3, 29, 0, 0)),
                        TrackerWindow.from(LocalDateTime.of(2017, 3, 20, 0, 0)).to(LocalDateTime.of(2017, 3, 29, 0, 0)))
                .deleteWith(deletions::add)
                .execute();

        assertTrue(toCreateOpt.isPresent());

        // Should detect overlap and move the start
        assertEquals(TrackerWindow
                        .from(LocalDateTime.of(2017, 3, 15, 0, 0))
                        .to(LocalDateTime.of(2017, 3, 29, 0, 0)),
                toCreateOpt.get());

        // This window that overlapped should be deleted, to be replaced by the new one
        assertEquals(Collections.singleton(TrackerWindow
                        .from(LocalDateTime.of(2017, 3, 20, 0, 0))
                        .to(LocalDateTime.of(2017, 3, 29, 0, 0))),
                deletions);
    }

    @Test
    public void testExistingSubsumedByNewInteger() {
        final Optional<WindowMerger.OverlapStyle> overlapStyle =
                integerMerger.determineOverlap(IntegerWindow.from(20).to(44), IntegerWindow.from(24).to(30));
        assertTrue(overlapStyle.isPresent());
        assertEquals(WindowMerger.OverlapStyle.EXISTING_SUBSUMED_BY_NEW, overlapStyle.get());
    }

    @Test
    public void testMergeExistingSubsumedByNewInteger() {
        final Set<IntegerWindow> deletions = new HashSet<>();

        final Optional<IntegerWindow> toCreateOpt = integerMerger
                .merge(IntegerWindow.from(20).to(44))
                .with(IntegerWindow.from(2).to(5),
                        IntegerWindow.from(24).to(30))
                .deleteWith(deletions::add)
                .execute();

        assertTrue(toCreateOpt.isPresent());

        // Should detect overlap and move the start
        assertEquals(IntegerWindow.from(20).to(44), toCreateOpt.get());

        // This window that overlapped should be deleted, to be replaced by the new one
        assertEquals(Collections.singleton(IntegerWindow.from(24).to(30)), deletions);
    }

    @Test
    public void testExistingSubsumedByNewDateTime() {
        final Optional<WindowMerger.OverlapStyle> overlapStyle =
                dateTimeMerger.determineOverlap(
                        TrackerWindow.from(LocalDateTime.of(2018, 10, 5, 0, 0)).to(LocalDateTime.of(2018, 12, 20, 0, 0)),
                        TrackerWindow.from(LocalDateTime.of(2018, 11, 20, 0, 0)).to(LocalDateTime.of(2018, 12, 13, 0, 0))
                );
        assertTrue(overlapStyle.isPresent());
        assertEquals(WindowMerger.OverlapStyle.EXISTING_SUBSUMED_BY_NEW, overlapStyle.get());
    }

    @Test
    public void testMergeExistingSubsumedByNewDateTime() {
        final Set<TrackerWindow> deletions = new HashSet<>();

        final Optional<TrackerWindow> toCreateOpt = dateTimeMerger
                .merge(TrackerWindow.from(LocalDateTime.of(2018, 10, 5, 0, 0)).to(LocalDateTime.of(2018, 12, 20, 0, 0)))
                .with(TrackerWindow.from(LocalDateTime.of(2017, 11, 20, 0, 0)).to(LocalDateTime.of(2017, 12, 13, 0, 0)),
                        TrackerWindow.from(LocalDateTime.of(2018, 11, 20, 0, 0)).to(LocalDateTime.of(2018, 12, 13, 0, 0)))
                .deleteWith(deletions::add)
                .execute();

        assertTrue(toCreateOpt.isPresent());

        // Should detect overlap and move the start
        assertEquals(TrackerWindow
                        .from(LocalDateTime.of(2018, 10, 5, 0, 0))
                        .to(LocalDateTime.of(2018, 12, 20, 0, 0)),
                toCreateOpt.get());

        // This window that overlapped should be deleted, to be replaced by the new one
        assertEquals(Collections.singleton(TrackerWindow
                        .from(LocalDateTime.of(2018, 11, 20, 0, 0))
                        .to(LocalDateTime.of(2018, 12, 13, 0, 0))),
                deletions);
    }

    @Test
    public void testNewSubsumedByExistingInteger() {
        final Optional<WindowMerger.OverlapStyle> overlapStyle =
                integerMerger.determineOverlap(IntegerWindow.from(24).to(30), IntegerWindow.from(20).to(44));
        assertTrue(overlapStyle.isPresent());
        assertEquals(WindowMerger.OverlapStyle.NEW_SUBSUMED_BY_EXISTING, overlapStyle.get());
    }

    @Test
    public void testMergeNewSubsumedByExistingInteger() {
        final Set<IntegerWindow> deletions = new HashSet<>();

        final Optional<IntegerWindow> toCreateOpt = integerMerger
                .merge(IntegerWindow.from(24).to(30))
                .with(IntegerWindow.from(2).to(5),
                        IntegerWindow.from(20).to(44))
                .deleteWith(deletions::add)
                .execute();

        assertFalse(toCreateOpt.isPresent());
        assertEquals(0, deletions.size());
    }

    @Test
    public void testNewSubsumedByExistingDateTime() {
        final Optional<WindowMerger.OverlapStyle> overlapStyle =
                dateTimeMerger.determineOverlap(
                        TrackerWindow.from(LocalDateTime.of(2018, 11, 20, 0, 0)).to(LocalDateTime.of(2018, 12, 13, 0, 0)),
                        TrackerWindow.from(LocalDateTime.of(2018, 10, 5, 0, 0)).to(LocalDateTime.of(2018, 12, 20, 0, 0)));
        assertTrue(overlapStyle.isPresent());
        assertEquals(WindowMerger.OverlapStyle.NEW_SUBSUMED_BY_EXISTING, overlapStyle.get());
    }

    @Test
    public void testMergeNewSubsumedByExistingDateTime() {
        final Set<TrackerWindow> deletions = new HashSet<>();

        final Optional<TrackerWindow> toCreateOpt = dateTimeMerger
                .merge(TrackerWindow.from(LocalDateTime.of(2018, 11, 20, 0, 0)).to(LocalDateTime.of(2018, 12, 13, 0, 0)))
                .with(TrackerWindow.from(LocalDateTime.of(2017, 10, 5, 0, 0)).to(LocalDateTime.of(2017, 12, 20, 0, 0)),
                        TrackerWindow.from(LocalDateTime.of(2018, 10, 5, 0, 0)).to(LocalDateTime.of(2018, 12, 20, 0, 0)))
                .deleteWith(deletions::add)
                .execute();

        assertFalse(toCreateOpt.isPresent());
        assertEquals(0, deletions.size());
    }

    @Test
    public void newIsNoOverlapAfterExistingInteger() {
        final Optional<WindowMerger.OverlapStyle> overlapStyle =
                integerMerger.determineOverlap(IntegerWindow.from(24).to(30), IntegerWindow.from(10).to(23));
        assertTrue(overlapStyle.isPresent());
        assertEquals(WindowMerger.OverlapStyle.NO_OVERLAP, overlapStyle.get());
    }

    @Test
    public void testMergeNewIsNoOverlapAfterExistingInteger() {
        final Set<IntegerWindow> deletions = new HashSet<>();

        final Optional<IntegerWindow> toCreateOpt = integerMerger
                .merge(IntegerWindow.from(24).to(30))
                .with(IntegerWindow.from(2).to(5),
                        IntegerWindow.from(10).to(23))
                .deleteWith(deletions::add)
                .execute();

        assertTrue(toCreateOpt.isPresent());

        // Should detect overlap and move the start
        assertEquals(IntegerWindow.from(24).to(30), toCreateOpt.get());

        // This window that overlapped should be deleted, to be replaced by the new one
        assertEquals(0, deletions.size());
    }

    @Test
    public void newIsNoOverlapAfterExistingDateTime() {
        final Optional<WindowMerger.OverlapStyle> overlapStyle =
                dateTimeMerger.determineOverlap(
                        TrackerWindow.from(LocalDateTime.of(2018, 2, 20, 0, 0)).to(LocalDateTime.of(2018, 3, 20, 0, 0)),
                        TrackerWindow.from(LocalDateTime.of(2018, 1, 10, 0, 0)).to(LocalDateTime.of(2018, 2, 10, 0, 0))
                );
        assertTrue(overlapStyle.isPresent());
        assertEquals(WindowMerger.OverlapStyle.NO_OVERLAP, overlapStyle.get());
    }

    @Test
    public void testMergeNewIsNoOverlapAfterExistingDateTime() {
        final Set<TrackerWindow> deletions = new HashSet<>();

        final Optional<TrackerWindow> toCreateOpt = dateTimeMerger
                .merge(TrackerWindow.from(LocalDateTime.of(2018, 2, 20, 0, 0)).to(LocalDateTime.of(2018, 3, 20, 0, 0)))
                .with(TrackerWindow.from(LocalDateTime.of(2015, 1, 10, 0, 0)).to(LocalDateTime.of(2017, 2, 10, 0, 0)),
                        TrackerWindow.from(LocalDateTime.of(2018, 1, 10, 0, 0)).to(LocalDateTime.of(2018, 2, 10, 0, 0)))
                .deleteWith(deletions::add)
                .execute();

        assertTrue(toCreateOpt.isPresent());

        // Should detect overlap and move the start
        assertEquals(TrackerWindow
                        .from(LocalDateTime.of(2018, 2, 20, 0, 0))
                        .to(LocalDateTime.of(2018, 3, 20, 0, 0)),
                toCreateOpt.get());

        // This window that overlapped should be deleted, to be replaced by the new one
        assertEquals(0, deletions.size());
    }

    @Test
    public void newIsNoOverlapBeforeExistingInteger() {
        final Optional<WindowMerger.OverlapStyle> overlapStyle =
                integerMerger.determineOverlap(IntegerWindow.from(10).to(13), IntegerWindow.from(20).to(44));
        assertTrue(overlapStyle.isPresent());
        assertEquals(WindowMerger.OverlapStyle.NO_OVERLAP, overlapStyle.get());
    }

    @Test
    public void testMergeNewIsNoOverlapBeforeExistingInteger() {
        final Set<IntegerWindow> deletions = new HashSet<>();

        final Optional<IntegerWindow> toCreateOpt = integerMerger
                .merge(IntegerWindow.from(10).to(13))
                .with(IntegerWindow.from(2).to(5),
                        IntegerWindow.from(20).to(44))
                .deleteWith(deletions::add)
                .execute();

        assertTrue(toCreateOpt.isPresent());

        // Should detect overlap and move the start
        assertEquals(IntegerWindow.from(10).to(13), toCreateOpt.get());

        // This window that overlapped should be deleted, to be replaced by the new one
        assertEquals(0, deletions.size());
    }

    @Test
    public void newIsNoOverlapBeforeExistingDateTime() {
        final Optional<WindowMerger.OverlapStyle> overlapStyle =
                dateTimeMerger.determineOverlap(
                        TrackerWindow.from(LocalDateTime.of(2016, 3, 1, 0, 0)).to(LocalDateTime.of(2017, 3, 1, 0, 0)),
                        TrackerWindow.from(LocalDateTime.of(2017, 3, 2, 0, 0)).to(LocalDateTime.of(2018, 3, 1, 0, 0)));
        assertTrue(overlapStyle.isPresent());
        assertEquals(WindowMerger.OverlapStyle.NO_OVERLAP, overlapStyle.get());
    }

    @Test
    public void testMergeNewIsNoOverlapBeforeExistingDateTime() {
        final Set<TrackerWindow> deletions = new HashSet<>();

        final Optional<TrackerWindow> toCreateOpt = dateTimeMerger
                .merge(TrackerWindow.from(LocalDateTime.of(2016, 3, 1, 0, 0)).to(LocalDateTime.of(2017, 3, 1, 0, 0)))
                .with(TrackerWindow.from(LocalDateTime.of(2015, 3, 2, 0, 0)).to(LocalDateTime.of(2015, 4, 1, 0, 0)),
                        TrackerWindow.from(LocalDateTime.of(2017, 3, 2, 0, 0)).to(LocalDateTime.of(2018, 4, 1, 0, 0)))
                .deleteWith(deletions::add)
                .execute();

        assertTrue(toCreateOpt.isPresent());

        // Should detect overlap and move the start
        assertEquals(TrackerWindow
                        .from(LocalDateTime.of(2016, 3, 1, 0, 0))
                        .to(LocalDateTime.of(2017, 3, 1, 0, 0)),
                toCreateOpt.get());

        // This window that overlapped should be deleted, to be replaced by the new one
        assertEquals(0, deletions.size());
    }

    /// Now some cases were windows should join up
    @Test
    public void testMergeNewDirectlyAfterExisting() {
        final Set<TrackerWindow> deletions = new HashSet<>();

        // New window is March 2018, existing windows are Jan and Feb
        final Optional<TrackerWindow> toCreateOpt = dateTimeMerger
                .merge(TrackerWindow.from(LocalDateTime.of(2018, 3, 1, 0, 0)).to(LocalDateTime.of(2018, 4, 1, 0, 0)))
                .with(TrackerWindow.from(LocalDateTime.of(2018, 1, 1, 0, 0)).to(LocalDateTime.of(2018, 1, 31, 0, 0)),
                        TrackerWindow.from(LocalDateTime.of(2018, 2, 1, 0, 0)).to(LocalDateTime.of(2018, 3, 1, 0, 0)))
                .deleteWith(deletions::add)
                .execute();

        assertTrue(toCreateOpt.isPresent());
        assertEquals(TrackerWindow
                        .from(LocalDateTime.of(2018, 2, 1, 0, 0))
                        .to(LocalDateTime.of(2018, 4, 1, 0, 0)),
                toCreateOpt.get());

        assertEquals(Collections.singleton(TrackerWindow
                        .from(LocalDateTime.of(2018, 2, 1, 0, 0))
                        .to(LocalDateTime.of(2018, 3, 1, 0, 0))),
                deletions);
    }

    @Test
    public void testMergeDirectlyBeforeExisting() {
        final Set<TrackerWindow> deletions = new HashSet<>();

        // New window is Jan 2017, existing windows are Feb and March
        final Optional<TrackerWindow> toCreateOpt = dateTimeMerger
                .merge(TrackerWindow.from(LocalDateTime.of(2017, 1, 1, 0, 0)).to(LocalDateTime.of(2017, 2, 1, 0, 0)))
                .with(TrackerWindow.from(LocalDateTime.of(2017, 2, 1, 0, 0)).to(LocalDateTime.of(2017, 2, 28, 0, 0)),
                        TrackerWindow.from(LocalDateTime.of(2017, 3, 1, 0, 0)).to(LocalDateTime.of(2017, 4, 1, 0, 0)))
                .deleteWith(deletions::add)
                .execute();

        assertTrue(toCreateOpt.isPresent());
        assertEquals(TrackerWindow
                        .from(LocalDateTime.of(2017, 1, 1, 0, 0))
                        .to(LocalDateTime.of(2017, 2, 28, 0, 0)),
                toCreateOpt.get());

        assertEquals(Collections.singleton(TrackerWindow
                        .from(LocalDateTime.of(2017, 2, 1, 0, 0))
                        .to(LocalDateTime.of(2017, 2, 28, 0, 0))),
                deletions);
    }

    @Test
    public void testMergeBetweenExisting() {
        final Set<TrackerWindow> deletions = new HashSet<>();

        // New window is Feb 2017, existing windows are Jan and March;

        final Optional<TrackerWindow> toCreateOpt = dateTimeMerger
                .merge(TrackerWindow.from(LocalDateTime.of(2017, 2, 1, 0, 0)).to(LocalDateTime.of(2017, 3, 1, 0, 0)))
                .with(TrackerWindow.from(LocalDateTime.of(2017, 1, 1, 0, 0)).to(LocalDateTime.of(2017, 2, 1, 0, 0)),
                        TrackerWindow.from(LocalDateTime.of(2017, 3, 1, 0, 0)).to(LocalDateTime.of(2017, 4, 1, 0, 0)))
                .deleteWith(deletions::add)
                .execute();

        assertTrue(toCreateOpt.isPresent());
        assertEquals(TrackerWindow
                        .from(LocalDateTime.of(2017, 1, 1, 0, 0))
                        .to(LocalDateTime.of(2017, 4, 1, 0, 0)),
                toCreateOpt.get());

        assertEquals(
                Stream.of(
                    TrackerWindow.from(LocalDateTime.of(2017, 1, 1, 0, 0)).to(LocalDateTime.of(2017, 2, 1, 0, 0)),
                    TrackerWindow.from(LocalDateTime.of(2017, 3, 1, 0, 0)).to(LocalDateTime.of(2017, 4, 1, 0, 0))
                ).collect(Collectors.toSet()),
            deletions);
    }
}
