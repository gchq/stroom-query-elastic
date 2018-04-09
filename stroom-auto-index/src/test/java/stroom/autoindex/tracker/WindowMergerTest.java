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
    private final WindowMerger<Long, TrackerWindow> longMerger =
            WindowMerger.<Long, TrackerWindow>withValueGenerator((from, to) -> TrackerWindow.from(from).to(to))
                    .comparator(Long::compare)
                    .build();

    private final WindowMerger<LocalDateTime, LocalDateTimeWindow> dateTimeMerger =
            WindowMerger.<LocalDateTime, LocalDateTimeWindow>withValueGenerator((from, to) -> LocalDateTimeWindow.from(from).to(to))
                    .comparator(LocalDateTime::compareTo)
                    .build();

    @Test
    public void testOverlapStartInteger() {
        final Optional<WindowMerger.OverlapStyle> overlapStyle =
                longMerger.determineOverlap(TrackerWindow.from(2L).to(5L), TrackerWindow.from(1L).to(4L));
        assertTrue(overlapStyle.isPresent());
        assertEquals(WindowMerger.OverlapStyle.OVERLAP_START, overlapStyle.get());
    }

    @Test
    public void testMergeOverlapStartInteger() {
        final Set<TrackerWindow> deletions = new HashSet<>();

        final Optional<TrackerWindow> toCreateOpt = longMerger
                .merge(TrackerWindow.from(2L).to(5L))
                .with(TrackerWindow.from(1L).to(4L),
                        TrackerWindow.from(9L).to(12L))
                .deleteWith(deletions::add)
                .execute();

        assertTrue(toCreateOpt.isPresent());

        // Should detect overlap and move the end
        assertEquals(toCreateOpt.get(), TrackerWindow.from(1L).to(5L));

        // This window that overlapped should be deleted, to be replaced by the new one
        assertEquals(Collections.singleton(TrackerWindow.from(1L).to(4L)), deletions);
    }

    @Test
    public void testOverlapStartDateTime() {
        final Optional<WindowMerger.OverlapStyle> overlapStyle =
                dateTimeMerger.determineOverlap(
                        LocalDateTimeWindow.from(LocalDateTime.of(2017, 3, 20, 0, 0)).to(LocalDateTime.of(2017, 3, 29, 0, 0)),
                        LocalDateTimeWindow.from(LocalDateTime.of(2017, 3, 15, 0, 0)).to(LocalDateTime.of(2017, 3, 25, 0, 0)));
        assertTrue(overlapStyle.isPresent());
        assertEquals(WindowMerger.OverlapStyle.OVERLAP_START, overlapStyle.get());
    }

    @Test
    public void testMergeOverlapStartDateTime() {
        final Set<LocalDateTimeWindow> deletions = new HashSet<>();

        final Optional<LocalDateTimeWindow> toCreateOpt = dateTimeMerger
                .merge(LocalDateTimeWindow.from(LocalDateTime.of(2017, 3, 20, 0, 0)).to(LocalDateTime.of(2017, 3, 29, 0, 0)))
                .with(LocalDateTimeWindow.from(LocalDateTime.of(2016, 3, 15, 0, 0)).to(LocalDateTime.of(2016, 3, 25, 0, 0)),
                        LocalDateTimeWindow.from(LocalDateTime.of(2017, 3, 15, 0, 0)).to(LocalDateTime.of(2017, 3, 25, 0, 0)))
                .deleteWith(deletions::add)
                .execute();

        assertTrue(toCreateOpt.isPresent());

        // Should detect overlap and move the end
        assertEquals(LocalDateTimeWindow
                        .from(LocalDateTime.of(2017, 3, 15, 0, 0))
                        .to(LocalDateTime.of(2017, 3, 29, 0, 0)), toCreateOpt.get());

        // This window that overlapped should be deleted, to be replaced by the new one
        assertEquals(Collections.singleton(LocalDateTimeWindow
                        .from(LocalDateTime.of(2017, 3, 15, 0, 0))
                        .to(LocalDateTime.of(2017, 3, 25, 0, 0))),
                deletions);
    }

    @Test
    public void testOverlapEndInteger() {
        final Optional<WindowMerger.OverlapStyle> overlapStyle =
                longMerger.determineOverlap(TrackerWindow.from(9L).to(12L), TrackerWindow.from(10L).to(13L));
        assertTrue(overlapStyle.isPresent());
        assertEquals(WindowMerger.OverlapStyle.OVERLAP_END, overlapStyle.get());
    }

    @Test
    public void testMergeOverlapEndInteger() {
        final Set<TrackerWindow> deletions = new HashSet<>();

        final Optional<TrackerWindow> toCreateOpt = longMerger
                .merge(TrackerWindow.from(9L).to(12L))
                .with(TrackerWindow.from(2L).to(5L),
                        TrackerWindow.from(10L).to(13L))
                .deleteWith(deletions::add)
                .execute();

        assertTrue(toCreateOpt.isPresent());

        // Should detect overlap and move the start
        assertEquals(TrackerWindow.from(9L).to(13L), toCreateOpt.get());

        // This window that overlapped should be deleted, to be replaced by the new one
        assertEquals(Collections.singleton(TrackerWindow.from(10L).to(13L)), deletions);
    }

    @Test
    public void testOverlapEndDateTime() {
        final Optional<WindowMerger.OverlapStyle> overlapStyle =
                dateTimeMerger.determineOverlap(
                        LocalDateTimeWindow.from(LocalDateTime.of(2017, 3, 15, 0, 0)).to(LocalDateTime.of(2017, 3, 25, 0, 0)),
                        LocalDateTimeWindow.from(LocalDateTime.of(2017, 3, 20, 0, 0)).to(LocalDateTime.of(2017, 3, 29, 0, 0)));
        assertTrue(overlapStyle.isPresent());
        assertEquals(WindowMerger.OverlapStyle.OVERLAP_END, overlapStyle.get());
    }

    @Test
    public void testMergeOverlapEndDateTime() {
        final Set<LocalDateTimeWindow> deletions = new HashSet<>();

        final Optional<LocalDateTimeWindow> toCreateOpt = dateTimeMerger
                .merge(LocalDateTimeWindow.from(LocalDateTime.of(2017, 3, 15, 0, 0)).to(LocalDateTime.of(2017, 3, 25, 0, 0)))
                .with(LocalDateTimeWindow.from(LocalDateTime.of(2016, 3, 20, 0, 0)).to(LocalDateTime.of(2016, 3, 29, 0, 0)),
                        LocalDateTimeWindow.from(LocalDateTime.of(2017, 3, 20, 0, 0)).to(LocalDateTime.of(2017, 3, 29, 0, 0)))
                .deleteWith(deletions::add)
                .execute();

        assertTrue(toCreateOpt.isPresent());

        // Should detect overlap and move the start
        assertEquals(LocalDateTimeWindow
                        .from(LocalDateTime.of(2017, 3, 15, 0, 0))
                        .to(LocalDateTime.of(2017, 3, 29, 0, 0)),
                toCreateOpt.get());

        // This window that overlapped should be deleted, to be replaced by the new one
        assertEquals(Collections.singleton(LocalDateTimeWindow
                        .from(LocalDateTime.of(2017, 3, 20, 0, 0))
                        .to(LocalDateTime.of(2017, 3, 29, 0, 0))),
                deletions);
    }

    @Test
    public void testExistingSubsumedByNewInteger() {
        final Optional<WindowMerger.OverlapStyle> overlapStyle =
                longMerger.determineOverlap(TrackerWindow.from(20L).to(44L), TrackerWindow.from(24L).to(30L));
        assertTrue(overlapStyle.isPresent());
        assertEquals(WindowMerger.OverlapStyle.EXISTING_SUBSUMED_BY_NEW, overlapStyle.get());
    }

    @Test
    public void testMergeExistingSubsumedByNewInteger() {
        final Set<TrackerWindow> deletions = new HashSet<>();

        final Optional<TrackerWindow> toCreateOpt = longMerger
                .merge(TrackerWindow.from(20L).to(44L))
                .with(TrackerWindow.from(2L).to(5L),
                        TrackerWindow.from(24L).to(30L))
                .deleteWith(deletions::add)
                .execute();

        assertTrue(toCreateOpt.isPresent());

        // Should detect overlap and move the start
        assertEquals(TrackerWindow.from(20L).to(44L), toCreateOpt.get());

        // This window that overlapped should be deleted, to be replaced by the new one
        assertEquals(Collections.singleton(TrackerWindow.from(24L).to(30L)), deletions);
    }

    @Test
    public void testExistingSubsumedByNewDateTime() {
        final Optional<WindowMerger.OverlapStyle> overlapStyle =
                dateTimeMerger.determineOverlap(
                        LocalDateTimeWindow.from(LocalDateTime.of(2018, 10, 5, 0, 0)).to(LocalDateTime.of(2018, 12, 20, 0, 0)),
                        LocalDateTimeWindow.from(LocalDateTime.of(2018, 11, 20, 0, 0)).to(LocalDateTime.of(2018, 12, 13, 0, 0))
                );
        assertTrue(overlapStyle.isPresent());
        assertEquals(WindowMerger.OverlapStyle.EXISTING_SUBSUMED_BY_NEW, overlapStyle.get());
    }

    @Test
    public void testMergeExistingSubsumedByNewDateTime() {
        final Set<LocalDateTimeWindow> deletions = new HashSet<>();

        final Optional<LocalDateTimeWindow> toCreateOpt = dateTimeMerger
                .merge(LocalDateTimeWindow.from(LocalDateTime.of(2018, 10, 5, 0, 0)).to(LocalDateTime.of(2018, 12, 20, 0, 0)))
                .with(LocalDateTimeWindow.from(LocalDateTime.of(2017, 11, 20, 0, 0)).to(LocalDateTime.of(2017, 12, 13, 0, 0)),
                        LocalDateTimeWindow.from(LocalDateTime.of(2018, 11, 20, 0, 0)).to(LocalDateTime.of(2018, 12, 13, 0, 0)))
                .deleteWith(deletions::add)
                .execute();

        assertTrue(toCreateOpt.isPresent());

        // Should detect overlap and move the start
        assertEquals(LocalDateTimeWindow
                        .from(LocalDateTime.of(2018, 10, 5, 0, 0))
                        .to(LocalDateTime.of(2018, 12, 20, 0, 0)),
                toCreateOpt.get());

        // This window that overlapped should be deleted, to be replaced by the new one
        assertEquals(Collections.singleton(LocalDateTimeWindow
                        .from(LocalDateTime.of(2018, 11, 20, 0, 0))
                        .to(LocalDateTime.of(2018, 12, 13, 0, 0))),
                deletions);
    }

    @Test
    public void testNewSubsumedByExistingInteger() {
        final Optional<WindowMerger.OverlapStyle> overlapStyle =
                longMerger.determineOverlap(TrackerWindow.from(24L).to(30L), TrackerWindow.from(20L).to(44L));
        assertTrue(overlapStyle.isPresent());
        assertEquals(WindowMerger.OverlapStyle.NEW_SUBSUMED_BY_EXISTING, overlapStyle.get());
    }

    @Test
    public void testMergeNewSubsumedByExistingInteger() {
        final Set<TrackerWindow> deletions = new HashSet<>();

        final Optional<TrackerWindow> toCreateOpt = longMerger
                .merge(TrackerWindow.from(24L).to(30L))
                .with(TrackerWindow.from(2L).to(5L),
                        TrackerWindow.from(20L).to(44L))
                .deleteWith(deletions::add)
                .execute();

        assertFalse(toCreateOpt.isPresent());
        assertEquals(0, deletions.size());
    }

    @Test
    public void testNewSubsumedByExistingDateTime() {
        final Optional<WindowMerger.OverlapStyle> overlapStyle =
                dateTimeMerger.determineOverlap(
                        LocalDateTimeWindow.from(LocalDateTime.of(2018, 11, 20, 0, 0)).to(LocalDateTime.of(2018, 12, 13, 0, 0)),
                        LocalDateTimeWindow.from(LocalDateTime.of(2018, 10, 5, 0, 0)).to(LocalDateTime.of(2018, 12, 20, 0, 0)));
        assertTrue(overlapStyle.isPresent());
        assertEquals(WindowMerger.OverlapStyle.NEW_SUBSUMED_BY_EXISTING, overlapStyle.get());
    }

    @Test
    public void testMergeNewSubsumedByExistingDateTime() {
        final Set<LocalDateTimeWindow> deletions = new HashSet<>();

        final Optional<LocalDateTimeWindow> toCreateOpt = dateTimeMerger
                .merge(LocalDateTimeWindow.from(LocalDateTime.of(2018, 11, 20, 0, 0)).to(LocalDateTime.of(2018, 12, 13, 0, 0)))
                .with(LocalDateTimeWindow.from(LocalDateTime.of(2017, 10, 5, 0, 0)).to(LocalDateTime.of(2017, 12, 20, 0, 0)),
                        LocalDateTimeWindow.from(LocalDateTime.of(2018, 10, 5, 0, 0)).to(LocalDateTime.of(2018, 12, 20, 0, 0)))
                .deleteWith(deletions::add)
                .execute();

        assertFalse(toCreateOpt.isPresent());
        assertEquals(0, deletions.size());
    }

    @Test
    public void newIsNoOverlapAfterExistingInteger() {
        final Optional<WindowMerger.OverlapStyle> overlapStyle =
                longMerger.determineOverlap(TrackerWindow.from(24L).to(30L), TrackerWindow.from(10L).to(23L));
        assertTrue(overlapStyle.isPresent());
        assertEquals(WindowMerger.OverlapStyle.NO_OVERLAP, overlapStyle.get());
    }

    @Test
    public void testMergeNewIsNoOverlapAfterExistingInteger() {
        final Set<TrackerWindow> deletions = new HashSet<>();

        final Optional<TrackerWindow> toCreateOpt = longMerger
                .merge(TrackerWindow.from(24L).to(30L))
                .with(TrackerWindow.from(2L).to(5L),
                        TrackerWindow.from(10L).to(23L))
                .deleteWith(deletions::add)
                .execute();

        assertTrue(toCreateOpt.isPresent());

        // Should detect overlap and move the start
        assertEquals(TrackerWindow.from(24L).to(30L), toCreateOpt.get());

        // This window that overlapped should be deleted, to be replaced by the new one
        assertEquals(0, deletions.size());
    }

    @Test
    public void newIsNoOverlapAfterExistingDateTime() {
        final Optional<WindowMerger.OverlapStyle> overlapStyle =
                dateTimeMerger.determineOverlap(
                        LocalDateTimeWindow.from(LocalDateTime.of(2018, 2, 20, 0, 0)).to(LocalDateTime.of(2018, 3, 20, 0, 0)),
                        LocalDateTimeWindow.from(LocalDateTime.of(2018, 1, 10, 0, 0)).to(LocalDateTime.of(2018, 2, 10, 0, 0))
                );
        assertTrue(overlapStyle.isPresent());
        assertEquals(WindowMerger.OverlapStyle.NO_OVERLAP, overlapStyle.get());
    }

    @Test
    public void testMergeNewIsNoOverlapAfterExistingDateTime() {
        final Set<LocalDateTimeWindow> deletions = new HashSet<>();

        final Optional<LocalDateTimeWindow> toCreateOpt = dateTimeMerger
                .merge(LocalDateTimeWindow.from(LocalDateTime.of(2018, 2, 20, 0, 0)).to(LocalDateTime.of(2018, 3, 20, 0, 0)))
                .with(LocalDateTimeWindow.from(LocalDateTime.of(2015, 1, 10, 0, 0)).to(LocalDateTime.of(2017, 2, 10, 0, 0)),
                        LocalDateTimeWindow.from(LocalDateTime.of(2018, 1, 10, 0, 0)).to(LocalDateTime.of(2018, 2, 10, 0, 0)))
                .deleteWith(deletions::add)
                .execute();

        assertTrue(toCreateOpt.isPresent());

        // Should detect overlap and move the start
        assertEquals(LocalDateTimeWindow
                        .from(LocalDateTime.of(2018, 2, 20, 0, 0))
                        .to(LocalDateTime.of(2018, 3, 20, 0, 0)),
                toCreateOpt.get());

        // This window that overlapped should be deleted, to be replaced by the new one
        assertEquals(0, deletions.size());
    }

    @Test
    public void newIsNoOverlapBeforeExistingInteger() {
        final Optional<WindowMerger.OverlapStyle> overlapStyle =
                longMerger.determineOverlap(TrackerWindow.from(10L).to(13L), TrackerWindow.from(20L).to(44L));
        assertTrue(overlapStyle.isPresent());
        assertEquals(WindowMerger.OverlapStyle.NO_OVERLAP, overlapStyle.get());
    }

    @Test
    public void testMergeNewIsNoOverlapBeforeExistingInteger() {
        final Set<TrackerWindow> deletions = new HashSet<>();

        final Optional<TrackerWindow> toCreateOpt = longMerger
                .merge(TrackerWindow.from(10L).to(13L))
                .with(TrackerWindow.from(2L).to(5L),
                        TrackerWindow.from(20L).to(44L))
                .deleteWith(deletions::add)
                .execute();

        assertTrue(toCreateOpt.isPresent());

        // Should detect overlap and move the start
        assertEquals(TrackerWindow.from(10L).to(13L), toCreateOpt.get());

        // This window that overlapped should be deleted, to be replaced by the new one
        assertEquals(0, deletions.size());
    }

    @Test
    public void newIsNoOverlapBeforeExistingDateTime() {
        final Optional<WindowMerger.OverlapStyle> overlapStyle =
                dateTimeMerger.determineOverlap(
                        LocalDateTimeWindow.from(LocalDateTime.of(2016, 3, 1, 0, 0)).to(LocalDateTime.of(2017, 3, 1, 0, 0)),
                        LocalDateTimeWindow.from(LocalDateTime.of(2017, 3, 2, 0, 0)).to(LocalDateTime.of(2018, 3, 1, 0, 0)));
        assertTrue(overlapStyle.isPresent());
        assertEquals(WindowMerger.OverlapStyle.NO_OVERLAP, overlapStyle.get());
    }

    @Test
    public void testMergeNewIsNoOverlapBeforeExistingDateTime() {
        final Set<LocalDateTimeWindow> deletions = new HashSet<>();

        final Optional<LocalDateTimeWindow> toCreateOpt = dateTimeMerger
                .merge(LocalDateTimeWindow.from(LocalDateTime.of(2016, 3, 1, 0, 0)).to(LocalDateTime.of(2017, 3, 1, 0, 0)))
                .with(LocalDateTimeWindow.from(LocalDateTime.of(2015, 3, 2, 0, 0)).to(LocalDateTime.of(2015, 4, 1, 0, 0)),
                        LocalDateTimeWindow.from(LocalDateTime.of(2017, 3, 2, 0, 0)).to(LocalDateTime.of(2018, 4, 1, 0, 0)))
                .deleteWith(deletions::add)
                .execute();

        assertTrue(toCreateOpt.isPresent());

        // Should detect overlap and move the start
        assertEquals(LocalDateTimeWindow
                        .from(LocalDateTime.of(2016, 3, 1, 0, 0))
                        .to(LocalDateTime.of(2017, 3, 1, 0, 0)),
                toCreateOpt.get());

        // This window that overlapped should be deleted, to be replaced by the new one
        assertEquals(0, deletions.size());
    }

    /// Now some cases were windows should join up
    @Test
    public void testMergeNewDirectlyAfterExisting() {
        final Set<LocalDateTimeWindow> deletions = new HashSet<>();

        // New window is March 2018, existing windows are Jan and Feb
        final Optional<LocalDateTimeWindow> toCreateOpt = dateTimeMerger
                .merge(LocalDateTimeWindow.from(LocalDateTime.of(2018, 3, 1, 0, 0)).to(LocalDateTime.of(2018, 4, 1, 0, 0)))
                .with(LocalDateTimeWindow.from(LocalDateTime.of(2018, 1, 1, 0, 0)).to(LocalDateTime.of(2018, 1, 31, 0, 0)),
                        LocalDateTimeWindow.from(LocalDateTime.of(2018, 2, 1, 0, 0)).to(LocalDateTime.of(2018, 3, 1, 0, 0)))
                .deleteWith(deletions::add)
                .execute();

        assertTrue(toCreateOpt.isPresent());
        assertEquals(LocalDateTimeWindow
                        .from(LocalDateTime.of(2018, 2, 1, 0, 0))
                        .to(LocalDateTime.of(2018, 4, 1, 0, 0)),
                toCreateOpt.get());

        assertEquals(Collections.singleton(LocalDateTimeWindow
                        .from(LocalDateTime.of(2018, 2, 1, 0, 0))
                        .to(LocalDateTime.of(2018, 3, 1, 0, 0))),
                deletions);
    }

    @Test
    public void testMergeDirectlyBeforeExisting() {
        final Set<LocalDateTimeWindow> deletions = new HashSet<>();

        // New window is Jan 2017, existing windows are Feb and March
        final Optional<LocalDateTimeWindow> toCreateOpt = dateTimeMerger
                .merge(LocalDateTimeWindow.from(LocalDateTime.of(2017, 1, 1, 0, 0)).to(LocalDateTime.of(2017, 2, 1, 0, 0)))
                .with(LocalDateTimeWindow.from(LocalDateTime.of(2017, 2, 1, 0, 0)).to(LocalDateTime.of(2017, 2, 28, 0, 0)),
                        LocalDateTimeWindow.from(LocalDateTime.of(2017, 3, 1, 0, 0)).to(LocalDateTime.of(2017, 4, 1, 0, 0)))
                .deleteWith(deletions::add)
                .execute();

        assertTrue(toCreateOpt.isPresent());
        assertEquals(LocalDateTimeWindow
                        .from(LocalDateTime.of(2017, 1, 1, 0, 0))
                        .to(LocalDateTime.of(2017, 2, 28, 0, 0)),
                toCreateOpt.get());

        assertEquals(Collections.singleton(LocalDateTimeWindow
                        .from(LocalDateTime.of(2017, 2, 1, 0, 0))
                        .to(LocalDateTime.of(2017, 2, 28, 0, 0))),
                deletions);
    }

    @Test
    public void testMergeBetweenExisting() {
        final Set<LocalDateTimeWindow> deletions = new HashSet<>();

        // New window is Feb 2017, existing windows are Jan and March;

        final Optional<LocalDateTimeWindow> toCreateOpt = dateTimeMerger
                .merge(LocalDateTimeWindow.from(LocalDateTime.of(2017, 2, 1, 0, 0)).to(LocalDateTime.of(2017, 3, 1, 0, 0)))
                .with(LocalDateTimeWindow.from(LocalDateTime.of(2017, 1, 1, 0, 0)).to(LocalDateTime.of(2017, 2, 1, 0, 0)),
                        LocalDateTimeWindow.from(LocalDateTime.of(2017, 3, 1, 0, 0)).to(LocalDateTime.of(2017, 4, 1, 0, 0)))
                .deleteWith(deletions::add)
                .execute();

        assertTrue(toCreateOpt.isPresent());
        assertEquals(LocalDateTimeWindow
                        .from(LocalDateTime.of(2017, 1, 1, 0, 0))
                        .to(LocalDateTime.of(2017, 4, 1, 0, 0)),
                toCreateOpt.get());

        assertEquals(
                Stream.of(
                    LocalDateTimeWindow.from(LocalDateTime.of(2017, 1, 1, 0, 0)).to(LocalDateTime.of(2017, 2, 1, 0, 0)),
                    LocalDateTimeWindow.from(LocalDateTime.of(2017, 3, 1, 0, 0)).to(LocalDateTime.of(2017, 4, 1, 0, 0))
                ).collect(Collectors.toSet()),
            deletions);
    }
}
