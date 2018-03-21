package stroom.autoindex.tracker;

import org.junit.Test;

import java.time.LocalDateTime;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class WindowMergerTest {

    /**
     * A simple class that contains a to/from window based on integers.
     */
    private static class IntegerWindow {
        final Integer from;
        final Integer to;

        IntegerWindow(final Integer from, final Integer to) {
            this.from = from;
            this.to = to;
        }

        private Integer getFrom() {
            return from;
        }

        private Integer getTo() {
            return to;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("IntegerWindow{");
            sb.append("from=").append(from);
            sb.append(", to=").append(to);
            sb.append('}');
            return sb.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            IntegerWindow that = (IntegerWindow) o;
            return Objects.equals(from, that.from) &&
                    Objects.equals(to, that.to);
        }

        @Override
        public int hashCode() {
            return Objects.hash(from, to);
        }

        private static Builder from(final Integer from) {
            return new Builder(from);
        }

        private static class Builder {
            final Integer from;

            private Builder(final Integer from) {
                this.from = from;
            }

            private IntegerWindow to(final Integer to) {
                return new IntegerWindow(this.from, to);
            }
        }
    }

    // Classes under test
    private final WindowMerger<IntegerWindow, Integer> integerMerger =
            WindowMerger.<IntegerWindow, Integer>withValueGenerator((from, to) -> IntegerWindow.from(from).to(to))
                    .comparator(Integer::compare)
                    .from(IntegerWindow::getFrom)
                    .to(IntegerWindow::getTo)
                    .build();

    private final WindowMerger<TrackerWindow, LocalDateTime> dateTimeMerger =
            WindowMerger.<TrackerWindow, LocalDateTime>withValueGenerator((from, to) -> TrackerWindow.from(from).to(to))
                    .comparator(LocalDateTime::compareTo)
                    .from(TrackerWindow::getFrom)
                    .to(TrackerWindow::getTo)
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

        final Optional<IntegerWindow> toCreateOpt = integerMerger.mergeWindows(
                IntegerWindow.from(2).to(5),
                Arrays.asList(
                        IntegerWindow.from(1).to(4),
                        IntegerWindow.from(9).to(12)
                ),
                deletions::add);

        assertTrue(toCreateOpt.isPresent());

        // Should detect overlap and move the end
        assertEquals(toCreateOpt.get(), IntegerWindow.from(1).to(5));

        // This window that overlapped should be deleted, to be replaced by the new one
        assertEquals(1, deletions.size());
        assertTrue(deletions.contains(IntegerWindow.from(1).to(4)));
    }

    @Test
    public void testOverlapStartDateTime() {
        final Optional<WindowMerger.OverlapStyle> overlapStyle =
                dateTimeMerger.determineOverlap(
                        TrackerWindow.from(LocalDateTime.of(2017, 3, 20, 0, 0)).to(LocalDateTime.of(2017, 3, 29, 0, 0)),
                        TrackerWindow.from(LocalDateTime.of(2017, 3, 15, 0, 0)).to(LocalDateTime.of(2017, 3, 25, 0, 0))
                );
        assertTrue(overlapStyle.isPresent());
        assertEquals(WindowMerger.OverlapStyle.OVERLAP_START, overlapStyle.get());
    }

    @Test
    public void testMergeOverlapStartDateTime() {
        final Set<TrackerWindow> deletions = new HashSet<>();

        final Optional<TrackerWindow> toCreateOpt = dateTimeMerger.mergeWindows(
                TrackerWindow.from(LocalDateTime.of(2017, 3, 20, 0, 0)).to(LocalDateTime.of(2017, 3, 29, 0, 0)),
                Arrays.asList(
                        TrackerWindow.from(LocalDateTime.of(2016, 3, 15, 0, 0)).to(LocalDateTime.of(2016, 3, 25, 0, 0)),
                        TrackerWindow.from(LocalDateTime.of(2017, 3, 15, 0, 0)).to(LocalDateTime.of(2017, 3, 25, 0, 0))
                ),
                deletions::add);

        assertTrue(toCreateOpt.isPresent());

        // Should detect overlap and move the end
        assertEquals(toCreateOpt.get(),
                TrackerWindow
                        .from(LocalDateTime.of(2017, 3, 15, 0, 0))
                        .to(LocalDateTime.of(2017, 3, 29, 0, 0)));

        // This window that overlapped should be deleted, to be replaced by the new one
        assertEquals(1, deletions.size());
        assertTrue(deletions.contains(
                TrackerWindow
                        .from(LocalDateTime.of(2017, 3, 15, 0, 0))
                        .to(LocalDateTime.of(2017, 3, 25, 0, 0))
                )
        );
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

        final Optional<IntegerWindow> toCreateOpt = integerMerger.mergeWindows(
                IntegerWindow.from(9).to(12),
                Arrays.asList(
                        IntegerWindow.from(2).to(5),
                        IntegerWindow.from(10).to(13)
                ),
                deletions::add);

        assertTrue(toCreateOpt.isPresent());

        // Should detect overlap and move the start
        assertEquals(toCreateOpt.get(), IntegerWindow.from(9).to(13));

        // This window that overlapped should be deleted, to be replaced by the new one
        assertEquals(1, deletions.size());
        assertTrue(deletions.contains(IntegerWindow.from(10).to(13)));
    }

    @Test
    public void testOverlapEndDateTime() {
        final Optional<WindowMerger.OverlapStyle> overlapStyle =
                dateTimeMerger.determineOverlap(
                        TrackerWindow.from(LocalDateTime.of(2017, 3, 15, 0, 0)).to(LocalDateTime.of(2017, 3, 25, 0, 0)),
                        TrackerWindow.from(LocalDateTime.of(2017, 3, 20, 0, 0)).to(LocalDateTime.of(2017, 3, 29, 0, 0))
                );
        assertTrue(overlapStyle.isPresent());
        assertEquals(WindowMerger.OverlapStyle.OVERLAP_END, overlapStyle.get());
    }

    @Test
    public void testMergeOverlapEndDateTime() {
        final Set<TrackerWindow> deletions = new HashSet<>();

        final Optional<TrackerWindow> toCreateOpt = dateTimeMerger.mergeWindows(
                TrackerWindow.from(LocalDateTime.of(2017, 3, 15, 0, 0)).to(LocalDateTime.of(2017, 3, 25, 0, 0)),
                Arrays.asList(
                        TrackerWindow.from(LocalDateTime.of(2016, 3, 20, 0, 0)).to(LocalDateTime.of(2016, 3, 29, 0, 0)),
                        TrackerWindow.from(LocalDateTime.of(2017, 3, 20, 0, 0)).to(LocalDateTime.of(2017, 3, 29, 0, 0))
                ),
                deletions::add);

        assertTrue(toCreateOpt.isPresent());

        // Should detect overlap and move the start
        assertEquals(toCreateOpt.get(),
                TrackerWindow
                        .from(LocalDateTime.of(2017, 3, 15, 0, 0))
                        .to(LocalDateTime.of(2017, 3, 29, 0, 0))
        );

        // This window that overlapped should be deleted, to be replaced by the new one
        assertEquals(1, deletions.size());
        assertTrue(deletions.contains(
                TrackerWindow
                        .from(LocalDateTime.of(2017, 3, 20, 0, 0))
                        .to(LocalDateTime.of(2017, 3, 29, 0, 0))
                )
        );
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

        final Optional<IntegerWindow> toCreateOpt = integerMerger.mergeWindows(
                IntegerWindow.from(20).to(44),
                Arrays.asList(
                        IntegerWindow.from(2).to(5),
                        IntegerWindow.from(24).to(30)
                ),
                deletions::add);

        assertTrue(toCreateOpt.isPresent());

        // Should detect overlap and move the start
        assertEquals(toCreateOpt.get(), IntegerWindow.from(20).to(44));

        // This window that overlapped should be deleted, to be replaced by the new one
        assertEquals(1, deletions.size());
        assertTrue(deletions.contains(IntegerWindow.from(24).to(30)));
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

        final Optional<TrackerWindow> toCreateOpt = dateTimeMerger.mergeWindows(
                TrackerWindow.from(LocalDateTime.of(2018, 10, 5, 0, 0)).to(LocalDateTime.of(2018, 12, 20, 0, 0)),
                Arrays.asList(
                        TrackerWindow.from(LocalDateTime.of(2017, 11, 20, 0, 0)).to(LocalDateTime.of(2017, 12, 13, 0, 0)),
                        TrackerWindow.from(LocalDateTime.of(2018, 11, 20, 0, 0)).to(LocalDateTime.of(2018, 12, 13, 0, 0))
                ),
                deletions::add);

        assertTrue(toCreateOpt.isPresent());

        // Should detect overlap and move the start
        assertEquals(toCreateOpt.get(),
                TrackerWindow
                        .from(LocalDateTime.of(2018, 10, 5, 0, 0))
                        .to(LocalDateTime.of(2018, 12, 20, 0, 0)));

        // This window that overlapped should be deleted, to be replaced by the new one
        assertEquals(1, deletions.size());
        assertTrue(deletions.contains(
                TrackerWindow
                        .from(LocalDateTime.of(2018, 11, 20, 0, 0))
                        .to(LocalDateTime.of(2018, 12, 13, 0, 0))
                )
        );
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

        final Optional<IntegerWindow> toCreateOpt = integerMerger.mergeWindows(
                IntegerWindow.from(24).to(30),
                Arrays.asList(
                        IntegerWindow.from(2).to(5),
                        IntegerWindow.from(20).to(44)
                ),
                deletions::add);

        assertFalse(toCreateOpt.isPresent());
        assertEquals(0, deletions.size());
    }

    @Test
    public void testNewSubsumedByExistingDateTime() {
        final Optional<WindowMerger.OverlapStyle> overlapStyle =
                dateTimeMerger.determineOverlap(
                        TrackerWindow.from(LocalDateTime.of(2018, 11, 20, 0, 0)).to(LocalDateTime.of(2018, 12, 13, 0, 0)),
                        TrackerWindow.from(LocalDateTime.of(2018, 10, 5, 0, 0)).to(LocalDateTime.of(2018, 12, 20, 0, 0))
                );
        assertTrue(overlapStyle.isPresent());
        assertEquals(WindowMerger.OverlapStyle.NEW_SUBSUMED_BY_EXISTING, overlapStyle.get());
    }

    @Test
    public void testMergeNewSubsumedByExistingDateTime() {
        final Set<TrackerWindow> deletions = new HashSet<>();

        final Optional<TrackerWindow> toCreateOpt = dateTimeMerger.mergeWindows(
                TrackerWindow.from(LocalDateTime.of(2018, 11, 20, 0, 0)).to(LocalDateTime.of(2018, 12, 13, 0, 0)),
                Arrays.asList(
                        TrackerWindow.from(LocalDateTime.of(2017, 10, 5, 0, 0)).to(LocalDateTime.of(2017, 12, 20, 0, 0)),
                        TrackerWindow.from(LocalDateTime.of(2018, 10, 5, 0, 0)).to(LocalDateTime.of(2018, 12, 20, 0, 0))
                ),
                deletions::add);

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

        final Optional<IntegerWindow> toCreateOpt = integerMerger.mergeWindows(
                IntegerWindow.from(24).to(30),
                Arrays.asList(
                        IntegerWindow.from(2).to(5),
                        IntegerWindow.from(10).to(23)
                ),
                deletions::add);

        assertTrue(toCreateOpt.isPresent());

        // Should detect overlap and move the start
        assertEquals(toCreateOpt.get(), IntegerWindow.from(24).to(30));

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

        final Optional<TrackerWindow> toCreateOpt = dateTimeMerger.mergeWindows(
                TrackerWindow.from(LocalDateTime.of(2018, 2, 20, 0, 0)).to(LocalDateTime.of(2018, 3, 20, 0, 0)),
                Arrays.asList(
                        TrackerWindow.from(LocalDateTime.of(2015, 1, 10, 0, 0)).to(LocalDateTime.of(2017, 2, 10, 0, 0)),
                        TrackerWindow.from(LocalDateTime.of(2018, 1, 10, 0, 0)).to(LocalDateTime.of(2018, 2, 10, 0, 0))
                ),
                deletions::add);

        assertTrue(toCreateOpt.isPresent());

        // Should detect overlap and move the start
        assertEquals(toCreateOpt.get(),
                TrackerWindow
                        .from(LocalDateTime.of(2018, 2, 20, 0, 0))
                        .to(LocalDateTime.of(2018, 3, 20, 0, 0)));

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

        final Optional<IntegerWindow> toCreateOpt = integerMerger.mergeWindows(
                IntegerWindow.from(10).to(13),
                Arrays.asList(
                        IntegerWindow.from(2).to(5),
                        IntegerWindow.from(20).to(44)
                ),
                deletions::add);

        assertTrue(toCreateOpt.isPresent());

        // Should detect overlap and move the start
        assertEquals(toCreateOpt.get(), IntegerWindow.from(10).to(13));

        // This window that overlapped should be deleted, to be replaced by the new one
        assertEquals(0, deletions.size());
    }

    @Test
    public void newIsNoOverlapBeforeExistingDateTime() {
        final Optional<WindowMerger.OverlapStyle> overlapStyle =
                dateTimeMerger.determineOverlap(
                        TrackerWindow.from(LocalDateTime.of(2016, 3, 1, 0, 0)).to(LocalDateTime.of(2017, 3, 1, 0, 0)),
                        TrackerWindow.from(LocalDateTime.of(2017, 3, 2, 0, 0)).to(LocalDateTime.of(2018, 3, 1, 0, 0))
                );
        assertTrue(overlapStyle.isPresent());
        assertEquals(WindowMerger.OverlapStyle.NO_OVERLAP, overlapStyle.get());
    }

    @Test
    public void testMergeNewIsNoOverlapBeforeExistingDateTime() {
        final Set<TrackerWindow> deletions = new HashSet<>();

        final Optional<TrackerWindow> toCreateOpt = dateTimeMerger.mergeWindows(
                TrackerWindow.from(LocalDateTime.of(2016, 3, 1, 0, 0)).to(LocalDateTime.of(2017, 3, 1, 0, 0)),
                Arrays.asList(
                        TrackerWindow.from(LocalDateTime.of(2015, 3, 2, 0, 0)).to(LocalDateTime.of(2015, 3, 1, 0, 0)),
                        TrackerWindow.from(LocalDateTime.of(2017, 3, 2, 0, 0)).to(LocalDateTime.of(2018, 3, 1, 0, 0))
                ),
                deletions::add);

        assertTrue(toCreateOpt.isPresent());

        // Should detect overlap and move the start
        assertEquals(toCreateOpt.get(),
                TrackerWindow
                        .from(LocalDateTime.of(2016, 3, 1, 0, 0))
                        .to(LocalDateTime.of(2017, 3, 1, 0, 0))
        );

        // This window that overlapped should be deleted, to be replaced by the new one
        assertEquals(0, deletions.size());
    }
}
