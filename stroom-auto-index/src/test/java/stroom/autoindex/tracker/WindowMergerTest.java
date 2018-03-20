package stroom.autoindex.tracker;

import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
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

    // Class under test
    private final WindowMerger<IntegerWindow, Integer> merger =
            WindowMerger.<IntegerWindow, Integer>withValueGenerator((from, to) -> IntegerWindow.from(from).to(to))
                    .from(IntegerWindow::getFrom)
                    .to(IntegerWindow::getTo)
                    .build();

    @Test
    public void testOverlapStart() {
        final Optional<WindowMerger.OverlapStyle> overlapStyle =
                merger.determineOverlap(IntegerWindow.from(2).to(5), IntegerWindow.from(1).to(4));
        assertTrue(overlapStyle.isPresent());
        assertEquals(WindowMerger.OverlapStyle.OVERLAP_START, overlapStyle.get());

    }

    @Test
    public void testOverlapEnd() {
        final Optional<WindowMerger.OverlapStyle> overlapStyle =
                merger.determineOverlap(IntegerWindow.from(9).to(12), IntegerWindow.from(10).to(13));
        assertTrue(overlapStyle.isPresent());
        assertEquals(WindowMerger.OverlapStyle.OVERLAP_END, overlapStyle.get());
    }

    @Test
    public void testExistingSubsumedByNew() {
        final Optional<WindowMerger.OverlapStyle> overlapStyle =
                merger.determineOverlap(IntegerWindow.from(20).to(44), IntegerWindow.from(24).to(30));
        assertTrue(overlapStyle.isPresent());
        assertEquals(WindowMerger.OverlapStyle.EXISTING_SUBSUMED_BY_NEW, overlapStyle.get());
    }

    @Test
    public void testNewSubsumedByExisting() {
        final Optional<WindowMerger.OverlapStyle> overlapStyle =
                merger.determineOverlap(IntegerWindow.from(24).to(30), IntegerWindow.from(20).to(44));
        assertTrue(overlapStyle.isPresent());
        assertEquals(WindowMerger.OverlapStyle.NEW_SUBSUMED_BY_EXISTING, overlapStyle.get());
    }

    @Test
    public void newIsNoOverlapAfterExisting() {
        final Optional<WindowMerger.OverlapStyle> overlapStyle =
                merger.determineOverlap(IntegerWindow.from(24).to(30), IntegerWindow.from(10).to(23));
        assertTrue(overlapStyle.isPresent());
        assertEquals(WindowMerger.OverlapStyle.NO_OVERLAP, overlapStyle.get());
    }

    @Test
    public void newIsNoOverlapBeforeExisting() {
        final Optional<WindowMerger.OverlapStyle> overlapStyle =
                merger.determineOverlap(IntegerWindow.from(10).to(13), IntegerWindow.from(20).to(44));
        assertTrue(overlapStyle.isPresent());
        assertEquals(WindowMerger.OverlapStyle.NO_OVERLAP, overlapStyle.get());
    }

    @Test
    public void testMergeOverlapStart() {
        final Set<IntegerWindow> deletions = new HashSet<>();

        final Optional<IntegerWindow> toCreateOpt = merger.mergeWindows(
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
    public void testMergeOverlapEnd() {
        final Set<IntegerWindow> deletions = new HashSet<>();

        final Optional<IntegerWindow> toCreateOpt = merger.mergeWindows(
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
}
