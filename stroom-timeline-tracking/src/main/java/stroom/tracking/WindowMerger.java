package stroom.tracking;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * A class that can be used to determine overlaps between windows of any type and merge them down
 * to a reduced set of windows that cover the same space.
 * @param <HAS_BOUNDS> The class that contains the comparables.
 * @param <COMPARABLE> The type of the property that defines the window
 */
class WindowMerger<COMPARABLE, HAS_BOUNDS extends HasBounds<COMPARABLE>> {

    private final Comparator<COMPARABLE> comparator;
    private final BiFunction<COMPARABLE, COMPARABLE, HAS_BOUNDS> newValueGenerator;

    private WindowMerger(final Builder<COMPARABLE, HAS_BOUNDS> builder) {
        this.comparator = Objects.requireNonNull(builder.comparator);
        this.newValueGenerator = Objects.requireNonNull(builder.newValueGenerator);
    }

    public class MergeProcessBuilder {
        private final HAS_BOUNDS windowToAdd;
        private final List<HAS_BOUNDS> existingWindows = new ArrayList<>();
        private Consumer<HAS_BOUNDS> deletionHandler;

        MergeProcessBuilder(final HAS_BOUNDS windowToAdd) {
            this.windowToAdd = windowToAdd;
        }

        MergeProcessBuilder with(final List<HAS_BOUNDS> existingWindows) {
            this.existingWindows.addAll(existingWindows);
            return this;
        }

        MergeProcessBuilder with(final HAS_BOUNDS ... existingWindows) {
            return with(Arrays.asList(existingWindows));
        }


        MergeProcessBuilder deleteWith(final Consumer<HAS_BOUNDS> deletionHandler) {
            this.deletionHandler = deletionHandler;
            return this;
        }

        Optional<HAS_BOUNDS> execute() {
            Objects.requireNonNull(this.windowToAdd, "Must specific window to merge");
            Objects.requireNonNull(this.deletionHandler, "Must specific a deletion handler");

            return WindowMerger.this.mergeWindows(this.windowToAdd, this.existingWindows, this.deletionHandler);
        }
    }

    MergeProcessBuilder merge(final HAS_BOUNDS windowToAdd) {
        return new MergeProcessBuilder(windowToAdd);
    }

    /**
     * Calculates which window needs to be added (if any) and which existing ones should be deleted when a new
     * window is being added.
     *
     * Windows should be merged where possible to minimise the number of time windowing terms being given to the underlying
     * data sources.
     * @param windowToAdd The Time Window being added
     * @param existingWindows The existing set of windows
     * @param deletionHandler A consumer for any existing tracker windows that should be deleted
     * @return The time window that should be added, it is optional because a new time window may already
     * be completely subsumed by an existing one.
     */
    private Optional<HAS_BOUNDS> mergeWindows(final HAS_BOUNDS windowToAdd,
                                            final List<HAS_BOUNDS> existingWindows,
                                            final Consumer<HAS_BOUNDS> deletionHandler) {
        HAS_BOUNDS windowToReturn = windowToAdd;

        for (final HAS_BOUNDS existingWindow : existingWindows) {
            final Optional<OverlapStyle> overlapStyle = determineOverlap(windowToReturn, existingWindow);

            // This would be odd...
            if (!overlapStyle.isPresent()) {
                continue;
            }

            switch (overlapStyle.get()) {
                case NEW_SUBSUMED_BY_EXISTING:
                    return Optional.empty();
                case EXISTING_SUBSUMED_BY_NEW:
                    deletionHandler.accept(existingWindow);
                    break;
                case OVERLAP_START:
                    deletionHandler.accept(existingWindow);
                    windowToReturn = newValueGenerator.apply(
                            existingWindow.getFrom(),
                            windowToReturn.getTo());
                    break;
                case OVERLAP_END:
                    deletionHandler.accept(existingWindow);
                    windowToReturn = newValueGenerator.apply(
                            windowToReturn.getFrom(),
                            existingWindow.getTo());
                    break;
            }
        }

        return Optional.of(windowToReturn);
    }

    Optional<OverlapStyle> determineOverlap(final HAS_BOUNDS newWindow,
                                         final HAS_BOUNDS existingWindow) {
        if (comparator.compare(existingWindow.getTo(), newWindow.getFrom()) < 0) {
            return OverlapStyle.NO_OVERLAP.opt();
        } else if (comparator.compare(existingWindow.getFrom(), newWindow.getTo()) > 0) {
            return OverlapStyle.NO_OVERLAP.opt();
        } else if (comparator.compare(existingWindow.getFrom(), newWindow.getFrom()) > 0) {
            if (comparator.compare(existingWindow.getTo(), newWindow.getTo()) < 0) {
                return OverlapStyle.EXISTING_SUBSUMED_BY_NEW.opt();
            } else if (comparator.compare(existingWindow.getTo(), newWindow.getTo()) > 0) {
                return OverlapStyle.OVERLAP_END.opt();
            }
        } else if (comparator.compare(existingWindow.getFrom(), newWindow.getFrom()) < 0) {
            if (comparator.compare(existingWindow.getTo(), newWindow.getTo()) < 0) {
                return OverlapStyle.OVERLAP_START.opt();
            } else if (comparator.compare(existingWindow.getTo(), newWindow.getTo()) > 0) {
                return OverlapStyle.NEW_SUBSUMED_BY_EXISTING.opt();
            }
        }

        // It really should hit one of the possibilities above
        return Optional.empty();
    }

    public enum OverlapStyle {
        NO_OVERLAP,
        OVERLAP_START,
        OVERLAP_END,
        NEW_SUBSUMED_BY_EXISTING,
        EXISTING_SUBSUMED_BY_NEW;

        public Optional<OverlapStyle> opt() {
            return Optional.of(this);
        }
    }

    public static <C, H extends HasBounds<C>> Builder<C, H>
    withValueGenerator(final BiFunction<C, C, H> newValueGenerator) {
        return new Builder<>(newValueGenerator);
    }

    public static class Builder<C, H extends HasBounds<C>> {
        private final BiFunction<C, C, H> newValueGenerator;
        private Comparator<C> comparator;

        private Builder(final BiFunction<C, C, H> newValueGenerator) {
            this.newValueGenerator = newValueGenerator;
        }

        public Builder<C, H> comparator(final Comparator<C> value) {
            this.comparator = value;
            return this;
        }

        public WindowMerger<C, H> build() {
            return new WindowMerger<>(this);
        }
    }
}
