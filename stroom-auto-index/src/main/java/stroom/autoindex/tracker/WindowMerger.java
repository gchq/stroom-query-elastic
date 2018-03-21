package stroom.autoindex.tracker;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A class that can be used to determine overlaps between windows of any type and merge them down
 * to a reduced set of windows that cover the same space.
 * @param <CONTAINER> The class that contains the comparables.
 * @param <COMPARABLE> The type of the property that defines the window
 */
public class WindowMerger<CONTAINER, COMPARABLE> {

    private final Comparator<COMPARABLE> comparator;
    private final Function<CONTAINER, COMPARABLE> fromSupplier;
    private final Function<CONTAINER, COMPARABLE> toSupplier;
    private final BiFunction<COMPARABLE, COMPARABLE, CONTAINER> newValueGenerator;

    private WindowMerger(final Builder<CONTAINER, COMPARABLE> builder) {
        this.comparator = builder.comparator;
        this.newValueGenerator = builder.newValueGenerator;
        this.fromSupplier = builder.fromSupplier;
        this.toSupplier = builder.toSupplier;
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
    public Optional<CONTAINER> mergeWindows(final CONTAINER windowToAdd,
                                               final List<CONTAINER> existingWindows,
                                               final Consumer<CONTAINER> deletionHandler) {
        CONTAINER windowToReturn = windowToAdd;

        for (final CONTAINER existingWindow : existingWindows) {
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
                            fromSupplier.apply(existingWindow),
                            toSupplier.apply(windowToReturn));
                    break;
                case OVERLAP_END:
                    deletionHandler.accept(existingWindow);
                    windowToReturn = newValueGenerator.apply(
                            fromSupplier.apply(windowToReturn),
                            toSupplier.apply(existingWindow));
                    break;
            }
        }

        return Optional.of(windowToReturn);
    }

    Optional<OverlapStyle> determineOverlap(final CONTAINER newWindow,
                                         final CONTAINER existingWindow) {
        if (comparator.compare(toSupplier.apply(existingWindow), fromSupplier.apply(newWindow)) < 0) {
            return OverlapStyle.NO_OVERLAP.opt();
        } else if (comparator.compare(fromSupplier.apply(existingWindow), toSupplier.apply(newWindow)) > 0) {
            return OverlapStyle.NO_OVERLAP.opt();
        } else if (comparator.compare(fromSupplier.apply(existingWindow), fromSupplier.apply(newWindow)) > 0) {
            if (comparator.compare(toSupplier.apply(existingWindow), toSupplier.apply(newWindow)) < 0) {
                return OverlapStyle.EXISTING_SUBSUMED_BY_NEW.opt();
            } else if (comparator.compare(toSupplier.apply(existingWindow), toSupplier.apply(newWindow)) > 0) {
                return OverlapStyle.OVERLAP_END.opt();
            }
        } else if (comparator.compare(fromSupplier.apply(existingWindow), fromSupplier.apply(newWindow)) < 0) {
            if (comparator.compare(toSupplier.apply(existingWindow), toSupplier.apply(newWindow)) < 0) {
                return OverlapStyle.OVERLAP_START.opt();
            } else if (comparator.compare(toSupplier.apply(existingWindow), toSupplier.apply(newWindow)) > 0) {
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

    public static <C, T> Builder<C, T>
    withValueGenerator(final BiFunction<T, T, C> newValueGenerator) {
        return new Builder<>(newValueGenerator);
    }

    public static class Builder<C, T> {
        private final BiFunction<T, T, C> newValueGenerator;
        private Comparator<T> comparator;
        private Function<C, T> fromSupplier;
        private Function<C, T> toSupplier;

        private Builder(final BiFunction<T, T, C> newValueGenerator) {
            this.newValueGenerator = newValueGenerator;
        }

        public Builder<C, T> comparator(final Comparator<T> value) {
            this.comparator = value;
            return this;
        }

        public Builder<C, T> from(final Function<C, T> value) {
            this.fromSupplier = value;
            return this;
        }

        public Builder<C, T> to(final Function<C, T> value) {
            this.toSupplier = value;
            return this;
        }

        public WindowMerger<C, T> build() {
            Objects.requireNonNull(this.newValueGenerator);
            Objects.requireNonNull(this.comparator);
            Objects.requireNonNull(this.fromSupplier);
            Objects.requireNonNull(this.toSupplier);

            return new WindowMerger<>(this);
        }
    }
}
