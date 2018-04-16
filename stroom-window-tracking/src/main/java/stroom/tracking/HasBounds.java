package stroom.tracking;

import java.util.Comparator;

/**
 * General form of an interface that defines boundaries.
 *
 * Primarily used for LocalDateTime, but can work with any class that is comparable.
 *
 * @param <T> The type of data that defines the boundaries.
 */
public interface HasBounds<T> {
    T getFrom();
    T getTo();

    /**
     * Indicates if a value is within the window, inclusive start, exclusive at end.
     * @param value The value to check
     * @param comparator A comparator to use between the value and the to/from values.
     * @return True if the value is within the window (from inclusive, to exclusive)
     */
    default boolean isInside(final T value, final Comparator<T> comparator) {
        return (comparator.compare(getFrom(), value) <= 0)
                && (comparator.compare(getTo(), value) > 0);
    }

    /**
     * A more precise definition of a value being inside a window.
     */
    enum Inside {
        LOWER,
        BOTTOM_EDGE,
        WITHIN,
        TOP_EDGE,
        HIGHER
    }

    /**
     * Given a value, works out the precise relationship between the value and the window.
     * @param value The value to compare
     * @param comparator The comparator to apply to the value and the to/from values.
     * @return The precise indication of the values place relative to the window.
     */
    default Inside checkInside(final T value, final Comparator<T> comparator) {
        int compareFrom = comparator.compare(getFrom(), value);
        int compareTo = comparator.compare(getTo(), value);

        if (compareFrom > 0) {
            return Inside.LOWER;
        } else if (compareFrom == 0) {
            return Inside.BOTTOM_EDGE;
        } else if (compareTo > 0) {
            return Inside.WITHIN;
        } else if (compareTo == 0) {
            return Inside.TOP_EDGE;
        } else {
            return Inside.HIGHER;
        }
    }
}
