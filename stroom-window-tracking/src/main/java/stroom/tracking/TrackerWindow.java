package stroom.tracking;

import java.util.Objects;
import java.util.Optional;

/**
 * Each instance encapsulates a single window of time for which data
 * has been extracted from the raw data source into the index source.
 */
public class TrackerWindow implements HasBounds<Long> {
    private final Long from;
    private final Long to;

    private TrackerWindow(final Long from,
                          final Long to) {
        this.from = from;
        this.to = to;
    }

    @Override
    public Long getFrom() {
        return from;
    }

    @Override
    public Long getTo() {
        return to;
    }

    public Long size() {
        return to - from;
    }

    public boolean isBound() {
        return Optional.ofNullable(this.from).isPresent()
                && Optional.ofNullable(this.to).isPresent();
    }

    public static TrackerWindow copy(final TrackerWindow original) {
        if (null != original) {
            return from(original.getFrom()).to(original.getTo());
        }
        return null;
    }

    public static Builder from(final Long from) {
        return new Builder(from);
    }

    public static class Builder {
        private Long from;

        private Builder(final Long from) {
            this.from = from;
        }

        public TrackerWindow to(final Long to) {
            if (to <= this.from) {
                throw new IllegalArgumentException("The FROM date must be before the TO");
            }
            return new TrackerWindow(this.from, to);
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("TrackerWindow{");
        sb.append("from=").append(from);
        sb.append(", to=").append(to);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TrackerWindow that = (TrackerWindow) o;
        return Objects.equals(from, that.from) &&
                Objects.equals(to, that.to);
    }

    @Override
    public int hashCode() {
        return Objects.hash(from, to);
    }
}
