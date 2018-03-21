package stroom.autoindex.tracker;

import java.time.LocalDateTime;
import java.util.Objects;
import java.util.Optional;

/**
 * Each instance encapsulates a single window of time for which data
 * has been extracted from the raw data source into the index source.
 */
public class TrackerWindow {
    private final LocalDateTime from;
    private final LocalDateTime to;

    private TrackerWindow(final LocalDateTime from,
                          final LocalDateTime to) {
        this.from = from;
        this.to = to;
    }

    public LocalDateTime getFrom() {
        return from;
    }

    public LocalDateTime getTo() {
        return to;
    }

    public boolean isBound() {
        return Optional.ofNullable(this.from).isPresent()
                && Optional.ofNullable(this.to).isPresent();
    }

    public static Builder from(final LocalDateTime from) {
        return new Builder(from);
    }

    public static class Builder {
        private LocalDateTime from;

        private Builder(final LocalDateTime from) {
            this.from = from;
        }

        public TrackerWindow to(final LocalDateTime to) {
            if (to.isBefore(this.from)) {
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
