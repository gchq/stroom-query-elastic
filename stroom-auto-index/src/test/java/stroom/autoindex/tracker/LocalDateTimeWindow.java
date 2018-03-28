package stroom.autoindex.tracker;

import java.time.LocalDateTime;
import java.util.Objects;

public class LocalDateTimeWindow implements HasBounds<LocalDateTime> {
    private final LocalDateTime from;
    private final LocalDateTime to;

    private LocalDateTimeWindow(final LocalDateTime from, final LocalDateTime to) {
        this.from = from;
        this.to = to;
    }

    @Override
    public LocalDateTime getFrom() {
        return from;
    }

    @Override
    public LocalDateTime getTo() {
        return to;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("LocalDateTimeWindow{");
        sb.append("from=").append(from);
        sb.append(", to=").append(to);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LocalDateTimeWindow that = (LocalDateTimeWindow) o;
        return Objects.equals(from, that.from) &&
                Objects.equals(to, that.to);
    }

    @Override
    public int hashCode() {
        return Objects.hash(from, to);
    }

    static Builder from(final LocalDateTime from) {
        return new LocalDateTimeWindow.Builder(from);
    }

    static class Builder {
        final LocalDateTime from;

        private Builder(final LocalDateTime from) {
            this.from = from;
        }

        LocalDateTimeWindow to(final LocalDateTime to) {
            return new LocalDateTimeWindow(this.from, to);
        }
    }
}
