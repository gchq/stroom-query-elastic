package stroom.autoindex.tracker;

import java.util.Objects;

/**
 * Simple window using integers, for testing classes based on HasBounds
 */
class IntegerWindow implements HasBounds<Integer> {
    private final Integer from;
    private final Integer to;

    IntegerWindow(final Integer from, final Integer to) {
        this.from = from;
        this.to = to;
    }

    @Override
    public Integer getFrom() {
        return from;
    }

    @Override
    public Integer getTo() {
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

    static Builder from(final Integer from) {
        return new Builder(from);
    }

    static class Builder {
        final Integer from;

        private Builder(final Integer from) {
            this.from = from;
        }

        IntegerWindow to(final Integer to) {
            return new IntegerWindow(this.from, to);
        }
    }
}
