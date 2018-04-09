package stroom.autoindex.tracker;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Each instance shows which time windows have been indexed from the raw data source into the indexed data source.
 */
public class AutoIndexTracker {
    public static final String TRACKER_WINDOW_TABLE_NAME = "tracker_window";
    public static final String TIMELINE_BOUNDS_TABLE_NAME = "timeline_bounds";

    /**
     * Special value of timeline that goes from min/max long values.
     */
    public static final TrackerWindow DEFAULT_TIMELINE_BOUNDS = TrackerWindow.from(Long.MIN_VALUE).to(Long.MAX_VALUE);

    private final String docRefUuid;
    private TrackerWindow timelineBounds;
    private final List<TrackerWindow> windows = new ArrayList<>();

    public static AutoIndexTracker forDocRef(final String docRefUuid) {
        return new AutoIndexTracker(docRefUuid);
    }

    public static AutoIndexTracker forBase(final AutoIndexTracker base) {
        return new AutoIndexTracker(base.getDocRefUuid())
                .withBounds(base.getTimelineBounds());
    }

    public AutoIndexTracker(final String docRefUuid) {
        this.docRefUuid = docRefUuid;
    }

    public AutoIndexTracker withBounds(final TrackerWindow timelineBounds) {
        this.timelineBounds = timelineBounds;
        return this;
    }

    public AutoIndexTracker withWindows(final Collection<TrackerWindow> windows) {
        this.windows.addAll(windows);
        return this;
    }

    public AutoIndexTracker withWindow(final TrackerWindow window) {
        this.windows.add(window);
        return this;
    }

    public String getDocRefUuid() {
        return docRefUuid;
    }

    public TrackerWindow getTimelineBounds() {
        return Optional.ofNullable(timelineBounds).orElse(DEFAULT_TIMELINE_BOUNDS);
    }

    /**
     * Getter for windows
     * @return a copied list of the windows
     */
    public List<TrackerWindow> getWindows() {
        return Collections.unmodifiableList(this.windows);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AutoIndexTracker{");
        sb.append("docRefUuid='").append(docRefUuid).append('\'');
        sb.append(", timelineBounds=").append(getTimelineBounds());
        sb.append(", windows=").append(getWindows());
        sb.append('}');
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AutoIndexTracker that = (AutoIndexTracker) o;
        return Objects.equals(docRefUuid, that.docRefUuid) &&
                Objects.equals(timelineBounds, that.timelineBounds) &&
                Objects.equals(getWindows(), that.getWindows());
    }

    @Override
    public int hashCode() {
        return Objects.hash(docRefUuid, windows);
    }
}
