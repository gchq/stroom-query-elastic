package stroom.tracking;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Each instance shows which time windows have been indexed from the raw data source into the indexed data source.
 */
public class TimelineTracker {
    public static final String TRACKER_WINDOW_TABLE_NAME = "tracker_window";
    public static final String TIMELINE_BOUNDS_TABLE_NAME = "timeline_bounds";

    private final String docRefUuid;
    private TrackerWindow timelineBounds;
    private final List<TrackerWindow> windows = new ArrayList<>();

    public static TimelineTracker forDocRef(final String docRefUuid) {
        return new TimelineTracker(docRefUuid);
    }

    public static TimelineTracker forBase(final TimelineTracker base) {
        return new TimelineTracker(base.getDocRefUuid())
                .withBounds(base.getTimelineBounds()
                        .map(TrackerWindow::copy)
                        .orElse(null));
    }

    public static TimelineTracker copy(final TimelineTracker original) {
        return forBase(original)
                .withWindows(original.getWindows().stream()
                        .map(TrackerWindow::copy)
                        .sorted(Comparator.comparing(TrackerWindow::getFrom))
                        .collect(Collectors.toList()));
    }

    public TimelineTracker(final String docRefUuid) {
        this.docRefUuid = docRefUuid;
    }

    public TimelineTracker withBounds(final TrackerWindow timelineBounds) {
        this.timelineBounds = timelineBounds;
        return this;
    }

    public TimelineTracker withWindows(final Collection<TrackerWindow> windows) {
        this.windows.addAll(windows);
        return this;
    }

    public TimelineTracker withWindow(final TrackerWindow... window) {
        return withWindows(Arrays.asList(window));
    }

    public String getDocRefUuid() {
        return docRefUuid;
    }

    public Optional<TrackerWindow> getTimelineBounds() {
        return Optional.ofNullable(timelineBounds);
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
        final StringBuilder sb = new StringBuilder("TimelineTracker{");
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
        TimelineTracker that = (TimelineTracker) o;
        return Objects.equals(docRefUuid, that.docRefUuid) &&
                Objects.equals(timelineBounds, that.timelineBounds) &&
                Objects.equals(getWindows(), that.getWindows());
    }

    @Override
    public int hashCode() {
        return Objects.hash(docRefUuid, windows);
    }
}
