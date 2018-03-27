package stroom.autoindex.tracker;

import java.util.*;

/**
 * Each instance shows which time windows have been indexed from the raw data source into the indexed data source.
 */
public class AutoIndexTracker {
    public static final String TABLE_NAME = "tracker_window";

    private final String docRefUuid;
    private final List<TrackerWindow> windows = new ArrayList<>();

    public static AutoIndexTracker forDocRef(final String docRefUuid) {
        return new AutoIndexTracker(docRefUuid);
    }

    public AutoIndexTracker(final String docRefUuid) {
        this.docRefUuid = docRefUuid;
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
                Objects.equals(getWindows(), that.getWindows());
    }

    @Override
    public int hashCode() {
        return Objects.hash(docRefUuid, windows);
    }
}
