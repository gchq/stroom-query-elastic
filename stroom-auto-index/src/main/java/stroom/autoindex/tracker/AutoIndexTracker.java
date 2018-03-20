package stroom.autoindex.tracker;

import stroom.query.api.v2.DocRef;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    public static AutoIndexTracker forDocRef(final DocRef docRef) {
        return forDocRef(docRef.getUuid());
    }

    public AutoIndexTracker(final String docRefUuid) {
        this.docRefUuid = docRefUuid;
    }

    public AutoIndexTracker withWindows(final Collection<TrackerWindow> windows) {
        this.windows.addAll(windows);
        return this;
    }

    public String getDocRefUuid() {
        return docRefUuid;
    }

    /**
     * Getter for windows
     * @return a copied sorted list of the windows, sorted by 'from' time
     */
    public List<TrackerWindow> getWindows() {
        return this.windows.stream()
                .sorted(Comparator.comparing(TrackerWindow::getFrom))
                .collect(Collectors.toList());
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
