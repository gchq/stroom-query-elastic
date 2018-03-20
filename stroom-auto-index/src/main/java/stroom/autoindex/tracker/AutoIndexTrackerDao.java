package stroom.autoindex.tracker;

/**
 * Interface for interacting with the auto index tracking information
 */
public interface AutoIndexTrackerDao {
    /**
     * Retrieve or Create the tracking information for a given doc ref UUID.
     *
     * @param docRefUuid The UUID of the Auto Index Doc Ref
     * @return The tracking information found
     */
    AutoIndexTracker get(final String docRefUuid);

    /**
     * Add a tracked window for a given Doc Ref UUID.
     *
     * @param docRefUuid The UUID of the Auto Index Doc Ref
     * @param window The window to add
     * @return The total updated tracking information
     */
    AutoIndexTracker addWindow(final String docRefUuid, final TrackerWindow window);

    /**
     * Used to explicitly clear down any existing time windows for a given tracker.
     * It will leave the tracker in place, but it will be empty.
     *
     * @param docRefUuid The UUID of the Auto Index Doc Ref
     * @return The updated tracking information (should be empty)
     */
    AutoIndexTracker clearWindows(final String docRefUuid);
}
