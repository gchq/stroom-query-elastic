package stroom.autoindex.tracker;

/**
 * This service manages the persistence of tracking information. It delegates to a DAO
 * for the persistence itself, but it adds the logic that keeps the tracking windows
 * and timeline bounds consistent.
 */
public interface AutoIndexTrackerService {
    /**
     * Retrieve or Create the tracking information for a given doc ref UUID.
     *
     * @param docRefUuid The UUID of the Auto Index Doc Ref
     * @return The tracking information found
     */
    AutoIndexTracker get(final String docRefUuid);

    /**
     * Sets the timeline bounds for a particular auto index doc ref.
     * @param docRefUuid The UUID of the Auto Index Doc Ref
     * @param timelineBounds The bounds to apply as the timeline of the underlying raw data source
     * @return The updated Auto Index Tracker
     */
    AutoIndexTracker setTimelineBounds(final String docRefUuid, final TrackerWindow timelineBounds);

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
