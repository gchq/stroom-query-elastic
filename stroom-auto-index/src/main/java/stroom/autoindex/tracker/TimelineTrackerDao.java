package stroom.autoindex.tracker;

import java.util.function.BiConsumer;
import java.util.function.BiFunction;

/**
 * Interface for persisting the auto index tracking information.
 * This interface just saves and loads information, for window merging and timeline bound
 * management, check {@link TimelineTrackerService} which will generally wrap this class.
 *
 * @param <TX> The class that represents a transaction.
 */
public interface TimelineTrackerDao<TX> {

    /**
     * The caller should call this function to start a transaction, they can supply a function
     * that then executes all the operations in a transaction.
     *
     * @param txFunction The function supplied that will then make further calls on us.
     * @return The current state of the TimelineTracker being modified.
     */
    TimelineTracker transactionResult(
            BiFunction<TimelineTrackerDao<TX>, TX, TimelineTracker> txFunction);

    /**
     * The caller should call this function to start a transaction, they can supply a function
     * that then executes all the operations in a transaction. This version doesn't return anything.
     *
     * @param txFunction The function supplied that will then make further calls on us.
     */
    void transaction(BiConsumer<TimelineTrackerDao<TX>, TX> txFunction);

    /**
     * Simply return the current state of the tracking information.
     *
     * @param transaction The current transaction
     * @param docRefUuid  The UUId of the Doc Ref to return tracking information.
     * @return The current state of the tracking information.
     */
    TimelineTracker get(TX transaction,
                        String docRefUuid);

    /**
     * Adds a window to the current tracking information.
     *
     * @param transaction The current transaction
     * @param docRefUuid  The UUID of the Doc Ref to return tracking information.
     * @param tw          The window to add
     */
    void insertTracker(TX transaction, String docRefUuid, TrackerWindow tw);

    /**
     * Deletes a window from the current tracking information.
     *
     * @param transaction The current transaction
     * @param docRefUuid  The UUID of the Doc Ref to return tracking information.
     * @param tw          The window to delete
     */
    void deleteTracker(TX transaction, String docRefUuid, TrackerWindow tw);

    /**
     * Update the timeline bounds of a bit of tracking information.
     * @param transaction The current transaction
     * @param docRefUuid  The UUID of the Doc Ref to modify.
     * @param timelineBounds The new values of the bounds.
     */
    void setTimelineBounds(TX transaction, String docRefUuid, TrackerWindow timelineBounds);
}
