package stroom.autoindex.tracker;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

public class TimelineTrackerDaoTestImpl implements TimelineTrackerDao<Integer> {
    private final AtomicInteger txValue = new AtomicInteger();
    private final ConcurrentHashMap<String, TimelineTracker> trackers = new ConcurrentHashMap<>();

    @Override
    public TimelineTracker transactionResult(final BiFunction<TimelineTrackerDao<Integer>, Integer, TimelineTracker> txFunction) {
        return txFunction.apply(this, txValue.getAndIncrement());
    }

    @Override
    public void transaction(final BiConsumer<TimelineTrackerDao<Integer>, Integer> txFunction) {
        txFunction.accept(this, txValue.getAndIncrement());
    }

    @Override
    public TimelineTracker get(final Integer transaction,
                               final String docRefUuid) {
        return TimelineTracker.copy(trackers.computeIfAbsent(docRefUuid, TimelineTracker::forDocRef));
    }

    @Override
    public void insertTracker(final Integer transaction,
                              final String docRefUuid,
                              final TrackerWindow tw) {
        trackers.computeIfAbsent(docRefUuid, TimelineTracker::forDocRef)
                .withWindow(tw);
    }

    @Override
    public void deleteTracker(final Integer transaction,
                              final String docRefUuid,
                              final TrackerWindow tw) {
        final TimelineTracker replacement = TimelineTracker
                .forDocRef(docRefUuid);

        if (trackers.containsKey(docRefUuid)) {
            final TimelineTracker existing = trackers.get(docRefUuid);
            existing.getTimelineBounds().ifPresent(replacement::withBounds);
            existing.getWindows().stream()
                    .filter(existingTw -> !tw.equals(existingTw))
                    .forEach(replacement::withWindow);
            trackers.replace(docRefUuid, replacement);
        } else {
            throw new AssertionError(String.format("Couldn't find Tracker to Delete %s", docRefUuid));
        }
    }

    @Override
    public void setTimelineBounds(final Integer transaction,
                                  final String docRefUuid,
                                  final TrackerWindow timelineBounds) {
        trackers.computeIfAbsent(docRefUuid, TimelineTracker::forDocRef)
                .withBounds(timelineBounds);
    }
}
