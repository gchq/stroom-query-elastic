package stroom.autoindex.tracker;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

public class AutoIndexTrackerDaoTestImpl implements AutoIndexTrackerDao<Integer> {
    private final AtomicInteger txValue = new AtomicInteger();
    private final ConcurrentHashMap<String, AutoIndexTracker> trackers = new ConcurrentHashMap<>();

    @Override
    public AutoIndexTracker transactionResult(final BiFunction<AutoIndexTrackerDao<Integer>, Integer, AutoIndexTracker> txFunction) {
        return txFunction.apply(this, txValue.getAndIncrement());
    }

    @Override
    public void transaction(final BiConsumer<AutoIndexTrackerDao<Integer>, Integer> txFunction) {
        txFunction.accept(this, txValue.getAndIncrement());
    }

    @Override
    public AutoIndexTracker get(final Integer transaction,
                                final String docRefUuid) {
        return AutoIndexTracker.copy(trackers.computeIfAbsent(docRefUuid, AutoIndexTracker::forDocRef));
    }

    @Override
    public void insertTracker(final Integer transaction,
                              final String docRefUuid,
                              final TrackerWindow tw) {
        trackers.computeIfAbsent(docRefUuid, AutoIndexTracker::forDocRef)
                .withWindow(tw);
    }

    @Override
    public void deleteTracker(final Integer transaction,
                              final String docRefUuid,
                              final TrackerWindow tw) {
        final AutoIndexTracker replacement = AutoIndexTracker
                .forDocRef(docRefUuid);

        if (trackers.containsKey(docRefUuid)) {
            final AutoIndexTracker existing = trackers.get(docRefUuid);
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
        trackers.computeIfAbsent(docRefUuid, AutoIndexTracker::forDocRef)
                .withBounds(timelineBounds);
    }
}
