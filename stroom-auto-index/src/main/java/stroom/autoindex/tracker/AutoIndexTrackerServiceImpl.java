package stroom.autoindex.tracker;

import javax.inject.Inject;
import java.util.Optional;

public class AutoIndexTrackerServiceImpl implements AutoIndexTrackerService {
    private static final WindowMerger<Long, TrackerWindow> longTimeMerger =
            WindowMerger.<Long, TrackerWindow>withValueGenerator((from, to) -> TrackerWindow.from(from).to(to))
                    .comparator(Long::compareTo)
                    .build();

    private final AutoIndexTrackerDao<?> autoIndexTrackerDao;

    @Inject
    public AutoIndexTrackerServiceImpl(final AutoIndexTrackerDao autoIndexTrackerDao) {
        this.autoIndexTrackerDao = autoIndexTrackerDao;
    }

    @Override
    public AutoIndexTracker get(final String docRefUuid) {
        return autoIndexTrackerDao.transactionResult((d, c) -> d.get(c, docRefUuid));
    }

    @Override
    public AutoIndexTracker setTimelineBounds(final String docRefUuid,
                                              final TrackerWindow timelineBounds) {
        return autoIndexTrackerDao.transactionResult((d, c) -> {
            d.setTimelineBounds(c, docRefUuid, timelineBounds);
            return d.get(c, docRefUuid);
        });
    }

    @Override
    public AutoIndexTracker addWindow(final String docRefUuid,
                                      final TrackerWindow window) {
        return autoIndexTrackerDao.transactionResult((d, c) -> {
            final AutoIndexTracker current = d.get(c, docRefUuid);

            // Attempt to merge this new window with any existing ones that can be replaced
            final Optional<TrackerWindow> windowToAdd = longTimeMerger.merge(window)
                    .with(current.getWindows())
                    .deleteWith(tw -> d.deleteTracker(c, docRefUuid, tw))
                    .execute();

            // If there is still a window to add, add it to the database
            windowToAdd.ifPresent(tw -> {
                d.insertTracker(c, docRefUuid, tw);

                final Optional<TrackerWindow> timelineBoundsOpt = current.getTimelineBounds();

                // Check that the timeline bounds are still sensible
                if (timelineBoundsOpt.isPresent()) {
                    final TrackerWindow timelineBounds = timelineBoundsOpt.get();

                    final HasBounds.Inside from = timelineBounds.checkInside(tw.getFrom(), Long::compareTo);
                    final HasBounds.Inside to = timelineBounds.checkInside(tw.getTo(), Long::compareTo);

                    final Long newFrom = HasBounds.Inside.LOWER.equals(from) ? tw.getFrom() : null;
                    final Long newTo = HasBounds.Inside.HIGHER.equals(to) ? tw.getTo() : null;

                    if ((null != newFrom) || (null != newTo)) {
                        final TrackerWindow newTimelineBounds = TrackerWindow
                                .from(newFrom != null ? newFrom : timelineBounds.getFrom())
                                .to(newTo != null ? newTo : timelineBounds.getTo());
                        d.setTimelineBounds(c, docRefUuid, newTimelineBounds);
                    }
                } else {
                    // Set the bounds to this window
                    d.setTimelineBounds(c, docRefUuid, tw);
                }
            });



            return d.get(c, docRefUuid);
        });
    }

    @Override
    public AutoIndexTracker clearWindows(final String docRefUuid) {
        return autoIndexTrackerDao.transactionResult((d, c) -> {
            final AutoIndexTracker current = d.get(c, docRefUuid);
            current.getWindows().forEach(tw -> d.deleteTracker(c, docRefUuid, tw));
            return d.get(c, docRefUuid);
        });
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AutoIndexTrackerServiceImpl{");
        sb.append("autoIndexTrackerDao=").append(autoIndexTrackerDao);
        sb.append('}');
        return sb.toString();
    }
}
