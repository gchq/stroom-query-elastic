package stroom.tracking;

import javax.inject.Inject;
import java.util.Optional;

public class TimelineTrackerServiceImpl implements TimelineTrackerService {
    private static final WindowMerger<Long, TrackerWindow> longTimeMerger =
            WindowMerger.<Long, TrackerWindow>withValueGenerator((from, to) -> TrackerWindow.from(from).to(to))
                    .comparator(Long::compareTo)
                    .build();

    private final TimelineTrackerDao<?> timelineTrackerDao;

    @Inject
    public TimelineTrackerServiceImpl(final TimelineTrackerDao timelineTrackerDao) {
        this.timelineTrackerDao = timelineTrackerDao;
    }

    @Override
    public TimelineTracker get(final String docRefUuid) {
        return timelineTrackerDao.transactionResult((d, c) -> d.get(c, docRefUuid));
    }

    @Override
    public TimelineTracker setTimelineBounds(final String docRefUuid,
                                             final TrackerWindow timelineBounds) {
        return timelineTrackerDao.transactionResult((d, c) -> {
            d.setTimelineBounds(c, docRefUuid, timelineBounds);
            return d.get(c, docRefUuid);
        });
    }

    @Override
    public TimelineTracker addWindow(final String docRefUuid,
                                     final TrackerWindow window) {
        return timelineTrackerDao.transactionResult((d, c) -> {
            final TimelineTracker current = d.get(c, docRefUuid);

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
    public TimelineTracker clearWindows(final String docRefUuid) {
        return timelineTrackerDao.transactionResult((d, c) -> {
            final TimelineTracker current = d.get(c, docRefUuid);
            current.getWindows().forEach(tw -> d.deleteTracker(c, docRefUuid, tw));
            return d.get(c, docRefUuid);
        });
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("TimelineTrackerServiceImpl{");
        sb.append("timelineTrackerDao=").append(timelineTrackerDao);
        sb.append('}');
        return sb.toString();
    }
}
