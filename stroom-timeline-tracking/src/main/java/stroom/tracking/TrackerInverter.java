package stroom.tracking;

import java.util.concurrent.atomic.AtomicLong;

public class TrackerInverter {
    private final TimelineTracker tracker;

    public static TrackerInverter withTracker(final TimelineTracker tracker) {
        return new TrackerInverter(tracker);
    }

    private TrackerInverter(TimelineTracker tracker) {
        this.tracker = tracker;
    }

    public TimelineTracker invert() {
        final TimelineTracker inverted = TimelineTracker.forBase(tracker);

        final TrackerWindow timelineBounds = tracker.getTimelineBounds()
                .orElseThrow(() -> new RuntimeException("Cannot invert a timeline that has no bounds"));

        final AtomicLong currentFrom = new AtomicLong(timelineBounds.getFrom());
        tracker.getWindows().forEach(tw -> {
            // If there is some gap between our current from and this window from, fill that in
            if (currentFrom.longValue() < tw.getFrom()) {
                inverted.withWindow(TrackerWindow
                        .from(currentFrom.longValue())
                        .to(tw.getFrom())
                );
            }
            currentFrom.set(tw.getTo());
        });

        if (currentFrom.longValue() < timelineBounds.getTo()) {
            inverted.withWindow(TrackerWindow
                    .from(currentFrom.longValue())
                    .to(timelineBounds.getTo())
            );
        }

        return inverted;
    }
}
