package stroom.autoindex.tracker;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This class takes a list of time windows, and suggests the next one to fill in the timeline.
 *
 * It requires a separate built instance for each run of the algorithm, starting with the
 * NextWindowSelector.fromNow function.
 *
 * This algorithm will work backwards through time, it will only suggest windows from the most recent epoch, given the window size.
 *
 * Example: Time now is 4:15, window size is 1 hour
 *              first window will be 3:00 - 4:00
 *              next one will be 2:00 - 3:00
 * Example: Time now is 13:00 on Wednesday 5th March, window size is 1 day
 *              first window will be 00:00 Tuesday 4th March to 00:00 Wednesday 5th March
 *              next one will be 00:00 Monday 3rd March to 00:00 Tuesday 4th March
 *
 * If there are awkward gaps in the windows, it will attempt to fill them in, it will suggest windows
 * that are neatly bound to the epoch of the window size. See the tests for examples.
 */
public class NextWindowSelector {
    private static final Logger LOGGER = LoggerFactory.getLogger(NextWindowSelector.class);

    private TrackerWindow timelineBounds;

    private long windowSize = 1;

    private final List<TrackerWindow> existingWindows = new ArrayList<>();

    private NextWindowSelector(final TrackerWindow timelineBounds) {
        this.timelineBounds = timelineBounds;
    }

    public NextWindowSelector existingWindows(final List<TrackerWindow> values) {
        this.existingWindows.addAll(values);
        return this;
    }

    public NextWindowSelector existingWindows(final TrackerWindow...values) {
        return this.existingWindows(Arrays.asList(values));
    }

    public NextWindowSelector windowSize(final long value) {
        this.windowSize = value;
        return this;
    }

    public Optional<TrackerWindow> suggestNextWindow() {
        final List<TrackerWindow> reversedWindows = Lists.reverse(
                this.existingWindows.stream()
                        .sorted(Comparator.comparing(TrackerWindow::getFrom))
                        .collect(Collectors.toList()));

        // Start iterating through the list backwards, cut to the last epoch
        final Long mostRecentEpoch = this.timelineBounds.getTo() - (this.timelineBounds.getTo() % this.windowSize);

        LOGGER.trace("Suggest Next Window - Existing: {}, Size: {}, Now: {}, Timeline Bounds: {}",
                this.existingWindows.size(),
                this.windowSize,
                this.timelineBounds,
                mostRecentEpoch);

        final Optional<TrackerWindow> window = tryNextWindow(reversedWindows.iterator(), mostRecentEpoch);
        LOGGER.trace("Returning Window {}", window);
        return window;
    }

    public Stream<TrackerWindow> suggestNextWindows(final int count) {
        final Optional<TrackerWindow> firstWindow = this.suggestNextWindow();
        if (firstWindow.isPresent()) {
            return Stream.iterate(firstWindow,
                    tw -> {
                        if (tw.isPresent()) {
                            return this.existingWindows(tw.get()).suggestNextWindow();
                        } else {
                            return Optional.empty();
                        }
                    })
                    .limit(count)
                    .filter(Optional::isPresent)
                    .map(Optional::get);
        } else {
            return Stream.empty();
        }
    }

    public static NextWindowSelector withBounds(final TrackerWindow timelineBounds) {
        return new NextWindowSelector(timelineBounds);
    }

    /**
     * Recurse backwards through the tracker windows looking for a gap
     * @param iter An iterator that works on a backwards ordered list of time windows.
     * @param currentTop The current timestamp we are windowing to. if there are gaps before this time, the window
     *                   returned will attempt to fill it.
     * @return The tracker window required to fill the next gap found looking backwards through time
     *          The return value is optional, if we have reached the lower bounds of the timeline, it should stop
     */
    private Optional<TrackerWindow> tryNextWindow(final Iterator<TrackerWindow> iter,
                                        final Long currentTop) {
        if (currentTop <= timelineBounds.getFrom()) {
            return Optional.empty();
        }
        if (iter.hasNext()) {
            final TrackerWindow tw = iter.next();

            if (tw.getTo() >= currentTop) {
                return tryNextWindow(iter, tw.getFrom());
            } else {
                final Long potentialFrom = goBackToNextFrom(currentTop);
                if (potentialFrom > tw.getTo()) {
                    return Optional.of(TrackerWindow.from(potentialFrom).to(currentTop));
                } else {
                    return Optional.of(TrackerWindow.from(tw.getTo()).to(currentTop));
                }
            }
        } else {
            // We have run out of windows, going backwards, so just create one that is the window size
            // ending at the current top seconds
            return Optional.of(TrackerWindow
                    .from(goBackToNextFrom(currentTop))
                    .to(currentTop));
        }
    }

    /**
     * Goes back a window size, then truncates that to the nearest epoch going backwards
     * @param currentTop The current top value, we are going backwards from that.
     * @return Either the current top MINUS the window size OR the next epoch in the backwards direction)
     */
    private Long goBackToNextFrom(final Long currentTop) {
        Long potentialFrom = currentTop - this.windowSize;

        // Go back to most recent epoch from our current top
        final Long epochBackFromTop = currentTop - (currentTop % this.windowSize);

        // If our current top is not on a neat epoch, we will want to go from there instead
        if (epochBackFromTop < currentTop) {
            potentialFrom = epochBackFromTop;
        }

        return potentialFrom;
    }
}
