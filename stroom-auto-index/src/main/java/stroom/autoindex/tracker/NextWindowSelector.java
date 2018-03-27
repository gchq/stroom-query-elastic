package stroom.autoindex.tracker;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static stroom.autoindex.TimeUtils.dateTimeFromLong;
import static stroom.autoindex.TimeUtils.getEpochSeconds;

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

    private final LocalDateTime now;

    private int windowSizeAmount = 1;

    private ChronoUnit windowSizeUnit;

    private final List<TrackerWindow> existingWindows = new ArrayList<>();

    private NextWindowSelector(final LocalDateTime now) {
        this.now = now;
    }

    public NextWindowSelector existingWindows(final List<TrackerWindow> values) {
        this.existingWindows.addAll(values);
        return this;
    }

    public NextWindowSelector existingWindows(final TrackerWindow...values) {
        return this.existingWindows(Arrays.asList(values));
    }

    public NextWindowSelector windowSizeAmount(final int value) {
        this.windowSizeAmount = value;
        return this;
    }

    public NextWindowSelector windowSizeUnit(final ChronoUnit value) {
        this.windowSizeUnit = value;
        return this;
    }

    public TrackerWindow suggestNextWindow() {
        Objects.requireNonNull(this.windowSizeUnit, "Must specify window size");

        final List<TrackerWindow> reversedWindows = Lists.reverse(
                this.existingWindows.stream()
                        .sorted(Comparator.comparing(TrackerWindow::getFrom))
                        .collect(Collectors.toList()));

        // Start iterating through the list backwards, cut to the last epoch
        final LocalDateTime mostRecentEpoch = findRecentEpoch(this.now);

        LOGGER.trace("Suggest Next Window - Existing: {}, Size: {} {}, Now: {}, mostRecentEpoch: {}",
                this.existingWindows.size(),
                this.windowSizeAmount,
                this.windowSizeUnit,
                this.now,
                mostRecentEpoch);

        final TrackerWindow window = tryNextWindow(reversedWindows.iterator(), mostRecentEpoch);
        LOGGER.trace("Returning Window {}", window);
        return window;
    }

    public Stream<TrackerWindow> suggestNextWindows(final int count) {
        return Stream.iterate(this.suggestNextWindow(),
                tw -> this.existingWindows(tw).suggestNextWindow()).limit(count);
    }

    public static NextWindowSelector fromNow(final LocalDateTime now) {
        return new NextWindowSelector(now);
    }

    /**
     * Internal utility function to round a date down to nearest epoch according to the windowSize.
     * @param value The date time to round down.
     * @return The rounded date time.
     */
    private LocalDateTime findRecentEpoch(final LocalDateTime value) {
        switch(this.windowSizeUnit) {
            case YEARS: {
                int year = value.getYear();
                year = year - (year % this.windowSizeAmount);
                return LocalDateTime.of(year, 1, 1, 0, 0);
            }
            case MONTHS: {
                int month = value.getMonthValue() - 1;
                month = month - (month % this.windowSizeAmount);
                return LocalDateTime.of(value.getYear(), month + 1, 1, 0, 0);
            }
            case DAYS:
            case HOURS:
            case MINUTES:{
                final Long valueSeconds = getEpochSeconds(value);
                final TemporalAmount duration = Duration.of(this.windowSizeAmount, this.windowSizeUnit);
                final Long windowSizeSeconds = duration.get(ChronoUnit.SECONDS);
                return dateTimeFromLong(valueSeconds - (valueSeconds % windowSizeSeconds));
            }
        }

        throw new IllegalArgumentException("Cannot support window size unit " + this.windowSizeUnit);
    }

    /**
     * Recurse backwards through the tracker windows looking for a gap
     * @param iter An iterator that works on a backwards ordered list of time windows.
     * @param currentTop The current timestamp we are windowing to. if there are gaps before this time, the window
     *                   returned will attempt to fill it.
     * @return The tracker window required to fill the next gap found looking backwards through time
     */
    private TrackerWindow tryNextWindow(final Iterator<TrackerWindow> iter,
                                        final LocalDateTime currentTop) {

        if (iter.hasNext()) {
            final TrackerWindow tw = iter.next();

            if (!tw.getTo().isBefore(currentTop)) {
                return tryNextWindow(iter, tw.getFrom());
            } else {
                LocalDateTime potentialFrom = currentTop.minus(this.windowSizeAmount, this.windowSizeUnit);

                // Go back to most recent epoch from our current top
                final LocalDateTime epochBackFromTop = findRecentEpoch(currentTop);

                // If our current top is not on a neat epoch, we will want to go from there instead
                if (epochBackFromTop.isBefore(currentTop)) {
                    potentialFrom = epochBackFromTop;
                }

                if (potentialFrom.isAfter(tw.getTo())) {
                    return TrackerWindow.from(potentialFrom).to(currentTop);
                } else {
                    return TrackerWindow.from(tw.getTo()).to(currentTop);
                }
            }
        } else {
            // We have run out of windows, going backwards, so just create one that is the window size
            // ending at the current top seconds
            return TrackerWindow
                    .from(currentTop.minus(this.windowSizeAmount, this.windowSizeUnit))
                    .to(currentTop);
        }

    }
}
