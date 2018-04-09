package stroom.autoindex.tracker;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class GapWindowSelector {
    private static final Logger LOGGER = LoggerFactory.getLogger(NextWindowSelector.class);

    private long windowSize = 1;

    private TrackerWindow timelineBounds;

    private final List<TrackerWindow> existingWindows = new ArrayList<>();

    private GapWindowSelector(final TrackerWindow timelineBounds) {
        this.timelineBounds = timelineBounds;
    }

    public GapWindowSelector existingWindows(final List<TrackerWindow> values) {
        this.existingWindows.addAll(values);
        return this;
    }

    public GapWindowSelector existingWindows(final TrackerWindow...values) {
        return this.existingWindows(Arrays.asList(values));
    }

    public GapWindowSelector windowSize(final long value) {
        this.windowSize = value;
        return this;
    }

    public static GapWindowSelector withTimelineBounds(final TrackerWindow timelineBounds) {
        return new GapWindowSelector(timelineBounds);
    }

    public List<TrackerWindow> getGaps() {
        final List<TrackerWindow> reversedWindows = Lists.reverse(
                this.existingWindows.stream()
                        .sorted(Comparator.comparing(TrackerWindow::getFrom))
                        .collect(Collectors.toList()));

        return Collections.emptyList();
    }
}
