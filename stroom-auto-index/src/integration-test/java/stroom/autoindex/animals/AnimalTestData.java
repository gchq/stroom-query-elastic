package stroom.autoindex.animals;

import stroom.autoindex.animals.app.AnimalSighting;
import stroom.autoindex.tracker.TrackerWindow;
import stroom.testdata.DataGenerator;
import stroom.testdata.FlatDataWriterBuilder;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

public class AnimalTestData implements Consumer<Consumer<String>> {

    public static final TrackerWindow TIMELINE_BOUNDS = TrackerWindow.from(2000L).to(4000L);
    public static final Long ROW_COUNT = TIMELINE_BOUNDS.size();
    public static final LocalDateTime FROM_DATE = LocalDateTime.of(2016, 1, 1, 0, 0, 0);
    public static final LocalDateTime TO_DATE = FROM_DATE.plus(2, ChronoUnit.YEARS);
    public static final Long WINDOW_AMOUNT = TIMELINE_BOUNDS.size() / 10;

    private AnimalTestData() {

    }

    public static final AnimalTestData build() {
        return new AnimalTestData();
    }

    private static List<TrackerWindow> expectedTrackerWindows;
    public static List<TrackerWindow> getExpectedTrackerWindows() {
        if (null == expectedTrackerWindows) {
            expectedTrackerWindows = new ArrayList<>();

            Long currentTo = TIMELINE_BOUNDS.getTo();

            while (currentTo > TIMELINE_BOUNDS.getFrom()) {
                Long thisFrom = currentTo - WINDOW_AMOUNT;
                expectedTrackerWindows.add(TrackerWindow.from(thisFrom).to(currentTo));
                currentTo = thisFrom;
            }
        }

        return expectedTrackerWindows;
    }

    @Override
    public void accept(final Consumer<String> writer) {
        DataGenerator.buildDefinition()
                .addFieldDefinition(DataGenerator.sequentialNumberField(AnimalSighting.STREAM_ID, TIMELINE_BOUNDS.getFrom(), TIMELINE_BOUNDS.getTo()))
                .addFieldDefinition(DataGenerator.randomValueField(AnimalSighting.SPECIES,
                        Arrays.asList("spider", "whale", "dog", "tiger", "monkey", "lion", "woodlouse", "honey-badger")))
                .addFieldDefinition(DataGenerator.randomValueField(AnimalSighting.LOCATION,
                        Arrays.asList("europe", "asia", "america", "antarctica", "africa", "australia")))
                .addFieldDefinition(DataGenerator.randomValueField(AnimalSighting.OBSERVER,
                        Arrays.asList("alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel")))
                .addFieldDefinition(DataGenerator.randomDateTimeField(AnimalSighting.TIME,
                        FROM_DATE,
                        TO_DATE,
                        DateTimeFormatter.ISO_LOCAL_DATE_TIME))
                .setDataWriter(FlatDataWriterBuilder.defaultCsvFormat())
                .rowCount(Math.toIntExact(ROW_COUNT))
                .consumedBy(s -> s.forEach(writer))
                .generate();
    }
}
