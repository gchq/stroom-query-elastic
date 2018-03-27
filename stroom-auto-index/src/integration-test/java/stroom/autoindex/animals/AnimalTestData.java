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

    public static final LocalDateTime FROM_DATE = LocalDateTime.of(2016, 1, 1, 0, 0, 0);
    public static final LocalDateTime TO_DATE = FROM_DATE.plus(2, ChronoUnit.YEARS);
    public static final int WINDOW_AMOUNT = 1;
    public static final ChronoUnit WINDOW_UNITS = ChronoUnit.MONTHS;

    private AnimalTestData() {

    }

    public static final AnimalTestData build() {
        return new AnimalTestData();
    }

    private static List<TrackerWindow> expectedTrackerWindows;
    public static List<TrackerWindow> getExpectedTrackerWindows() {
        if (null == expectedTrackerWindows) {
            expectedTrackerWindows = new ArrayList<>();

            LocalDateTime currentTo = TO_DATE;

            while (currentTo.isAfter(FROM_DATE)) {
                LocalDateTime thisFrom = currentTo.minus(WINDOW_AMOUNT, WINDOW_UNITS);
                expectedTrackerWindows.add(TrackerWindow.from(thisFrom).to(currentTo));
                currentTo = thisFrom;
            }
        }

        return expectedTrackerWindows;
    }

    @Override
    public void accept(final Consumer<String> writer) {
        DataGenerator.buildDefinition()
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
                .rowCount(1000)
                .consumedBy(s -> s.forEach(writer))
                .generate();
    }
}
