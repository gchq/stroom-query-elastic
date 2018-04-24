package stroom.test;

import stroom.query.api.v2.*;
import stroom.query.csv.CsvDataRow;
import stroom.query.csv.CsvFieldSupplier;
import stroom.testdata.DataGenerator;
import stroom.testdata.FlatDataWriterBuilder;
import stroom.tracking.TrackerWindow;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.function.Consumer;

import static org.junit.Assert.assertTrue;

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

    public static SearchRequest getTestSearchRequest(final DocRef docRef,
                                                     final ExpressionOperator expressionOperator,
                                                     final OffsetRange offsetRange) {
        final String queryKey = UUID.randomUUID().toString();
        return new SearchRequest.Builder()
                .query(new Query.Builder()
                        .dataSource(docRef)
                        .expression(expressionOperator)
                        .build())
                .key(queryKey)
                .dateTimeLocale("en-gb")
                .incremental(true)
                .addResultRequests(new ResultRequest.Builder()
                        .fetch(ResultRequest.Fetch.ALL)
                        .resultStyle(ResultRequest.ResultStyle.FLAT)
                        .componentId("componentId")
                        .requestedRange(offsetRange)
                        .addMappings(new TableSettings.Builder()
                                .queryId(queryKey)
                                .extractValues(false)
                                .showDetail(false)
                                .addFields(new Field.Builder()
                                        .name(AnimalSighting.STREAM_ID)
                                        .expression("${" + AnimalSighting.STREAM_ID + "}")
                                        .build())
                                .addFields(new Field.Builder()
                                        .name(AnimalSighting.SPECIES)
                                        .expression("${" + AnimalSighting.SPECIES + "}")
                                        .build())
                                .addFields(new Field.Builder()
                                        .name(AnimalSighting.LOCATION)
                                        .expression("${" + AnimalSighting.LOCATION + "}")
                                        .build())
                                .addFields(new Field.Builder()
                                        .name(AnimalSighting.OBSERVER)
                                        .expression("${" + AnimalSighting.OBSERVER + "}")
                                        .build())
                                .addFields(new Field.Builder()
                                        .name(AnimalSighting.TIME)
                                        .expression("${" + AnimalSighting.TIME + "}")
                                        .build())
                                .addMaxResults(1000)
                                .build())
                        .build())
                .build();
    }

    public static Set<AnimalSighting> getAnimalSightingsFromResponse(final SearchResponse searchResponse) {
        final Set<AnimalSighting> resultsSet = new HashSet<>();
        final CsvFieldSupplier csvFieldSupplier = new AnimalFieldSupplier();

        assertTrue("No results seen", searchResponse.getResults().size() > 0);
        for (final Result result : searchResponse.getResults()) {
            assertTrue(result instanceof FlatResult);

            final FlatResult flatResult = (FlatResult) result;
            flatResult.getValues().stream()
                    .map(o -> {
                        final CsvDataRow row = new CsvDataRow();
                        csvFieldSupplier.getFields()
                                .forEach(f -> row.withField(f, o.get(f.getPosition() + 3))); // skip over 3 std fields
                        return row;
                    })
                    .map(AnimalSighting::new)
                    .forEach(resultsSet::add);
        }

        return resultsSet;
    }
}
