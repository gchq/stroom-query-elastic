package stroom.query.akka.app;

import stroom.testdata.DataGenerator;
import stroom.testdata.FlatDataWriterBuilder;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.function.Consumer;

public class TreesTestData implements Consumer<Consumer<String>> {

    public static final Long ROW_COUNT = 200L;
    public static final LocalDateTime FROM_DATE = LocalDateTime.of(2016, 1, 1, 0, 0, 0);
    public static final LocalDateTime TO_DATE = FROM_DATE.plus(2, ChronoUnit.YEARS);

    private TreesTestData() {

    }

    public static final TreesTestData build() {
        return new TreesTestData();
    }

    @Override
    public void accept(final Consumer<String> writer) {
        DataGenerator.buildDefinition()
                .addFieldDefinition(DataGenerator.randomValueField(TreeSighting.SPECIES,
                        Arrays.asList("oak", "beech", "chestnut", "birch", "redwood", "pine")))
                .addFieldDefinition(DataGenerator.randomValueField(TreeSighting.LOCATION,
                        Arrays.asList("field", "city", "village", "arboretum")))
                .addFieldDefinition(DataGenerator.randomValueField(TreeSighting.LUMBERJACK,
                        Arrays.asList("adam", "bill", "charlie", "derek", "edward", "felicity", "gemma", "horatio")))
                .addFieldDefinition(DataGenerator.randomDateTimeField(TreeSighting.TIME,
                        FROM_DATE,
                        TO_DATE,
                        DateTimeFormatter.ISO_LOCAL_DATE_TIME))
                .setDataWriter(FlatDataWriterBuilder.defaultCsvFormat())
                .rowCount(Math.toIntExact(ROW_COUNT))
                .consumedBy(s -> s.forEach(writer))
                .generate();
    }
}
