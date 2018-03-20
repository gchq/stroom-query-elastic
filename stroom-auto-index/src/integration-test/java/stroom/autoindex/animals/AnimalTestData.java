package stroom.autoindex.animals;

import stroom.autoindex.animals.app.AnimalSighting;
import stroom.testdata.DataGenerator;
import stroom.testdata.FlatDataWriterBuilder;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.function.Consumer;

public class AnimalTestData implements Consumer<Consumer<String>> {

    private AnimalTestData() {

    }

    public static final AnimalTestData build() {
        return new AnimalTestData();
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
                        LocalDateTime.of(2016, 1, 1, 0, 0, 0),
                        LocalDateTime.of(2018, 1, 1, 0, 0, 0),
                        DateTimeFormatter.ISO_LOCAL_DATE_TIME))
                .setDataWriter(FlatDataWriterBuilder.defaultCsvFormat())
                .rowCount(1000)
                .consumedBy(s -> s.forEach(writer))
                .generate();
    }
}
