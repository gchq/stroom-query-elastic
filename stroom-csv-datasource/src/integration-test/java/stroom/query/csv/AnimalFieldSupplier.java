package stroom.query.csv;

import stroom.datasource.api.v2.DataSourceField;

import java.util.stream.Stream;

public class AnimalFieldSupplier implements CsvFieldSupplier {

    private static CsvField SPECIES_CSV = CsvField.withName(AnimalSighting.SPECIES)
            .andType(DataSourceField.DataSourceFieldType.FIELD)
            .atPosition(0)
            .build();
    private static CsvField LOCATION_CSV = CsvField.withName(AnimalSighting.LOCATION)
            .andType(DataSourceField.DataSourceFieldType.FIELD)
            .atPosition(1)
            .build();
    private static CsvField OBSERVER_CSV = CsvField.withName(AnimalSighting.OBSERVER)
            .andType(DataSourceField.DataSourceFieldType.FIELD)
            .atPosition(2)
            .build();
    private static CsvField TIME_CSV = CsvField.withName(AnimalSighting.TIME)
            .andType(DataSourceField.DataSourceFieldType.DATE_FIELD)
            .atPosition(3)
            .build();

    @Override
    public Stream<CsvField> getFields() {
        return Stream.of(SPECIES_CSV, LOCATION_CSV, TIME_CSV, OBSERVER_CSV);
    }
}
