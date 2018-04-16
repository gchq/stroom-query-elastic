package stroom.query.akka.app;

import stroom.datasource.api.v2.DataSourceField;
import stroom.query.csv.CsvField;
import stroom.query.csv.CsvFieldSupplier;

import java.util.stream.Stream;

public class TreesFieldSupplier implements CsvFieldSupplier {

    private static CsvField SPECIES_CSV = CsvField.withName(TreeSighting.SPECIES)
            .andType(DataSourceField.DataSourceFieldType.FIELD)
            .atPosition(0)
            .build();
    private static CsvField LOCATION_CSV = CsvField.withName(TreeSighting.LOCATION)
            .andType(DataSourceField.DataSourceFieldType.FIELD)
            .atPosition(1)
            .build();
    private static CsvField LUMBERJACK_CSV = CsvField.withName(TreeSighting.LUMBERJACK)
            .andType(DataSourceField.DataSourceFieldType.FIELD)
            .atPosition(2)
            .build();
    private static CsvField TIME_CSV = CsvField.withName(TreeSighting.TIME)
            .andType(DataSourceField.DataSourceFieldType.DATE_FIELD)
            .atPosition(3)
            .build();

    @Override
    public Stream<CsvField> getFields() {
        return Stream.of(SPECIES_CSV, LOCATION_CSV, TIME_CSV, LUMBERJACK_CSV);
    }
}
