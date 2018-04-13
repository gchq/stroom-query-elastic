package stroom.query.csv;

import java.util.stream.Stream;

public interface CsvFieldSupplier {
    Stream<CsvField> getFields();

    default long count() {
        return getFields().count();
    }
}
