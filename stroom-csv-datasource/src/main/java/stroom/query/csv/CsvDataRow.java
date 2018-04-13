package stroom.query.csv;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

public class CsvDataRow {
    private final Map<String, Object> data = new HashMap<>();

    public CsvDataRow() {

    }

    public CsvDataRow withField(final CsvField field,
                                final Object value) {
        Object valueObj = null;

        switch (field.getType()) {
            case DATE_FIELD:
                valueObj = LocalDateTime.parse(String.valueOf(value), DateTimeFormatter.ISO_LOCAL_DATE_TIME);
                break;
            case FIELD:
            case ID:
                valueObj = String.valueOf(value);
                break;
            case NUMERIC_FIELD:
                valueObj = Long.valueOf(String.valueOf(value));
                break;
        }

        if (null != value) {
            data.put(field.getName(), valueObj);
        }

        return this;
    }

    public Map<String, Object> getData() {
        return data;
    }
}
