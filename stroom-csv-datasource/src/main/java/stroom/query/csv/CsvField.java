package stroom.query.csv;

import stroom.datasource.api.v2.DataSourceField;

import java.util.Objects;

public class CsvField {
    private final Integer position;
    private final DataSourceField.DataSourceFieldType type;
    private final String name;

    private CsvField(final Builder builder) {
        this.position = Objects.requireNonNull(builder.position);
        this.type = Objects.requireNonNull(builder.type);
        this.name = Objects.requireNonNull(builder.name);
    }

    public String getName() {
        return name;
    }

    public DataSourceField.DataSourceFieldType getType() {
        return type;
    }

    public Integer getPosition() {
        return position;
    }

    public static Builder withName(final String name) {
        return new Builder(name);
    }

    public static class Builder {
        private DataSourceField.DataSourceFieldType type;
        private Integer position;
        private final String name;

        private Builder(final String name) {
            this.name = name;
        }

        public Builder andType(final DataSourceField.DataSourceFieldType type) {
            this.type = type;
            return this;
        }

        public Builder atPosition(final Integer position) {
            this.position = position;
            return this;
        }

        public CsvField build() {
            return new CsvField(this);
        }
    }
}
