package stroom.query.csv;

import stroom.query.audit.model.DocRefEntity;

import java.util.Objects;

public class CsvDocRefEntity extends DocRefEntity {
    public static final String TYPE = "CsvDirectory";

    public static final String DATA_DIRECTORY = "DataDirectory";

    private String dataDirectory;

    public String getDataDirectory() {
        return dataDirectory;
    }

    public void setDataDirectory(String dataDirectory) {
        this.dataDirectory = dataDirectory;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        CsvDocRefEntity that = (CsvDocRefEntity) o;
        return Objects.equals(dataDirectory, that.dataDirectory);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), dataDirectory);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CsvDocRefEntity{");
        sb.append("super='").append(super.toString()).append('\'');
        sb.append("dataDirectory='").append(dataDirectory).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public static final class Builder extends BaseBuilder<CsvDocRefEntity, Builder> {

        public Builder() {
            this(new CsvDocRefEntity());
        }

        public Builder(final CsvDocRefEntity instance) {
            super(instance);
        }

        public Builder dataDirectory(final String value) {
            this.instance.dataDirectory = value;
            return self();
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
