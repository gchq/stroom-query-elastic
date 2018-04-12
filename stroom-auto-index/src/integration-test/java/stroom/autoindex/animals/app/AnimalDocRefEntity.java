package stroom.autoindex.animals.app;

import stroom.query.audit.model.DocRefEntity;

import java.util.Objects;

public class AnimalDocRefEntity extends DocRefEntity {
    public static final String TYPE = "AnimalHuntingGroup";

    public static final String DATA_DIRECTORY = "Species";

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
        AnimalDocRefEntity that = (AnimalDocRefEntity) o;
        return Objects.equals(dataDirectory, that.dataDirectory);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), dataDirectory);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AnimalDocRefEntity{");
        sb.append("super='").append(super.toString()).append('\'');
        sb.append("dataDirectory='").append(dataDirectory).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public static final class Builder extends DocRefEntity.BaseBuilder<AnimalDocRefEntity, Builder> {

        public Builder() {
            this(new AnimalDocRefEntity());
        }

        public Builder(final AnimalDocRefEntity instance) {
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
