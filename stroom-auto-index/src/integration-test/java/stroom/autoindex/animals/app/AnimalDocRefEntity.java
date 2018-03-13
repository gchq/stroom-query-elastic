package stroom.autoindex.animals.app;

import stroom.query.audit.model.DocRefEntity;

import java.util.Objects;

public class AnimalDocRefEntity extends DocRefEntity {
    public static final String TYPE = "AnimalDocRefEntity";

    public static final String SPECIES = "Species";

    private String species;

    public String getSpecies() {
        return species;
    }

    public void setSpecies(String species) {
        this.species = species;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        AnimalDocRefEntity that = (AnimalDocRefEntity) o;
        return Objects.equals(species, that.species);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), species);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AnimalDocRefEntity{");
        sb.append("super='").append(super.toString()).append('\'');
        sb.append("species='").append(species).append('\'');
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

        public Builder species(final String value) {
            this.instance.species = value;
            return self();
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
