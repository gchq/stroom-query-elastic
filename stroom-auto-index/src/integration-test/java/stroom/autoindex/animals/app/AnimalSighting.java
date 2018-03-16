package stroom.autoindex.animals.app;

import java.time.LocalDateTime;
import java.util.Objects;

public class AnimalSighting {
    public static final String SPECIES = "species";
    public static final String LOCATION = "location";
    public static final String TIME = "time";
    public static final String OBSERVER = "observer";

    private final String species;
    private final String location;
    private final LocalDateTime time;
    private final String observer;

    private AnimalSighting(final Builder builder) {
        this.species = builder.species;
        this.location = builder.location;
        this.time = builder.time;
        this.observer = builder.observer;
    }

    public String getSpecies() {
        return species;
    }

    public String getLocation() {
        return location;
    }

    public LocalDateTime getTime() {
        return time;
    }

    public String getObserver() {
        return observer;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AnimalSighting{");
        sb.append("species='").append(species).append('\'');
        sb.append(", location='").append(location).append('\'');
        sb.append(", time=").append(time);
        sb.append(", observer='").append(observer).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public static class Builder {
        private String species;
        private String location;
        private LocalDateTime time;
        private String observer;

        public Builder species(final String value) {
            this.species = value;
            return this;
        }

        public Builder location(final String value) {
            this.location = value;
            return this;
        }

        public Builder time(final LocalDateTime value) {
            this.time = value;
            return this;
        }

        public Builder observer(final String value) {
            this.observer = value;
            return this;
        }

        public AnimalSighting build() {
            Objects.requireNonNull(this.species);
            Objects.requireNonNull(this.location);
            Objects.requireNonNull(this.time);
            Objects.requireNonNull(this.observer);

            return new AnimalSighting(this);
        }
    }
}
