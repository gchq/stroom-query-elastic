package stroom.query.csv;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

public class AnimalSighting {
    public static final String SPECIES = "species";
    public static final String LOCATION = "location";
    public static final String TIME = "time";
    public static final String OBSERVER = "observer";

    private final String species;
    private final String location;
    private final LocalDateTime time;
    private final String observer;

    public AnimalSighting(final CsvDataRow data) {
        species = String.valueOf(data.getData().get(SPECIES));
        location = String.valueOf(data.getData().get(LOCATION));
        time = Optional.ofNullable(data.getData().get(TIME))
                .map(String::valueOf)
                .map(t -> LocalDateTime.parse(t, DateTimeFormatter.ISO_LOCAL_DATE_TIME))
                .orElse(null);
        observer = String.valueOf(data.getData().get(OBSERVER));

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
}
