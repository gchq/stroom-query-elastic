package stroom.query.akka.app;

import stroom.query.csv.CsvDataRow;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

public class TreeSighting {
    public static final String SPECIES = "species";
    public static final String LOCATION = "location";
    public static final String TIME = "time";
    public static final String LUMBERJACK = "lumberjack";

    private final String species;
    private final String location;
    private final LocalDateTime time;
    private final String lumberjack;

    public TreeSighting(final CsvDataRow data) {
        species = String.valueOf(data.getData().get(SPECIES));
        location = String.valueOf(data.getData().get(LOCATION));
        time = Optional.ofNullable(data.getData().get(TIME))
                .map(String::valueOf)
                .map(t -> LocalDateTime.parse(t, DateTimeFormatter.ISO_LOCAL_DATE_TIME))
                .orElse(null);
        lumberjack = String.valueOf(data.getData().get(LUMBERJACK));

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

    public String getLumberjack() {
        return lumberjack;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("TreeSighting{");
        sb.append("species='").append(species).append('\'');
        sb.append(", location='").append(location).append('\'');
        sb.append(", time=").append(time);
        sb.append(", lumberjack='").append(lumberjack).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
