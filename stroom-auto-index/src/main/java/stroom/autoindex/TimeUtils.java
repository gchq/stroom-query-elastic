package stroom.autoindex;

import org.jooq.types.ULong;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Optional;

/**
 * Some utility functions for converting between LocalDateTime and Long/ULong.
 */
public final class TimeUtils {
    public static LocalDateTime dateTimeFromULong(final ULong longValue) {
        return Optional.ofNullable(longValue)
                .map(ULong::longValue)
                .map(l -> Instant.ofEpochSecond(l).atZone(ZoneId.of(ZoneOffset.UTC.getId())).toLocalDateTime())
                .orElse(null);
    }

    public static LocalDateTime dateTimeFromLong(final Long longValue) {
        return Optional.ofNullable(longValue)
                .map(l ->  Instant.ofEpochSecond(l).atZone(ZoneId.of(ZoneOffset.UTC.getId())).toLocalDateTime())
                .orElse(null);
    }

    public static ULong getEpochSecondsULong(final LocalDateTime dateTime) {
        return ULong.valueOf(getEpochSeconds(dateTime));
    }

    public static Long getEpochSeconds(final LocalDateTime dateTime) {
        return dateTime.atZone(ZoneOffset.UTC).toInstant().getEpochSecond();
    }

    public static LocalDateTime nowUtcSeconds() {
        return LocalDateTime.now(Clock.systemUTC()).truncatedTo(ChronoUnit.SECONDS);
    }

    private TimeUtils() {

    }
}
