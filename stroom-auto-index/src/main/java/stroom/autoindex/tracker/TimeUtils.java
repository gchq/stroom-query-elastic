package stroom.autoindex.tracker;

import org.jooq.types.ULong;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Optional;

/**
 * Some utility functions for converting between LocalDateTime and Long/ULong.
 */
final class TimeUtils {
    static LocalDateTime dateTimeFromULong(final ULong longValue) {
        return Optional.ofNullable(longValue)
                .map(ULong::longValue)
                .map(l ->  Instant.ofEpochSecond(l).atZone(ZoneId.systemDefault()).toLocalDateTime())
                .orElse(null);
    }

    static LocalDateTime dateTimeFromLong(final Long longValue) {
        return Optional.ofNullable(longValue)
                .map(l ->  Instant.ofEpochSecond(l).atZone(ZoneId.of(ZoneOffset.UTC.getId())).toLocalDateTime())
                .orElse(null);
    }

    static ULong getEpochSecondsULong(final LocalDateTime dateTime) {
        return ULong.valueOf(getEpochSeconds(dateTime));
    }

    static Long getEpochSeconds(final LocalDateTime dateTime) {
        return dateTime.atZone(ZoneOffset.UTC).toInstant().getEpochSecond();
    }

    private TimeUtils() {

    }
}
