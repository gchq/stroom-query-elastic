package stroom.autoindex.tracker;

import io.dropwizard.db.DataSourceFactory;
import org.jooq.*;
import org.jooq.impl.DSL;
import org.jooq.types.ULong;

import javax.inject.Inject;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class AutoIndexTrackerDaoImpl implements AutoIndexTrackerDao {
    private final DSLContext database;

    private static final String DOC_REF_UUID = "docRefUuid";
    private static final String FROM = "fromTime";
    private static final String TO = "toTime";

    private static final Table<Record> WINDOW_TABLE = DSL.table(AutoIndexTracker.TABLE_NAME);
    private static final Field<String> FIELD_DOC_REF_UUID = DSL.field(DOC_REF_UUID, String.class);
    private static final Field<ULong> FIELD_FROM = DSL.field(FROM, ULong.class);
    private static final Field<ULong> FIELD_TO = DSL.field(TO, ULong.class);

    private AutoIndexTrackerDaoImpl(final DSLContext database) {
        this.database = database;
    }

    @Inject
    public AutoIndexTrackerDaoImpl(final Configuration jooqConfig) {
        database = DSL.using(jooqConfig);
    }

    @Override
    public AutoIndexTracker get(final String docRefUuid) {
        return database.transactionResult(c -> getInTransaction(c, docRefUuid));
    }

    @Override
    public AutoIndexTracker addWindow(final String docRefUuid,
                                      final TrackerWindow window) {
        return database.transactionResult(c -> {
            final AutoIndexTracker current = getInTransaction(c, docRefUuid);


            DSL.using(c).insertInto(WINDOW_TABLE)
                    .columns(FIELD_DOC_REF_UUID, FIELD_FROM, FIELD_TO)
                    .values(docRefUuid, getEpochMillis(window.getFrom()), getEpochMillis(window.getTo()))
                    .execute();

            return getInTransaction(c, docRefUuid);
        });
    }

    @Override
    public AutoIndexTracker clearWindows(final String docRefUuid) {
        return database.transactionResult(c -> {
            DSL.using(c).deleteFrom(WINDOW_TABLE)
                    .where(FIELD_DOC_REF_UUID.equal(docRefUuid))
                    .execute();

            return getInTransaction(c, docRefUuid);
        });
    }

    private AutoIndexTracker getInTransaction(final Configuration c,
                                              final String docRefUuid) {
        final Result<Record> results = DSL.using(c)
                .select()
                .from(WINDOW_TABLE)
                .where(FIELD_DOC_REF_UUID.equal(docRefUuid))
                .orderBy(FIELD_FROM)
                .fetch();

        final List<TrackerWindow> windows = results.stream()
                .map(r -> TrackerWindow.from(fromLong(r.get(FIELD_FROM))).to(fromLong(r.get(FIELD_TO))))
                .filter(TrackerWindow::isBound)
                .collect(Collectors.toList());

        return new AutoIndexTracker(docRefUuid)
                .withWindows(windows);
    }

    private LocalDateTime fromLong(final ULong longValue) {
        return Optional.ofNullable(longValue)
                .map(ULong::longValue)
                .map(l ->  Instant.ofEpochSecond(l).atZone(ZoneId.systemDefault()).toLocalDateTime())
                .orElse(null);
    }

    private ULong getEpochMillis(final LocalDateTime dateTime) {
        return ULong.valueOf(dateTime.atZone(ZoneOffset.systemDefault()).toInstant().getEpochSecond());
    }

    /**
     * Allow building of instances of the services using raw database credentials.
     * @param url The URL of the remote database
     * @return A builder for constructing the instance.
     */
    public static DbCredsBuilder withDatabase(final String url) {
        return new DbCredsBuilder(url);
    }

    public static class DbCredsBuilder {
        private final String url;
        private String username;
        private String password;

        private DbCredsBuilder(final String url) {
            this.url = url;
        }

        public DbCredsBuilder username(final String username) {
            this.username = username;
            return this;
        }

        public DbCredsBuilder password(final String password) {
            this.password = password;
            return this;
        }

        public AutoIndexTrackerDaoImpl build() {
            if ((null != username) && (null != password)) {
                return new AutoIndexTrackerDaoImpl(DSL.using(this.url, this.username, this.password));
            } else {
                return new AutoIndexTrackerDaoImpl(DSL.using(this.url));
            }
        }
    }
}
