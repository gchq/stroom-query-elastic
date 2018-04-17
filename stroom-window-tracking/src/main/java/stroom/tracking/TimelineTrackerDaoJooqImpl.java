package stroom.tracking;

import org.jooq.*;
import org.jooq.impl.DSL;
import org.jooq.types.ULong;

import javax.inject.Inject;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

public class TimelineTrackerDaoJooqImpl implements TimelineTrackerDao<Configuration> {

    private final DSLContext database;

    private static final String DOC_REF_UUID = "docRefUuid";
    private static final String FROM = "fromValue";
    private static final String TO = "toValue";

    private static final Table<Record> TRACKER_WINDOW_TABLE = DSL.table(TimelineTracker.TRACKER_WINDOW_TABLE_NAME);
    private static final Table<Record> TIMELINE_BOUNDS_TABLE = DSL.table(TimelineTracker.TIMELINE_BOUNDS_TABLE_NAME);
    public static final Field<String> FIELD_DOC_REF_UUID = DSL.field(DOC_REF_UUID, String.class);
    public static final Field<ULong> FIELD_FROM = DSL.field(FROM, ULong.class);
    public static final Field<ULong> FIELD_TO = DSL.field(TO, ULong.class);

    @Inject
    public TimelineTrackerDaoJooqImpl(final DSLContext database) {
        this.database = database;
    }

    @Override
    public TimelineTracker transactionResult(
            final BiFunction<TimelineTrackerDao<Configuration>, Configuration, TimelineTracker> txFunction) {
        return database.transactionResult(c -> txFunction.apply(this, c));
    }

    @Override
    public void transaction(BiConsumer<TimelineTrackerDao<Configuration>, Configuration> txFunction) {
        database.transaction(c -> txFunction.accept(this, c));
    }

    public TimelineTracker get(final Configuration c,
                               final String docRefUuid) {
        final TrackerWindow timelineBounds = DSL.using(c).select()
                .from(TIMELINE_BOUNDS_TABLE)
                .where(FIELD_DOC_REF_UUID.equal(docRefUuid))
                .fetchOne(TimelineTrackerDaoJooqImpl::fromRecord);

        return DSL.using(c)
                .select()
                .from(TRACKER_WINDOW_TABLE)
                .where(FIELD_DOC_REF_UUID.equal(docRefUuid))
                .orderBy(FIELD_FROM)
                .fetch(TimelineTrackerDaoJooqImpl::fromRecord)
                .stream()
                .filter(TrackerWindow::isBound)
                .reduce(TimelineTracker.forDocRef(docRefUuid)
                                .withBounds(timelineBounds),
                        TimelineTracker::withWindow,
                        (a, b) -> a.withWindows(b.getWindows()));
    }

    public void deleteTracker(final Configuration transaction,
                              final String docRefUuid,
                              final TrackerWindow tw) {
        DSL.using(transaction)
                .deleteFrom(TRACKER_WINDOW_TABLE)
                .where(FIELD_DOC_REF_UUID.equal(docRefUuid)
                        .and(FIELD_FROM.equal(ULong.valueOf(tw.getFrom()))))
                .execute();
    }

    public void insertTracker(final Configuration transaction,
                              final String docRefUuid,
                              final TrackerWindow tw) {
        DSL.using(transaction).insertInto(TRACKER_WINDOW_TABLE)
                .columns(FIELD_DOC_REF_UUID, FIELD_FROM, FIELD_TO)
                .values(docRefUuid, ULong.valueOf(tw.getFrom()), ULong.valueOf(tw.getTo()))
                .execute();
    }

    @Override
    public void setTimelineBounds(final Configuration configuration,
                                  final String docRefUuid,
                                  final TrackerWindow timelineBounds) {
        int existing = DSL.using(configuration).update(TIMELINE_BOUNDS_TABLE)
                .set(FIELD_FROM, ULong.valueOf(timelineBounds.getFrom()))
                .set(FIELD_TO, ULong.valueOf(timelineBounds.getTo()))
                .where(FIELD_DOC_REF_UUID.equal(docRefUuid))
                .execute();

        if (0 == existing) {
            DSL.using(configuration).insertInto(TIMELINE_BOUNDS_TABLE)
                    .columns(FIELD_DOC_REF_UUID, FIELD_FROM, FIELD_TO)
                    .values(docRefUuid, ULong.valueOf(timelineBounds.getFrom()), ULong.valueOf(timelineBounds.getTo()))
                    .execute();
        }
    }

    /**
     * JOOQ {@link RecordMapper} for {@link TrackerWindow}
     * @param r The jOOQ Record to map
     * @return The Tracker Window created from those details
     */
    public static TrackerWindow fromRecord(final Record r) {
        return TrackerWindow
                .from(r.get(FIELD_FROM).longValue())
                .to(r.get(FIELD_TO).longValue());
    }
}
