package stroom.autoindex.indexing;

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.types.ULong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.autoindex.AutoIndexConstants;
import stroom.autoindex.service.AutoIndexDocRefEntity;
import stroom.autoindex.service.AutoIndexDocRefServiceImpl;
import stroom.query.audit.security.ServiceUser;
import stroom.tracking.*;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.Optional;
import java.util.UUID;

import static stroom.tracking.TimelineTrackerDaoJooqImpl.*;

public class IndexJobDaoImpl implements IndexJobDao {
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexJobDaoImpl.class);

    private static final String JOB_ID = "jobId";
    private static final String STARTED_TIME = "startedTime";
    private static final String CREATE_TIME = "createTime";
    private static final String COMPLETED_TIME = "completedTime";

    public static final Table<Record> JOB_TABLE = DSL.table(IndexJob.TABLE_NAME);
    public static final Field<String> FIELD_JOB_ID = DSL.field(JOB_ID, String.class);
    public static final Field<ULong> FIELD_STARTED_TIME = DSL.field(STARTED_TIME, ULong.class);
    public static final Field<ULong> FIELD_CREATE_TIME = DSL.field(CREATE_TIME, ULong.class);
    public static final Field<ULong> FIELD_COMPLETED_TIME = DSL.field(COMPLETED_TIME, ULong.class);

    private final DSLContext database;
    private final TimelineTrackerService timelineTrackerService;
    private final AutoIndexDocRefServiceImpl autoIndexDocRefService;
    private final ServiceUser serviceUser;

    @Inject
    public IndexJobDaoImpl(final DSLContext database,
                           final TimelineTrackerService timelineTrackerService,
                           final AutoIndexDocRefServiceImpl autoIndexDocRefService,
                           @Named(AutoIndexConstants.STROOM_SERVICE_USER)
                               final ServiceUser serviceUser) {
        this.database = database;
        this.timelineTrackerService = timelineTrackerService;
        this.autoIndexDocRefService = autoIndexDocRefService;
        this.serviceUser = serviceUser;
    }

    @Override
    public Optional<IndexJob> getOrCreate(final String docRefUuid) {

        // Get or create
        return Optional.ofNullable(
                database.transactionResult(c -> Optional.ofNullable(
                DSL.using(c).select()
                        .from(JOB_TABLE)
                        .where(FIELD_DOC_REF_UUID.equal(docRefUuid))
                        .and(FIELD_STARTED_TIME.equal(ULong.valueOf(0L)))
                        .and(FIELD_COMPLETED_TIME.equal(ULong.valueOf(0L)))
                        .fetchOne(this::getFromRecord))
                .orElseGet(() -> {
                    // Get hold of the current state of the auto index, and its trackers
                    final AutoIndexDocRefEntity autoIndex = getAutoIndex(docRefUuid);
                    final TimelineTracker tracker = timelineTrackerService.get(docRefUuid);

                    final Optional<TrackerWindow> timelineBounds = tracker.getTimelineBounds();
                    if (!timelineBounds.isPresent()) {
                        LOGGER.warn("Can't fetch Index Job for {}, timeline bounds not set", docRefUuid);
                        return null;
                    }

                    // Calculate the next window to try
                    final Optional<TrackerWindow> nextWindow = NextWindowSelector
                            .withBounds(timelineBounds.get())
                            .windowSize(autoIndex.getIndexWindow())
                            .existingWindows(tracker.getWindows())
                            .suggestNextWindow();

                    if (!nextWindow.isPresent()) {
                        LOGGER.warn("Can't fetch Index Job for {}, next window not available", docRefUuid);
                        return null;
                    }

                    // Create a job for that window
                    final IndexJob indexJob = IndexJob.forAutoIndex(autoIndex)
                            .jobId(UUID.randomUUID().toString())
                            .trackerWindow(nextWindow.get())
                            .createdTimeMillis(System.currentTimeMillis())
                            .build();

                    // Add to database
                    DSL.using(c)
                            .insertInto(JOB_TABLE)
                            .columns(FIELD_JOB_ID,
                                    FIELD_DOC_REF_UUID,
                                    FIELD_CREATE_TIME,
                                    FIELD_STARTED_TIME,
                                    FIELD_COMPLETED_TIME,
                                    FIELD_FROM,
                                    FIELD_TO)
                            .values(indexJob.getJobId(),
                                    docRefUuid,
                                    ULong.valueOf(indexJob.getCreatedTimeMillis()),
                                    ULong.valueOf(indexJob.getStartedTimeMillis()),
                                    ULong.valueOf(indexJob.getCompletedTimeMillis()),
                                    ULong.valueOf(indexJob.getTrackerWindow().getFrom()),
                                    ULong.valueOf(indexJob.getTrackerWindow().getTo()))
                            .execute();

                    return indexJob;
                })));
    }

    @Override
    public Optional<IndexJob> get(String jobId) {
        return database.transactionResult(c ->  Optional.ofNullable(
                DSL.using(c).select()
                        .from(JOB_TABLE)
                        .where(FIELD_JOB_ID.equal(jobId))
                        .fetchOne(this::getFromRecord)));
    }

    @Override
    public IndexJob markAsStarted(final String jobId) {
        return database.transactionResult(c -> {
            final int rowsAffected = DSL.using(c)
                    .update(JOB_TABLE)
                    .set(FIELD_STARTED_TIME, ULong.valueOf(System.currentTimeMillis()))
                    .where(FIELD_JOB_ID.equal(jobId))
                    .and(FIELD_STARTED_TIME.equal(ULong.valueOf(0L)))
                    .and(FIELD_COMPLETED_TIME.equal(ULong.valueOf(0L)))
                    .execute();

            if (0 == rowsAffected) {
                throw new RuntimeException(String.format("Could not mark Job %s as started, rows affected %d", jobId, rowsAffected));
            }

            return DSL.using(c).select()
                    .from(JOB_TABLE)
                    .where(FIELD_JOB_ID.equal(jobId))
                    .fetchOne(this::getFromRecord);
        });
    }

    @Override
    public IndexJob markAsComplete(final String jobId) {
        return database.transactionResult(c -> {
            // First attempt to update the completed time
            final int rowsAffected = DSL.using(c)
                    .update(JOB_TABLE)
                    .set(FIELD_COMPLETED_TIME, ULong.valueOf(System.currentTimeMillis()))
                    .where(FIELD_JOB_ID.equal(jobId))
                    .and(FIELD_STARTED_TIME.greaterThan(ULong.valueOf(0L)))
                    .and(FIELD_COMPLETED_TIME.equal(ULong.valueOf(0L)))
                    .execute();

            if (0 == rowsAffected) {
                throw new RuntimeException(String.format("Could not mark Job %s as started, rows affected %d", jobId, rowsAffected));
            }

            // Now retrieve the updated state
            final IndexJob indexJob = DSL.using(c).select()
                    .from(JOB_TABLE)
                    .where(FIELD_JOB_ID.equal(jobId))
                    .fetchOne(this::getFromRecord);

            // then add the window to the tracker
            timelineTrackerService.addWindow(indexJob.getAutoIndexDocRefEntity().getUuid(), indexJob.getTrackerWindow());

            return indexJob;
        });
    }

    private IndexJob getFromRecord(final Record record) {

        final String docRefUuid = record.get(FIELD_DOC_REF_UUID);

        final AutoIndexDocRefEntity autoIndex = getAutoIndex(docRefUuid);

        return IndexJob.forAutoIndex(autoIndex)
                .jobId(record.get(FIELD_JOB_ID))
                .createdTimeMillis(record.get(FIELD_CREATE_TIME).longValue())
                .startedTimeMillis(record.get(FIELD_STARTED_TIME).longValue())
                .completedTimeMillis(record.get(FIELD_COMPLETED_TIME).longValue())
                .trackerWindow(TimelineTrackerDaoJooqImpl.fromRecord(record))
                .build();
    }

    /**
     * Wrapper of Get Auto index that wraps any exception in a Runtime
     * @param docRefUuid The Doc Ref of the auto index to fetch
     * @return The AutoIndexDocRefEntity found
     */
    private AutoIndexDocRefEntity getAutoIndex(final String docRefUuid) {
        try {
            return autoIndexDocRefService.get(serviceUser, docRefUuid)
                    .orElseThrow(() -> new RuntimeException(String.format("Could not find Auto Index for UUID: %s", docRefUuid)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
