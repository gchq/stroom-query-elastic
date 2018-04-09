package stroom.autoindex.indexing;

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.types.ULong;
import stroom.autoindex.AutoIndexConstants;
import stroom.autoindex.service.AutoIndexDocRefEntity;
import stroom.autoindex.service.AutoIndexDocRefServiceImpl;
import stroom.autoindex.tracker.AutoIndexTracker;
import stroom.autoindex.tracker.AutoIndexTrackerDao;
import stroom.autoindex.tracker.AutoIndexTrackerDaoImpl;
import stroom.autoindex.tracker.NextWindowSelector;
import stroom.autoindex.tracker.TrackerWindow;
import stroom.query.audit.security.ServiceUser;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.Optional;
import java.util.UUID;

import static stroom.autoindex.tracker.AutoIndexTrackerDaoImpl.FIELD_DOC_REF_UUID;
import static stroom.autoindex.tracker.AutoIndexTrackerDaoImpl.FIELD_FROM;
import static stroom.autoindex.tracker.AutoIndexTrackerDaoImpl.FIELD_TO;

public class IndexJobDaoImpl implements IndexJobDao {

    private static final String JOB_ID = "jobId";
    private static final String STARTED_TIME = "startedTime";
    private static final String CREATE_TIME = "createTime";

    public static final Table<Record> JOB_TABLE = DSL.table(IndexJob.TABLE_NAME);
    public static final Field<String> FIELD_JOB_ID = DSL.field(JOB_ID, String.class);
    public static final Field<ULong> FIELD_STARTED_TIME = DSL.field(STARTED_TIME, ULong.class);
    public static final Field<ULong> FIELD_CREATE_TIME = DSL.field(CREATE_TIME, ULong.class);

    private final DSLContext database;
    private final AutoIndexTrackerDao autoIndexTrackerDao;
    private final AutoIndexDocRefServiceImpl autoIndexDocRefService;
    private final ServiceUser serviceUser;

    @Inject
    public IndexJobDaoImpl(final DSLContext database,
                           final AutoIndexTrackerDao autoIndexTrackerDao,
                           final AutoIndexDocRefServiceImpl autoIndexDocRefService,
                           @Named(AutoIndexConstants.STROOM_SERVICE_USER)
                               final ServiceUser serviceUser) {
        this.database = database;
        this.autoIndexTrackerDao = autoIndexTrackerDao;
        this.autoIndexDocRefService = autoIndexDocRefService;
        this.serviceUser = serviceUser;
    }

    @Override
    public IndexJob getOrCreate(final AutoIndexDocRefEntity autoIndexDocRefEntity) {

        final String docRefUuid = autoIndexDocRefEntity.getUuid();

        // Get or create
        return database.transactionResult(c -> Optional.ofNullable(
                DSL.using(c).select()
                        .from(JOB_TABLE)
                        .where(FIELD_DOC_REF_UUID.equal(docRefUuid))
                        .fetchOne(this::getFromRecord))
                .orElseGet(() -> {
                    // Get hold of the current state of the auto index, and its trackers
                    final AutoIndexDocRefEntity autoIndex = getAutoIndex(docRefUuid);
                    final AutoIndexTracker tracker = autoIndexTrackerDao.get(docRefUuid);

                    // Calculate the next window to try
                    final TrackerWindow nextWindow = NextWindowSelector
                            .withBounds(tracker.getTimelineBounds())
                            .windowSize(autoIndex.getIndexWindow())
                            .existingWindows(tracker.getWindows())
                            .suggestNextWindow();

                    // Create a job for that window
                    final IndexJob indexJob = IndexJob.forAutoIndex(autoIndex)
                            .jobId(UUID.randomUUID().toString())
                            .trackerWindow(nextWindow)
                            .startedTimeMillis(0L)
                            .createdTimeMillis(System.currentTimeMillis())
                            .build();

                    // Add to database
                    DSL.using(c)
                            .insertInto(JOB_TABLE)
                            .columns(FIELD_JOB_ID,
                                    FIELD_DOC_REF_UUID,
                                    FIELD_STARTED_TIME,
                                    FIELD_CREATE_TIME,
                                    FIELD_FROM,
                                    FIELD_TO)
                            .values(indexJob.getJobId(),
                                    docRefUuid,
                                    ULong.valueOf(indexJob.getStartedTimeMillis()),
                                    ULong.valueOf(indexJob.getCreatedTimeMillis()),
                                    ULong.valueOf(indexJob.getTrackerWindow().getFrom()),
                                    ULong.valueOf(indexJob.getTrackerWindow().getTo()))
                            .execute();

                    return indexJob;
                }));
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
    public void markAsStarted(final String jobId) {
        final int rowsAffected = database.transactionResult(c ->
                DSL.using(c)
                        .update(JOB_TABLE)
                        .set(FIELD_STARTED_TIME, ULong.valueOf(System.currentTimeMillis()))
                        .where(FIELD_JOB_ID.equal(jobId))
                        .execute());

        if (0 == rowsAffected) {
            throw new RuntimeException(String.format("Could not mark Job %s as started, rows affected %d", jobId, rowsAffected));
        }
    }

    @Override
    public void markAsComplete(final String jobId) {
        // Fetch the job
        final IndexJob indexJob = get(jobId)
                .orElseThrow(() -> new RuntimeException(String.format("Could not find Index Job for %s", jobId)));

        // then add the window to the tracker
        autoIndexTrackerDao.addWindow(indexJob.getAutoIndexDocRefEntity().getUuid(), indexJob.getTrackerWindow());

        // Now delete the job
        final int rowsAffected = database.transactionResult(c ->
                DSL.using(c).deleteFrom(JOB_TABLE)
                        .where(FIELD_JOB_ID.equal(jobId))
                        .execute());

        if (0 == rowsAffected) {
            throw new RuntimeException(String.format("Could not mark Job %s as complete, rows affected %d", jobId, rowsAffected));
        }
    }

    private IndexJob getFromRecord(final Record record) {

        final String docRefUuid = record.get(FIELD_DOC_REF_UUID);

        final AutoIndexDocRefEntity autoIndex = getAutoIndex(docRefUuid);

        return IndexJob.forAutoIndex(autoIndex)
                .jobId(record.get(FIELD_JOB_ID))
                .createdTimeMillis(record.get(FIELD_CREATE_TIME).longValue())
                .startedTimeMillis(record.get(FIELD_STARTED_TIME).longValue())
                .trackerWindow(AutoIndexTrackerDaoImpl.fromRecord(record))
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
