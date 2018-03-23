package stroom.autoindex.indexing;

import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.types.ULong;
import stroom.autoindex.AutoIndexDocRefEntity;
import stroom.autoindex.AutoIndexDocRefServiceImpl;
import stroom.autoindex.tracker.AutoIndexTracker;
import stroom.autoindex.tracker.AutoIndexTrackerDao;
import stroom.query.audit.security.ServiceUser;

import javax.inject.Inject;
import java.util.List;

public class IndexJobDaoImpl implements IndexJobDao {

    private static final String DOC_REF_UUID = "docRefUuid";
    private static final String STARTED = "started";
    private static final String CREATE_TIME = "createTime";

    private static final Table<Record> JOB_TABLE = DSL.table(IndexJob.TABLE_NAME);
    private static final Field<String> FIELD_DOC_REF_UUID = DSL.field(DOC_REF_UUID, String.class);
    private static final Field<Boolean> FIELD_STARTED = DSL.field(STARTED, Boolean.class);
    private static final Field<ULong> FIELD_CREATE_TIME = DSL.field(CREATE_TIME, ULong.class);

    private final DSLContext database;
    private final AutoIndexTrackerDao autoIndexTrackerDao;
    private final AutoIndexDocRefServiceImpl autoIndexDocRefService;

    private static final ServiceUser INTERNAL = new ServiceUser("INTERNAL", "INVALID_JWT");


    private IndexJobDaoImpl(final Builder builder) {
        this.database = builder.database;
        this.autoIndexTrackerDao = builder.autoIndexTrackerDao;
        this.autoIndexDocRefService = builder.autoIndexDocRefService;
    }

    @Inject
    public IndexJobDaoImpl(final Configuration jooqConfig,
                           final AutoIndexTrackerDao autoIndexTrackerDao,
                           final AutoIndexDocRefServiceImpl autoIndexDocRefService) {
        this.database = DSL.using(jooqConfig);
        this.autoIndexTrackerDao = autoIndexTrackerDao;
        this.autoIndexDocRefService = autoIndexDocRefService;
    }

    @Override
    public IndexJob getOrCreate(final String docRefUuid) throws Exception {

        // Get hold of the current state of the auto index, and its trackers
        final AutoIndexDocRefEntity autoIndex = autoIndexDocRefService.get(INTERNAL, docRefUuid)
                .orElseThrow(() -> new RuntimeException(String.format("Could not find Auto Index for UUID: %s", docRefUuid)));
        final AutoIndexTracker tracker = autoIndexTrackerDao.get(docRefUuid);



        return null;
    }

    @Override
    public List<IndexJob> getAll() {
        return null;
    }

    /**
     * Allow building of instances of the services using raw database credentials.
     * @param database The database to use
     * @return An instance of the DAO Impl using the given database
     */
    public static Builder withDatabase(final DSLContext database) {
        return new Builder(database);
    }

    public static class Builder {

        private final DSLContext database;
        private AutoIndexTrackerDao autoIndexTrackerDao;
        private AutoIndexDocRefServiceImpl autoIndexDocRefService;


        public Builder(final DSLContext database) {
            this.database = database;
        }

        public Builder autoIndexTrackerDao(final AutoIndexTrackerDao value) {
            this.autoIndexTrackerDao = value;
            return this;
        }

        public Builder autoIndexDocRefService(final AutoIndexDocRefServiceImpl value) {
            this.autoIndexDocRefService = value;
            return this;
        }

        public IndexJobDaoImpl build() {
            return new IndexJobDaoImpl(this);
        }
    }
}
