package stroom.autoindex.indexing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.autoindex.AutoIndexDocRefEntity;
import stroom.autoindex.AutoIndexDocRefServiceImpl;
import stroom.autoindex.IndexingConfig;
import stroom.autoindex.tracker.AutoIndexTrackerDao;
import stroom.query.audit.security.ServiceUser;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.Comparator;
import java.util.TimerTask;
import java.util.UUID;
import java.util.function.Consumer;

/**
 * This timer task is in charge of kicking off indexing tasks.
 * It looks for auto index documents that have unstarted indexing jobs.
 */
public class IndexingTimerTask extends TimerTask {
    public static final String TASK_HANDLER_NAME = "IndexingTaskHandler";

    private static final Logger LOGGER = LoggerFactory.getLogger(IndexingTimerTask.class);

    private final IndexingConfig config;
    private final IndexJobDao indexJobDao;
    private final AutoIndexTrackerDao autoIndexTrackerDao;
    private final AutoIndexDocRefServiceImpl autoIndexDocRefService;
    private final Consumer<IndexJob> taskHandler;

    private static final ServiceUser INTERNAL = new ServiceUser.Builder()
            .name(IndexJobDaoImpl.class.getName())
            .jwt(UUID.randomUUID().toString())
            .build();

    @Inject
    public IndexingTimerTask(final IndexingConfig config,
                             final IndexJobDao indexJobDao,
                             final AutoIndexTrackerDao autoIndexTrackerDao,
                             final AutoIndexDocRefServiceImpl autoIndexDocRefService,
                             @Named(TASK_HANDLER_NAME)
                             final Consumer<IndexJob> taskHandler) {
        this.config = config;
        this.indexJobDao = indexJobDao;
        this.autoIndexTrackerDao = autoIndexTrackerDao;
        this.autoIndexDocRefService = autoIndexDocRefService;
        this.taskHandler = taskHandler;
    }

    @Override
    public void run() {
        try {
            LOGGER.debug("Running Indexing Timer Task ");

            // Get the list of unstarted index jobs
            autoIndexDocRefService.getAll(INTERNAL).stream()
                    .map(indexJobDao::getOrCreate)
                    .filter(j -> j.getStartedTimeMillis() == 0)
                    .sorted(Comparator.comparingLong(IndexJob::getCreatedTimeMillis)) // ensure fair rotation
                    .limit(config.getNumberOfTasksPerRun())
                    .forEach(taskHandler);

        } catch (final Exception e) {
            LOGGER.error(e.getLocalizedMessage());
        }
    }
}
