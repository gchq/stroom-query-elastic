package stroom.autoindex.indexing;

import akka.actor.ActorRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.autoindex.app.IndexingConfig;
import stroom.autoindex.service.AutoIndexDocRefServiceImpl;
import stroom.query.audit.model.DocRefEntity;
import stroom.query.audit.security.ServiceUser;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.Comparator;
import java.util.Optional;
import java.util.TimerTask;
import java.util.UUID;

import static stroom.autoindex.AutoIndexConstants.TASK_HANDLER_NAME;

/**
 * This timer task is in charge of kicking off indexing tasks.
 * It looks for auto index documents that have unstarted indexing jobs.
 */
public class IndexingTimerTask extends TimerTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(IndexingTimerTask.class);

    private final IndexingConfig config;
    private final IndexJobDao indexJobDao;
    private final AutoIndexDocRefServiceImpl autoIndexDocRefService;
    private final ActorRef indexJobActor;

    private static final ServiceUser INTERNAL = new ServiceUser.Builder()
            .name(IndexJobDaoImpl.class.getName())
            .jwt(UUID.randomUUID().toString())
            .build();

    @Inject
    public IndexingTimerTask(final IndexingConfig config,
                             final IndexJobDao indexJobDao,
                             final AutoIndexDocRefServiceImpl autoIndexDocRefService,
                             @Named(TASK_HANDLER_NAME)
                             final ActorRef indexJobActor) {
        this.config = config;
        this.indexJobDao = indexJobDao;
        this.autoIndexDocRefService = autoIndexDocRefService;
        this.indexJobActor = indexJobActor;
    }

    @Override
    public void run() {
        try {
            LOGGER.debug("Running Indexing Timer Task ");

            // Get the list of unstarted index jobs
            autoIndexDocRefService.getAll(INTERNAL).stream()
                    .map(DocRefEntity::getUuid)
                    .map(indexJobDao::getOrCreate)
                    .filter(Optional::isPresent).map(Optional::get)
                    .filter(j -> j.getStartedTimeMillis() == 0)
                    .sorted(Comparator.comparingLong(IndexJob::getCreatedTimeMillis)) // ensure fair rotation
                    .limit(config.getNumberOfTasksPerRun())
                    .forEach(t -> indexJobActor.tell(t, ActorRef.noSender()));

        } catch (final Exception e) {
            LOGGER.error(e.getLocalizedMessage());
        }
    }
}
