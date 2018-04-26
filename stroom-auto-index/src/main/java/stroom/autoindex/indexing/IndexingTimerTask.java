package stroom.autoindex.indexing;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.autoindex.app.IndexingConfig;
import stroom.autoindex.service.AutoIndexDocRefServiceImpl;
import stroom.query.audit.model.DocRefEntity;
import stroom.security.ServiceUser;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.Comparator;
import java.util.Optional;
import java.util.TimerTask;
import java.util.UUID;

import static stroom.autoindex.AutoIndexConstants.TASK_HANDLER_PARENT;

/**
 * This timer task is in charge of kicking off indexing tasks.
 * It looks for auto index documents that have unstarted indexing jobs.
 */
public class IndexingTimerTask extends TimerTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(IndexingTimerTask.class);

    private final IndexingConfig config;
    private final IndexJobDao indexJobDao;
    private final IndexJobHandler indexJobHandler;
    private final AutoIndexDocRefServiceImpl autoIndexDocRefService;
    private final ActorRef indexJobActorParent;
    private final ActorSystem actorSystem;

    private static final ServiceUser INTERNAL = new ServiceUser.Builder()
            .name(IndexJobDaoImpl.class.getName())
            .jwt(UUID.randomUUID().toString())
            .build();

    @Inject
    public IndexingTimerTask(final IndexingConfig config,
                             final IndexJobDao indexJobDao,
                             final IndexJobHandler indexJobHandler,
                             final AutoIndexDocRefServiceImpl autoIndexDocRefService,
                             final ActorSystem actorSystem,
                             @Named(TASK_HANDLER_PARENT)
                             final ActorRef indexJobActorParent) {
        this.config = config;
        this.indexJobDao = indexJobDao;
        this.indexJobHandler = indexJobHandler;
        this.autoIndexDocRefService = autoIndexDocRefService;
        this.actorSystem = actorSystem;
        this.indexJobActorParent = indexJobActorParent;
        LOGGER.info("Task Handler Parent {}", indexJobActorParent);
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
                    .map(IndexJobMessages::search) // wrap as messages for actor
                    .forEach(t -> {
                        final ActorRef indexJobActor =
                                actorSystem.actorOf(IndexJobActor.props(indexJobActorParent, indexJobHandler));
                        indexJobActor.tell(t, ActorRef.noSender());
                    });

        } catch (final Exception e) {
            LOGGER.error(e.getLocalizedMessage());
        }
    }
}
