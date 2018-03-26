package stroom.autoindex.indexing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * A Consumer of Index Jobs that simple marks any it receives as started then completed immediately.
 * It makes the list of IndexJobs that are accumulated and then extracted by the running tests.
 */
class TestIndexJobConsumer implements Consumer<IndexJob> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestIndexJobConsumer.class);

    private final IndexJobDao indexJobDao;

    private List<IndexJob> tasksToHandle = new ArrayList<>();

    @Inject
    public TestIndexJobConsumer(final IndexJobDao indexJobDao) {
        this.indexJobDao = indexJobDao;
    }

    @Override
    public void accept(final IndexJob indexJob) {
        LOGGER.debug("Handling Job " + indexJob);
        indexJobDao.markAsStarted(indexJob.getJobId());
        indexJobDao.markAsComplete(indexJob.getJobId());
        tasksToHandle.add(indexJob);
    }

    void clear() {
        tasksToHandle.clear();
    }

    List<IndexJob> extractJobs() {
        final List<IndexJob> listForRun = new ArrayList<>(tasksToHandle);
        this.clear();
        return listForRun;
    }
}
