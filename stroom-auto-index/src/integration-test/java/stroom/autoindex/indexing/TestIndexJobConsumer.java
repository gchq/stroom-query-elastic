package stroom.autoindex.indexing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

/**
 * A Consumer of Index Jobs that simple marks any it receives as started then completed immediately.
 * It makes the list of IndexJobs that are accumulated and then extracted by the running tests.
 */
class TestIndexJobConsumer implements IndexJobHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestIndexJobConsumer.class);

    private final IndexJobDao indexJobDao;

    @Inject
    public TestIndexJobConsumer(final IndexJobDao indexJobDao) {
        this.indexJobDao = indexJobDao;
    }

    @Override
    public IndexJob apply(final IndexJob indexJob) {
        LOGGER.debug("Handling Job " + indexJob);
        indexJobDao.markAsStarted(indexJob.getJobId());
        return indexJobDao.markAsComplete(indexJob.getJobId());
    }
}
