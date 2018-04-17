package stroom.autoindex.indexing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.query.api.v2.SearchResponse;

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
    public SearchResponse search(final IndexJob indexJob) {
        LOGGER.info("Searching for Index Job {}", indexJob.getJobId());
        indexJobDao.markAsStarted(indexJob.getJobId());
        return new SearchResponse.FlatResultBuilder().build();
    }

    @Override
    public IndexJob write(final IndexJob indexJob,
                          final SearchResponse searchResponse) {
        LOGGER.info("Writing for Index Job {}", indexJob.getJobId());
        // Do nowt
        return indexJob;
    }

    @Override
    public IndexJob complete(final IndexJob indexJob) {
        LOGGER.info("Completing Index Job {}", indexJob.getJobId());
        return indexJobDao.markAsComplete(indexJob.getJobId());
    }
}
