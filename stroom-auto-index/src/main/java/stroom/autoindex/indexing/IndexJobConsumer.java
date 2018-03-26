package stroom.autoindex.indexing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.function.Consumer;

public class IndexJobConsumer implements Consumer<IndexJob> {

    private static final Logger LOGGER = LoggerFactory.getLogger(IndexJobConsumer.class);

    private final IndexJobDao indexJobDao;

    @Inject
    public IndexJobConsumer(final IndexJobDao indexJobDao) {
        this.indexJobDao = indexJobDao;
    }

    @Override
    public void accept(final IndexJob indexJob) {
        LOGGER.debug("Handling Job " + indexJob);
        indexJobDao.markAsStarted(indexJob.getJobId());

    }
}
