package stroom.query.akka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.datasource.api.v2.DataSource;
import stroom.query.api.v2.DocRef;
import stroom.query.api.v2.QueryKey;
import stroom.query.api.v2.SearchRequest;
import stroom.query.api.v2.SearchResponse;
import stroom.query.audit.security.ServiceUser;
import stroom.query.audit.service.QueryApiException;
import stroom.query.audit.service.QueryService;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;


/**
 * An empty query service that just keeps track of what calls were made to it
 */
public class TestQueryService implements QueryService {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestQueryService.class);

    private final SearchResponse searchResponse;

    private final List<SearchRequest> searchRequests;

    public TestQueryService() {
        this(null);
    }

    public TestQueryService(final SearchResponse searchResponse) {
        this.searchResponse = searchResponse;
        this.searchRequests = new ArrayList<>();
    }

    @Override
    public Optional<DataSource> getDataSource(final ServiceUser user,
                                              final DocRef docRef) throws QueryApiException {
        return Optional.empty();
    }

    @Override
    public Optional<SearchResponse> search(final ServiceUser user,
                                           final SearchRequest request) throws QueryApiException {
        LOGGER.info("Searching Test Query Service");
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        this.searchRequests.add(request);
        LOGGER.info("Ready to respond");
        return Optional.ofNullable(searchResponse);
    }

    @Override
    public Boolean destroy(final ServiceUser user,
                           final QueryKey queryKey) throws QueryApiException {
        return Boolean.FALSE;
    }

    @Override
    public Optional<DocRef> getDocRefForQueryKey(final ServiceUser user,
                                                 final QueryKey queryKey) throws QueryApiException {
        return Optional.empty();
    }

    public List<SearchRequest> getSearchRequests() {
        return searchRequests;
    }
}
