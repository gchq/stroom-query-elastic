package stroom.autoindex.service;

import org.eclipse.jetty.http.HttpStatus;
import stroom.autoindex.QueryClientCache;
import stroom.autoindex.tracker.AutoIndexTracker;
import stroom.autoindex.tracker.AutoIndexTrackerDao;
import stroom.datasource.api.v2.DataSource;
import stroom.query.api.v2.*;
import stroom.query.audit.client.QueryResourceHttpClient;
import stroom.query.audit.security.ServiceUser;
import stroom.query.audit.service.DocRefService;
import stroom.query.audit.service.QueryService;

import javax.inject.Inject;
import javax.ws.rs.core.Response;
import java.util.Optional;

public class AutoIndexQueryServiceImpl implements QueryService {

    private final DocRefService<AutoIndexDocRefEntity> docRefService;

    private final QueryClientCache<QueryResourceHttpClient> queryClientCache;

    private final AutoIndexTrackerDao trackerDao;

    @Inject
    @SuppressWarnings("unchecked")
    public AutoIndexQueryServiceImpl(final DocRefService docRefService,
                                     final AutoIndexTrackerDao trackerDao,
                                     final QueryClientCache<QueryResourceHttpClient> queryClientCache) {
        this.docRefService = docRefService;
        this.queryClientCache = queryClientCache;
        this.trackerDao = trackerDao;
    }

    @Override
    public Optional<DataSource> getDataSource(final ServiceUser user,
                                              final DocRef docRef) throws Exception {
        final Optional<AutoIndexDocRefEntity> docRefEntityOpt =
                docRefService.get(user, docRef.getUuid());
        if (!docRefEntityOpt.isPresent()) {
            return Optional.empty();
        }
        final AutoIndexDocRefEntity docRefEntity = docRefEntityOpt.get();

        final QueryResourceHttpClient rawClient = queryClientCache.apply(docRefEntity.getRawDocRef().getType())
                .orElseThrow(() -> new RuntimeException("Could not get HTTP Client for Query Resource"));

        final Response response = rawClient.getDataSource(user, docRefEntity.getRawDocRef());

        if (response.getStatus() == HttpStatus.OK_200) {
            return Optional.of(response.readEntity(DataSource.class));
        } else {
            response.close();
            return Optional.empty();
        }
    }

    @Override
    public Optional<SearchResponse> search(final ServiceUser user,
                                           final SearchRequest request) throws Exception {
        final String docRefUuid = request.getQuery().getDataSource().getUuid();
        final Optional<AutoIndexDocRefEntity> docRefEntityOpt =
                docRefService.get(user, docRefUuid);
        if (!docRefEntityOpt.isPresent()) {
            return Optional.empty();
        }
        final AutoIndexDocRefEntity docRefEntity = docRefEntityOpt.get();

        final QueryResourceHttpClient rawClient = queryClientCache.apply(docRefEntity.getRawDocRef().getType())
                .orElseThrow(() -> new RuntimeException("Could not get HTTP Client for Query Resource"));

        final QueryResourceHttpClient indexClient = queryClientCache.apply(docRefEntity.getIndexDocRef().getType())
                .orElseThrow(() -> new RuntimeException("Could not get HTTP Client for Query Resource"));

        final AutoIndexTracker tracker = trackerDao.get(docRefUuid);

        // Get access to the input query
        final Query inputQuery = request.getQuery();

        // Create a query for the raw data source
        final Query.Builder rawQueryBuilder = new Query.Builder()
                .expression(inputQuery.getExpression());
        inputQuery.getParams().forEach(rawQueryBuilder::addParams);
        rawQueryBuilder.dataSource(docRefEntity.getRawDocRef());

        final SearchRequest.Builder rawSearchRequestBuilder = new SearchRequest.Builder()
                .dateTimeLocale(request.getDateTimeLocale())
                .incremental(request.incremental())
                .key(request.getKey())
                .timeout(request.getTimeout())
                .query(rawQueryBuilder.build());

        request.getResultRequests().forEach(rawSearchRequestBuilder::addResultRequests);

        final Response rawResponse = rawClient.search(user, rawSearchRequestBuilder.build());

        if (rawResponse.getStatus() == HttpStatus.OK_200) {
            final SearchResponse rawSearchResponse = rawResponse.readEntity(SearchResponse.class);
            return Optional.of(rawSearchResponse);
        } else {
            rawResponse.close();
            return Optional.empty();
        }
    }

    @Override
    public Boolean destroy(final ServiceUser user,
                           final QueryKey queryKey) throws Exception {
        return Boolean.TRUE;
    }

    @Override
    public Optional<DocRef> getDocRefForQueryKey(final ServiceUser user,
                                                 final QueryKey queryKey) throws Exception {
        return Optional.empty();
    }
}
