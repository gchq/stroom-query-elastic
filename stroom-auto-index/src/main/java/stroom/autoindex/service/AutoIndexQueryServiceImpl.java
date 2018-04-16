package stroom.autoindex.service;

import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.autoindex.QueryClientCache;
import stroom.tracking.TimelineTracker;
import stroom.tracking.TimelineTrackerService;
import stroom.datasource.api.v2.DataSource;
import stroom.query.api.v2.DocRef;
import stroom.query.api.v2.QueryKey;
import stroom.query.api.v2.SearchRequest;
import stroom.query.api.v2.SearchResponse;
import stroom.query.audit.rest.QueryResource;
import stroom.query.audit.security.ServiceUser;
import stroom.query.audit.service.DocRefService;
import stroom.query.audit.service.QueryApiException;
import stroom.query.audit.service.QueryService;

import javax.inject.Inject;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class AutoIndexQueryServiceImpl implements QueryService {
    private static final Logger LOGGER = LoggerFactory.getLogger(AutoIndexQueryServiceImpl.class);

    private final DocRefService<AutoIndexDocRefEntity> docRefService;

    private final QueryClientCache<QueryResource> queryClientCache;

    private final TimelineTrackerService trackerService;

    @Inject
    @SuppressWarnings("unchecked")
    public AutoIndexQueryServiceImpl(final DocRefService docRefService,
                                     final TimelineTrackerService trackerService,
                                     final QueryClientCache<QueryResource> queryClientCache) {
        this.docRefService = docRefService;
        this.queryClientCache = queryClientCache;
        this.trackerService = trackerService;
    }

    @Override
    public Optional<DataSource> getDataSource(final ServiceUser user,
                                              final DocRef docRef) throws QueryApiException {
        final Optional<AutoIndexDocRefEntity> docRefEntityOpt =
                docRefService.get(user, docRef.getUuid());
        if (!docRefEntityOpt.isPresent()) {
            return Optional.empty();
        }
        final AutoIndexDocRefEntity docRefEntity = docRefEntityOpt.get();

        final QueryResource rawClient = queryClientCache.apply(docRefEntity.getRawDocRef().getType())
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
                                           final SearchRequest request) throws QueryApiException {
        // Retrieve the full Auto Index Doc Ref for the request
        final String docRefUuid = request.getQuery().getDataSource().getUuid();
        final Optional<AutoIndexDocRefEntity> docRefEntityOpt =
                docRefService.get(user, docRefUuid);
        if (!docRefEntityOpt.isPresent()) {
            return Optional.empty();
        }
        final AutoIndexDocRefEntity docRefEntity = docRefEntityOpt.get();

        // Retrieve the tracker for this doc ref
        final TimelineTracker tracker = trackerService.get(docRefUuid);

        final SplitSearchRequest splitSearchRequest = SearchRequestSplitter.withSearchRequest(request)
                .autoIndex(docRefEntity)
                .tracker(tracker)
                .split();

        // Create the results merger
        final SearchResponseMerger merger = SearchResponseMerger.start();

        // Work through each split search request, sending it to the appropriate client and collate the results
        for (final Map.Entry<DocRef, List<SearchRequest>> requestEntry : splitSearchRequest.getRequests().entrySet()) {
            final QueryResource client = queryClientCache.apply(requestEntry.getKey().getType())
                    .orElseThrow(() -> new RuntimeException("Could not get HTTP Client for Query Resource"));

            // There may be several requests to send to each client, for fragmented windows.
            for (final SearchRequest partRequest : requestEntry.getValue()) {
                final Response partHttpResponse = client.search(user, partRequest);
                if (partHttpResponse.getStatus() == HttpStatus.OK_200) {
                    final SearchResponse partResponse = partHttpResponse.readEntity(SearchResponse.class);
                    merger.response(partResponse);
                } else {
                    LOGGER.warn("Invalid status returned by client", partHttpResponse.getStatus());
                    partHttpResponse.close();
                }
            }
        }

        return merger.merge();
    }

    @Override
    public Boolean destroy(final ServiceUser user,
                           final QueryKey queryKey) throws QueryApiException {
        return Boolean.TRUE;
    }

    @Override
    public Optional<DocRef> getDocRefForQueryKey(final ServiceUser user,
                                                 final QueryKey queryKey) throws QueryApiException {
        return Optional.empty();
    }
}
