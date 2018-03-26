package stroom.autoindex;

import org.eclipse.jetty.http.HttpStatus;
import stroom.datasource.api.v2.DataSource;
import stroom.query.api.v2.DocRef;
import stroom.query.api.v2.Query;
import stroom.query.api.v2.QueryKey;
import stroom.query.api.v2.SearchRequest;
import stroom.query.api.v2.SearchResponse;
import stroom.query.audit.client.QueryResourceHttpClient;
import stroom.query.audit.security.ServiceUser;
import stroom.query.audit.service.DocRefService;
import stroom.query.audit.service.QueryService;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.Optional;
import java.util.function.Function;

public class AutoIndexQueryServiceImpl implements QueryService {

    public static final String QUERY_HTTP_CLIENT_CACHE = "queryHttpClientCache";

    private final DocRefService<AutoIndexDocRefEntity> docRefService;

    private final Function<String, Optional<QueryResourceHttpClient>> queryClientCache;

    @Inject
    @SuppressWarnings("unchecked")
    public AutoIndexQueryServiceImpl(final DocRefService docRefService,
                                     @Named(QUERY_HTTP_CLIENT_CACHE)
                                     final Function<String, Optional<QueryResourceHttpClient>> queryClientCache) {
        this.docRefService = docRefService;
        this.queryClientCache = queryClientCache;
    }

    @Override
    public Optional<DataSource> getDataSource(final ServiceUser user,
                                              final DocRef docRef) throws Exception {
        return getUnderlyingDocRef(user, docRef.getUuid(), AutoIndexDocRefEntity::getRawDocRef)
                .map(d -> d.client.getDataSource(user, d.docRef))
                .filter(r -> r.getStatus() == HttpStatus.OK_200)
                .map(r -> r.readEntity(DataSource.class));
    }

    @Override
    public Optional<SearchResponse> search(final ServiceUser user,
                                           final SearchRequest request) throws Exception {
        return getUnderlyingDocRef(user, request.getQuery().getDataSource().getUuid(), AutoIndexDocRefEntity::getRawDocRef)
                .map(d -> {
                    // Translate the request to query the underlying document
                    final Query oQuery = request.getQuery();
                    final Query.Builder queryBuilder = new Query.Builder()
                            .expression(oQuery.getExpression());
                    oQuery.getParams().forEach(queryBuilder::addParams);
                    queryBuilder.dataSource(d.docRef);

                    final SearchRequest.Builder xRequestBuilder = new SearchRequest.Builder()
                            .dateTimeLocale(request.getDateTimeLocale())
                            .incremental(request.incremental())
                            .key(request.getKey())
                            .timeout(request.getTimeout())
                            .query(queryBuilder.build());

                    request.getResultRequests().forEach(xRequestBuilder::addResultRequests);

                    return d.client.search(user, xRequestBuilder.build());
                })
                .filter(r -> r.getStatus() == HttpStatus.OK_200)
                .map(r -> r.readEntity(SearchResponse.class));
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

    /**
     * Used to wrap an underlying doc ref up with a client that can be used to communicate with the
     * underlying service that owns the doc ref.
     */
    private class UnderlyingDocRef {
        private final DocRef docRef;
        private final QueryResourceHttpClient client;

        public UnderlyingDocRef(final DocRef docRef,
                                final QueryResourceHttpClient client) {
            this.docRef = docRef;
            this.client = client;
        }
    }

    /**
     * Used to take the outer doc ref UUID and lookup the wrapped Doc Ref details.
     * @param user The logged in user
     * @param docRefExtractor Method on AutoIndexDocRefEntity for pulling out the doc ref
     * @param outerDocRefUuid The UUID of the outer document
     * @return Effectively a tuple that contains the wrapped Doc Ref and a HTTP client for querying the underlying datasource
     * @throws Exception If something goes wrong
     */
    private Optional<UnderlyingDocRef> getUnderlyingDocRef(final ServiceUser user,
                                                           final String outerDocRefUuid,
                                                           final Function<AutoIndexDocRefEntity, DocRef> docRefExtractor) throws Exception {
        final Optional<AutoIndexDocRefEntity> docRefEntity = docRefService.get(user, outerDocRefUuid);

        if (!docRefEntity.isPresent()) {
            return Optional.empty();
        }

        final DocRef rawDocRef = docRefEntity
                .map(docRefExtractor)
                .orElseThrow(() -> new RuntimeException("Underlying Doc ref not specified"));

        final QueryResourceHttpClient client = queryClientCache.apply(rawDocRef.getType())
                .orElseThrow(() -> new RuntimeException("Could not get HTTP Client for Query Resource"));

        return Optional.of(new UnderlyingDocRef(rawDocRef, client));
    }
}
