package stroom.autoindex;

import org.eclipse.jetty.http.HttpStatus;
import stroom.datasource.api.v2.DataSource;
import stroom.query.api.v2.*;
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

    private final Function<String, QueryResourceHttpClient> queryClientCache;

    @Inject
    public AutoIndexQueryServiceImpl(final DocRefService<AutoIndexDocRefEntity> docRefService,
                                     @Named(QUERY_HTTP_CLIENT_CACHE)
                                     final Function<String, QueryResourceHttpClient> queryClientCache) {
        this.docRefService = docRefService;
        this.queryClientCache = queryClientCache;
    }

    @Override
    public Optional<DataSource> getDataSource(final ServiceUser user,
                                              final DocRef docRef) throws Exception {
        return getUnderlyingDocRef(user, docRef.getUuid())
                .map(d -> d.client.getDataSource(user, d.docRef))
                .filter(r -> r.getStatus() == HttpStatus.OK_200)
                .map(r -> r.readEntity(DataSource.class));
    }

    @Override
    public Optional<SearchResponse> search(final ServiceUser user,
                                           final SearchRequest request) throws Exception {
        return getUnderlyingDocRef(user, request.getQuery().getDataSource().getUuid())
                .map(d -> {
                    // Translate the request to query the underlying document
                    final Query oQuery = request.getQuery();
                    final Query.Builder queryBuilder = new Query.Builder()
                            .expression(oQuery.getExpression());
                    oQuery.getParams().forEach(queryBuilder::addParams);
                    queryBuilder.dataSource(d.docRef);

                    final SearchRequest xRequest = new SearchRequest.Builder()
                            .dateTimeLocale(request.getDateTimeLocale())
                            .incremental(request.incremental())
                            .key(request.getKey())
                            .timeout(request.getTimeout())
                            .query(queryBuilder.build())
                            .build();

                    return d.client.search(user, xRequest);
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
     * @param outerDocRefUuid The UUID of the outer document
     * @return Effectively a tuple that contains the wrapped Doc Ref and a HTTP client for querying the underlying datasource
     * @throws Exception If something goes wrong
     */
    private Optional<UnderlyingDocRef> getUnderlyingDocRef(final ServiceUser user,
                                                           final String outerDocRefUuid) throws Exception {
        final Optional<AutoIndexDocRefEntity> docRefEntity = docRefService.get(user, outerDocRefUuid);

        if (!docRefEntity.isPresent()) {
            return Optional.empty();
        }

        final DocRef underlyingDocRef = docRefEntity
                .map(d -> new DocRef.Builder()
                        .type(d.getWrappedDocRefType())
                        .uuid(d.getWrappedDocRefUuid())
                        .build()
                )
                .orElseThrow(() -> new RuntimeException("Could not get underlying Doc Ref"));

        final QueryResourceHttpClient client = docRefEntity
                .map(AutoIndexDocRefEntity::getWrappedDataSourceURL)
                .map(queryClientCache)
                .orElseThrow(() -> new RuntimeException("Could not get HTTP Client for Query Resource"));

        return Optional.of(new UnderlyingDocRef(underlyingDocRef, client));
    }
}
