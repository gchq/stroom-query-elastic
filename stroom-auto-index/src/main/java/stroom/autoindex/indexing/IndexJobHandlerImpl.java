package stroom.autoindex.indexing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.autoindex.AutoIndexConstants;
import stroom.autoindex.app.IndexingConfig;
import stroom.autoindex.service.AutoIndexDocRefEntity;
import stroom.datasource.api.v2.DataSource;
import stroom.datasource.api.v2.DataSourceField;
import stroom.query.api.v2.*;
import stroom.query.audit.client.RemoteClientCache;
import stroom.query.audit.security.ServiceUser;
import stroom.query.audit.service.QueryApiException;
import stroom.query.audit.service.QueryService;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.UUID;
import java.util.function.Function;

public class IndexJobHandlerImpl implements IndexJobHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(IndexJobHandlerImpl.class);

    private final IndexJobDao indexJobDao;

    private final IndexingConfig indexingConfig;

    private final RemoteClientCache<QueryService> remoteClientCache;

    private final ServiceUser serviceUser;

    private final IndexWriter indexWriter;

    private final Function<DocRef, DataSource> fieldNamesCache = new Function<DocRef, DataSource>() {
        @Override
        public DataSource apply(final DocRef docRef) {

            final QueryService rawClient = remoteClientCache.apply(docRef.getType())
                    .orElseThrow(() -> new RuntimeException("Could not retrieve query client for " + docRef.getType()));

            try {
                return rawClient.getDataSource(serviceUser, docRef)
                        .orElseThrow(() -> new RuntimeException("Could not retrieve datasource " + docRef.getType()));
            } catch (final QueryApiException e) {
                throw new RuntimeException("Couldn't retrieve datasource", e);
            }
        }
    };

    @Inject
    public IndexJobHandlerImpl(final IndexJobDao indexJobDao,
                               final IndexingConfig indexingConfig,
                               final RemoteClientCache<QueryService> remoteClientCache,
                               @Named(AutoIndexConstants.STROOM_SERVICE_USER)
                               final ServiceUser serviceUser,
                               final IndexWriter indexWriter) {
        this.indexJobDao = indexJobDao;
        this.indexingConfig = indexingConfig;
        this.remoteClientCache = remoteClientCache;
        this.serviceUser = serviceUser;
        this.indexWriter = indexWriter;
    }

    @Override
    public SearchResponse search(IndexJob indexJob) {
        LOGGER.debug("Handling Job " + indexJob);
        indexJobDao.markAsStarted(indexJob.getJobId());

        final AutoIndexDocRefEntity autoIndex = indexJob.getAutoIndexDocRefEntity();
        final DocRef docRef = autoIndex.getRawDocRef();

        final QueryService rawClient = remoteClientCache.apply(docRef.getType())
                .orElseThrow(() -> new RuntimeException("Could not retrieve query client for " + docRef.getType()));

        final String timeBoundTerm = String.format("%d,%d",
                indexJob.getTrackerWindow().getFrom(),
                indexJob.getTrackerWindow().getTo());

        final ExpressionOperator timeBound = new ExpressionOperator.Builder()
                .addTerm(autoIndex.getTimeFieldName(), ExpressionTerm.Condition.BETWEEN, timeBoundTerm)
                .build();

        final DataSource dataSource = fieldNamesCache.apply(docRef);
        final SearchRequest searchRequest = getSearchRequest(autoIndex.getRawDocRef(), dataSource, timeBound);

        try {
            return rawClient.search(serviceUser, searchRequest)
                    .orElseThrow(() -> new RuntimeException("Could not search " + docRef.getType()));
        } catch (final QueryApiException e) {
            throw new RuntimeException("Couldn't search", e);
        }
    }

    @Override
    public IndexJob write(final IndexJob indexJob,
                          final SearchResponse searchResponse) {
        indexJobDao.markAsComplete(indexJob.getJobId());

        final AutoIndexDocRefEntity autoIndex = indexJob.getAutoIndexDocRefEntity();
        final DocRef docRef = autoIndex.getRawDocRef();

        final DataSource dataSource = fieldNamesCache.apply(docRef);
        indexWriter.writeResults(autoIndex.getIndexDocRef(), dataSource, searchResponse);

        return indexJob;
    }

    private SearchRequest getSearchRequest(final DocRef docRef,
                                           final DataSource dataSource,
                                           final ExpressionOperator expressionOperator) {
        final String queryKey = UUID.randomUUID().toString();

        final TableSettings.Builder tableSettingsBuilder = new TableSettings.Builder()
                .queryId(queryKey)
                .extractValues(false)
                .showDetail(false)
                .addMaxResults(1000);

        dataSource.getFields().stream()
                .map(DataSourceField::getName)
                .forEach(fieldName -> {
                    tableSettingsBuilder.addFields(new Field.Builder()
                            .name(fieldName)
                            .expression(String.format("${%s}", fieldName))
                            .build());
                });

        return new SearchRequest.Builder()
                .query(new Query.Builder()
                        .dataSource(docRef)
                        .expression(expressionOperator)
                        .build())
                .addResultRequests(new ResultRequest.Builder()
                        .fetch(ResultRequest.Fetch.ALL)
                        .resultStyle(ResultRequest.ResultStyle.FLAT)
                        .componentId("componentId")
                        .requestedRange(new OffsetRange.Builder()
                                .length(1000L)
                                .offset(0L)
                                .build())
                        .addMappings(tableSettingsBuilder.build())
                        .build())
                .key(queryKey)
                .dateTimeLocale("en-gb")
                .incremental(true)
                .addResultRequests()
                .build();
    }
}
