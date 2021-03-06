package stroom.autoindex.indexing;

import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.autoindex.AutoIndexConstants;
import stroom.autoindex.QueryClientCache;
import stroom.autoindex.app.IndexingConfig;
import stroom.autoindex.service.AutoIndexDocRefEntity;
import stroom.datasource.api.v2.DataSource;
import stroom.datasource.api.v2.DataSourceField;
import stroom.query.api.v2.DocRef;
import stroom.query.api.v2.ExpressionOperator;
import stroom.query.api.v2.ExpressionTerm;
import stroom.query.api.v2.Field;
import stroom.query.api.v2.OffsetRange;
import stroom.query.api.v2.Query;
import stroom.query.api.v2.ResultRequest;
import stroom.query.api.v2.SearchRequest;
import stroom.query.api.v2.SearchResponse;
import stroom.query.api.v2.TableSettings;
import stroom.query.audit.rest.QueryResource;
import stroom.query.audit.security.ServiceUser;

import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.core.Response;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;

public class IndexJobConsumer implements Consumer<IndexJob> {

    private static final Logger LOGGER = LoggerFactory.getLogger(IndexJobConsumer.class);

    private final IndexJobDao indexJobDao;

    private final IndexingConfig indexingConfig;

    private final QueryClientCache<QueryResource> queryClientCache;

    private final ServiceUser serviceUser;

    private final IndexWriter indexWriter;

    private final Function<DocRef, DataSource> fieldNamesCache = new Function<DocRef, DataSource>() {
        @Override
        public DataSource apply(final DocRef docRef) {

            final QueryResource rawClientOpt = queryClientCache.apply(docRef.getType())
                    .orElseThrow(() -> new RuntimeException("Could not retrieve query client for " + docRef.getType()));

            final Response response = rawClientOpt.getDataSource(serviceUser, docRef);

            if (response.getStatus() != HttpStatus.SC_OK) {
                response.close();
                throw new RuntimeException("Invalid response from Query Data Source " + response.getStatus());
            }

            return response.readEntity(DataSource.class);
        }
    };

    @Inject
    public IndexJobConsumer(final IndexJobDao indexJobDao,
                            final IndexingConfig indexingConfig,
                            final QueryClientCache<QueryResource> queryClientCache,
                            @Named(AutoIndexConstants.STROOM_SERVICE_USER)
                            final ServiceUser serviceUser,
                            final IndexWriter indexWriter) {
        this.indexJobDao = indexJobDao;
        this.indexingConfig = indexingConfig;
        this.queryClientCache = queryClientCache;
        this.serviceUser = serviceUser;
        this.indexWriter = indexWriter;
    }

    @Override
    public void accept(final IndexJob indexJob) {
        LOGGER.debug("Handling Job " + indexJob);
        indexJobDao.markAsStarted(indexJob.getJobId());

        final AutoIndexDocRefEntity autoIndex = indexJob.getAutoIndexDocRefEntity();
        final DocRef docRef = autoIndex.getRawDocRef();
        final QueryResource rawClientOpt = queryClientCache.apply(docRef.getType())
                .orElseThrow(() -> new RuntimeException("Could not retrieve query client for " + docRef.getType()));

        final String timeBoundTerm = String.format("%d,%d",
                indexJob.getTrackerWindow().getFrom(),
                indexJob.getTrackerWindow().getTo());

        final ExpressionOperator timeBound = new ExpressionOperator.Builder()
                .addTerm(autoIndex.getTimeFieldName(), ExpressionTerm.Condition.BETWEEN, timeBoundTerm)
                .build();

        final DataSource dataSource = fieldNamesCache.apply(docRef);
        final SearchRequest searchRequest = getSearchRequest(autoIndex.getRawDocRef(), dataSource, timeBound);

        final Response searchRawResponse = rawClientOpt.search(serviceUser, searchRequest);
        if (searchRawResponse.getStatus() != HttpStatus.SC_OK) {
            searchRawResponse.close();
            throw new RuntimeException("Invalid response from Query Search " + searchRawResponse.getStatus());
        }

        final SearchResponse searchResponse = searchRawResponse.readEntity(SearchResponse.class);

        indexWriter.writeResults(autoIndex.getIndexDocRef(), dataSource, searchResponse);

        indexJobDao.markAsComplete(indexJob.getJobId());
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
