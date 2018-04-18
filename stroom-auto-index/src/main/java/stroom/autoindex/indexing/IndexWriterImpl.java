package stroom.autoindex.indexing;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.autoindex.AutoIndexConstants;
import stroom.datasource.api.v2.DataSource;
import stroom.datasource.api.v2.DataSourceField;
import stroom.query.api.v2.DocRef;
import stroom.query.api.v2.FlatResult;
import stroom.query.api.v2.SearchResponse;
import stroom.query.audit.client.RemoteClientCache;
import stroom.query.audit.security.ServiceUser;
import stroom.query.audit.service.DocRefService;
import stroom.query.audit.service.QueryApiException;
import stroom.query.elastic.model.ElasticIndexDocRefEntity;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class IndexWriterImpl implements IndexWriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexWriterImpl.class);

    private final TransportClient client;
    private final DocRefService<ElasticIndexDocRefEntity> elasticDocRefService;
    private final ServiceUser serviceUser;

    @Inject
    public IndexWriterImpl(final TransportClient client,
                           final RemoteClientCache<DocRefService> docRefServiceCache,
                           @Named(AutoIndexConstants.STROOM_SERVICE_USER)
                           final ServiceUser serviceUser) {
        this.client = client;
        this.elasticDocRefService = (DocRefService<ElasticIndexDocRefEntity>) docRefServiceCache.apply(ElasticIndexDocRefEntity.TYPE)
                .orElseThrow(() -> new RuntimeException("Could not get Doc Ref Service"));
        this.serviceUser = serviceUser;
    }

    @Override
    public void writeResults(final DocRef elasticDocRef,
                             final DataSource dataSource,
                             final SearchResponse searchResponse) {
        final ElasticIndexDocRefEntity elasticIndex;
        try {
            elasticIndex = elasticDocRefService.get(serviceUser, elasticDocRef.getUuid())
                    .orElseThrow(() -> new RuntimeException("Could not get document entity for " + elasticDocRef));
        } catch (QueryApiException e) {
            throw new RuntimeException("Could not get document entity for " + elasticDocRef);
        }

        final BulkRequestBuilder bulkRequest = client.prepareBulk();
        final AtomicBoolean resultsFound = new AtomicBoolean(false);

        searchResponse.getResults().stream()
                .filter(r -> r instanceof FlatResult)
                .map(r -> (FlatResult) r)
                .flatMap(tr -> tr.getValues().stream())
                .forEach(r -> {
                    try {
                        final XContentBuilder b = jsonBuilder().startObject();
                        for (int x = 0; x < dataSource.getFields().size(); x++) {
                            final DataSourceField field = dataSource.getFields().get(x);
                            final Object value = r.get(3 + x); // skip over the system fields
                            b.field(field.getName(), value);
                        }
                        resultsFound.set(true);
                        b.endObject();
                        bulkRequest.add(
                                client.prepareIndex(elasticIndex.getIndexName(),
                                        elasticIndex.getIndexedType(),
                                        UUID.randomUUID().toString())
                                .setSource(b));
                    } catch (final IOException e) {
                        LOGGER.error("Could not add row to bulk request {}", e.getLocalizedMessage());
                    }
                });

        // Only try to write results if we have something to send
        if (resultsFound.get()) {
            final BulkResponse bulkResponse = bulkRequest.get();
            if (bulkResponse.hasFailures()) {
                // process failures by iterating through each bulk response item
                LOGGER.warn("Bulk Response has Failures {}", bulkResponse.buildFailureMessage());
            }
        }
    }
}
