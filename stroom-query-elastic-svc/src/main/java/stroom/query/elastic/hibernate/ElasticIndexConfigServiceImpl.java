package stroom.query.elastic.hibernate;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.util.Optional;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class ElasticIndexConfigServiceImpl implements ElasticIndexConfigService {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticIndexConfigServiceImpl.class);

    private final TransportClient client;

    @Inject
    public ElasticIndexConfigServiceImpl(final TransportClient client) {
        this.client = client;
    }

    @Override
    public Optional<ElasticIndexConfig> set(final String uuid, final ElasticIndexConfig update) {
        try {
            client.prepareIndex(STROOM_INDEX_NAME, DOC_REF_INDEXED_TYPE, uuid)
                    .setSource(jsonBuilder()
                            .startObject()
                            .field(ElasticIndexConfig.INDEX_NAME, update.getIndexName())
                            .field(ElasticIndexConfig.INDEXED_TYPE, update.getIndexedType())
                            .endObject()
                    )
                    .get();
        } catch (IOException e) {
            LOGGER.warn("Could not update index config", e);
            return Optional.empty();
        }

        return get(uuid);
    }

    @Override
    public Optional<ElasticIndexConfig> get(final String uuid) {
        GetResponse searchResponse = client
                .prepareGet(STROOM_INDEX_NAME, DOC_REF_INDEXED_TYPE, uuid)
                .get();

        if (searchResponse.isExists() && !searchResponse.isSourceEmpty()) {
            return Optional.of(new ElasticIndexConfig.Builder()
                    .indexName(searchResponse.getSource().get(ElasticIndexConfig.INDEX_NAME))
                    .indexedType(searchResponse.getSource().get(ElasticIndexConfig.INDEXED_TYPE))
                    .build());
        } else {
            return Optional.empty();
        }
    }

    @Override
    public void remove(String uuid) {
        client.prepareDelete(STROOM_INDEX_NAME, DOC_REF_INDEXED_TYPE, uuid).get();
    }
}
