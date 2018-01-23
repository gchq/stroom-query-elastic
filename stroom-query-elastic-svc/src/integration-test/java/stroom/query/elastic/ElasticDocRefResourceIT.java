package stroom.query.elastic;

import stroom.query.elastic.config.Config;
import stroom.query.elastic.hibernate.ElasticIndexDocRefEntity;
import stroom.query.testing.DocRefResourceIT;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class ElasticDocRefResourceIT
        extends DocRefResourceIT<ElasticIndexDocRefEntity, Config, App> {
    public ElasticDocRefResourceIT() {
        super(App.class, ElasticIndexDocRefEntity.class, ElasticIndexDocRefEntity.TYPE);
    }

    @Override
    protected ElasticIndexDocRefEntity createPopulatedEntity() {
        return new ElasticIndexDocRefEntity.Builder()
                .indexedType(UUID.randomUUID().toString())
                .indexName(UUID.randomUUID().toString())
                .build();
    }

    @Override
    protected Map<String, String> exportValues(final ElasticIndexDocRefEntity docRefEntity) {
        final Map<String, String> values = new HashMap<>();
        values.put(ElasticIndexDocRefEntity.INDEX_NAME, docRefEntity.getIndexName());
        values.put(ElasticIndexDocRefEntity.INDEXED_TYPE, docRefEntity.getIndexedType());
        return values;
    }
}
