package stroom.query.elastic.noauth;

import org.junit.ClassRule;
import stroom.query.elastic.App;
import stroom.query.elastic.config.Config;
import stroom.query.elastic.hibernate.ElasticIndexDocRefEntity;
import stroom.query.testing.DocRefResourceNoAuthIT;
import stroom.query.testing.DropwizardAppWithClientsRule;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static io.dropwizard.testing.ResourceHelpers.resourceFilePath;

public class ElasticDocRefResourceNoAuthIT
        extends DocRefResourceNoAuthIT<ElasticIndexDocRefEntity, Config> {

    @ClassRule
    public static final DropwizardAppWithClientsRule<Config> appRule =
            new DropwizardAppWithClientsRule<>(App.class, resourceFilePath("config_noauth.yml"));

    public ElasticDocRefResourceNoAuthIT() {
        super(ElasticIndexDocRefEntity.class,
                appRule);
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
