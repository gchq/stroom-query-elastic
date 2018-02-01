package stroom.query.elastic.auth;

import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import org.junit.ClassRule;
import stroom.query.elastic.App;
import stroom.query.elastic.config.Config;
import stroom.query.elastic.hibernate.ElasticIndexDocRefEntity;
import stroom.query.testing.DocRefResourceIT;
import stroom.query.testing.DropwizardAppWithClientsRule;
import stroom.query.testing.StroomAuthenticationRule;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static io.dropwizard.testing.ResourceHelpers.resourceFilePath;

public class ElasticDocRefResourceIT
        extends DocRefResourceIT<ElasticIndexDocRefEntity, Config> {

    @ClassRule
    public static final DropwizardAppWithClientsRule<Config> appRule =
            new DropwizardAppWithClientsRule<>(App.class, resourceFilePath("config_auth.yml"));

    @ClassRule
    public static StroomAuthenticationRule authRule =
            new StroomAuthenticationRule(WireMockConfiguration.options().port(10080), ElasticIndexDocRefEntity.TYPE);

    public ElasticDocRefResourceIT() {
        super(ElasticIndexDocRefEntity.class,
                appRule,
                authRule);
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
