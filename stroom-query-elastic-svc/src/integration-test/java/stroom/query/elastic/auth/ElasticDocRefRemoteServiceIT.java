package stroom.query.elastic.auth;

import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import org.junit.ClassRule;
import stroom.elastic.test.ElasticTestIndexRule;
import stroom.query.elastic.App;
import stroom.query.elastic.config.Config;
import stroom.query.elastic.model.ElasticIndexDocRefEntity;
import stroom.query.elastic.service.ElasticIndexDocRefServiceImpl;
import stroom.query.testing.DocRefRemoteServiceIT;
import stroom.query.testing.DropwizardAppWithClientsRule;
import stroom.query.testing.StroomAuthenticationRule;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static io.dropwizard.testing.ResourceHelpers.resourceFilePath;

public class ElasticDocRefRemoteServiceIT
        extends DocRefRemoteServiceIT<ElasticIndexDocRefEntity, Config> {

    public static final String LOCAL_ELASTIC_HTTP_HOST = "localhost:19200";

    @ClassRule
    public static final DropwizardAppWithClientsRule<Config> appRule =
            new DropwizardAppWithClientsRule<>(App.class, resourceFilePath("config_auth.yml"));

    @ClassRule
    public static final StroomAuthenticationRule authRule =
            new StroomAuthenticationRule(WireMockConfiguration.options().port(10080));

    @ClassRule
    public static final ElasticTestIndexRule stroomIndexRule = ElasticTestIndexRule
            .forIndex(ElasticDocRefRemoteServiceIT.class, ElasticIndexDocRefServiceImpl.STROOM_INDEX_NAME)
            .httpUrl(LOCAL_ELASTIC_HTTP_HOST)
            .build();

    public ElasticDocRefRemoteServiceIT() {
        super(ElasticIndexDocRefEntity.TYPE,
                ElasticIndexDocRefEntity.class,
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
