package stroom.autoindex;

import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;
import stroom.autoindex.animals.app.AnimalDocRefEntity;
import stroom.elastic.test.ElasticTestIndexRule;
import stroom.query.elastic.hibernate.ElasticIndexDocRefEntity;
import stroom.query.elastic.service.ElasticIndexDocRefServiceImpl;
import stroom.query.testing.DocRefResourceIT;
import stroom.query.testing.DropwizardAppWithClientsRule;
import stroom.query.testing.StroomAuthenticationRule;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static io.dropwizard.testing.ResourceHelpers.resourceFilePath;

public class AutoIndexDocRefResourceIT extends DocRefResourceIT<AutoIndexDocRefEntity, Config> {

    public static final String LOCAL_ELASTIC_HTTP_HOST = "localhost:19200";

    @ClassRule
    public static final DropwizardAppWithClientsRule<Config> appRule =
            new DropwizardAppWithClientsRule<>(App.class, resourceFilePath("autoindex/config.yml"));

    @ClassRule
    public static StroomAuthenticationRule authRule =
            new StroomAuthenticationRule(WireMockConfiguration.options().port(10080), AutoIndexDocRefEntity.TYPE);

    @ClassRule
    public static final ElasticTestIndexRule stroomIndexRule = ElasticTestIndexRule
            .forIndex(AutoIndexDocRefResourceIT.class, ElasticIndexDocRefServiceImpl.STROOM_INDEX_NAME)
            .httpUrl(LOCAL_ELASTIC_HTTP_HOST)
            .build();

    @ClassRule
    public static TemporaryFolder temporaryFolder;

    public AutoIndexDocRefResourceIT() {
        super(AutoIndexDocRefEntity.class,
                appRule,
                authRule);
    }

    @Override
    protected AutoIndexDocRefEntity createPopulatedEntity() {
        return new AutoIndexDocRefEntity.Builder()
                .indexedType(UUID.randomUUID().toString())
                .indexName(UUID.randomUUID().toString())
                .wrappedDataSourceURL("http://localhost:8199/")
                .wrappedDocRefType(AnimalDocRefEntity.TYPE)
                .wrappedDocRefUuid(UUID.randomUUID().toString())
                .build();
    }

    @Override
    protected Map<String, String> exportValues(final AutoIndexDocRefEntity docRefEntity) {
        final Map<String, String> values = new HashMap<>();
        values.put(ElasticIndexDocRefEntity.INDEX_NAME, docRefEntity.getIndexName());
        values.put(ElasticIndexDocRefEntity.INDEXED_TYPE, docRefEntity.getIndexedType());
        values.put(AutoIndexDocRefEntity.WRAPPED_DATASOURCE_URL, docRefEntity.getWrappedDataSourceURL());
        values.put(AutoIndexDocRefEntity.WRAPPED_DOC_REF_UUID, docRefEntity.getWrappedDocRefUuid());
        values.put(AutoIndexDocRefEntity.WRAPPED_DOC_REF_TYPE, docRefEntity.getWrappedDocRefType());
        return values;
    }
}
