package stroom.autoindex;

import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;
import stroom.autoindex.animals.app.AnimalDocRefEntity;
import stroom.elastic.test.ElasticTestIndexRule;
import stroom.query.api.v2.DocRef;
import stroom.query.audit.model.DocRefEntity;
import stroom.query.elastic.hibernate.ElasticIndexDocRefEntity;
import stroom.query.elastic.service.ElasticIndexDocRefServiceImpl;
import stroom.query.testing.DocRefResourceIT;
import stroom.query.testing.DropwizardAppWithClientsRule;
import stroom.query.testing.StroomAuthenticationRule;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static io.dropwizard.testing.ResourceHelpers.resourceFilePath;

public class AutoIndexDocRefResourceIT extends DocRefResourceIT<AutoIndexDocRefEntity, Config> {

    @ClassRule
    public static final DropwizardAppWithClientsRule<Config> appRule =
            new DropwizardAppWithClientsRule<>(App.class, resourceFilePath(TestConstants.AUTO_INDEX_APP_CONFIG));

    @ClassRule
    public static final StroomAuthenticationRule authRule =
            new StroomAuthenticationRule(
                    WireMockConfiguration.options().port(TestConstants.TEST_AUTH_PORT),
                    AutoIndexDocRefEntity.TYPE);

    @ClassRule
    public static final ElasticTestIndexRule stroomIndexRule = ElasticTestIndexRule
            .forIndex(AutoIndexDocRefResourceIT.class, ElasticIndexDocRefServiceImpl.STROOM_INDEX_NAME)
            .httpUrl(TestConstants.LOCAL_ELASTIC_HTTP_HOST)
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
                .rawDocRef(new DocRef.Builder()
                        .uuid(UUID.randomUUID().toString())
                        .type(AnimalDocRefEntity.TYPE)
                        .name(UUID.randomUUID().toString())
                        .build())
                .indexDocRef(new DocRef.Builder()
                        .uuid(UUID.randomUUID().toString())
                        .type(ElasticIndexDocRefEntity.TYPE)
                        .name(UUID.randomUUID().toString())
                        .build())
                .build();
    }

    @Override
    protected Map<String, String> exportValues(final AutoIndexDocRefEntity docRefEntity) {
        final Map<String, String> values = new HashMap<>();
        Stream.of(
                new AbstractMap.SimpleEntry<>(AutoIndexDocRefEntity.RAW_PREFIX, docRefEntity.getRawDocRef()),
                new AbstractMap.SimpleEntry<>(AutoIndexDocRefEntity.INDEX_PREFIX, docRefEntity.getIndexDocRef())
        )
                .forEach(entry -> {
                    values.put(entry.getKey() + DocRefEntity.UUID, entry.getValue().getUuid());
                    values.put(entry.getKey() + DocRefEntity.NAME, entry.getValue().getName());
                    values.put(entry.getKey() + AutoIndexDocRefEntity.DOC_REF_TYPE, entry.getValue().getType());
                });

        values.put(ElasticIndexDocRefEntity.INDEX_NAME, docRefEntity.getIndexName());
        values.put(ElasticIndexDocRefEntity.INDEXED_TYPE, docRefEntity.getIndexedType());
        return values;
    }
}
