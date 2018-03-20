package stroom.autoindex;

import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import stroom.autoindex.animals.app.AnimalDocRefEntity;
import stroom.autoindex.tracker.AutoIndexTracker;
import stroom.elastic.test.ElasticTestIndexRule;
import stroom.query.api.v2.DocRef;
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

    @ClassRule
    public static final DropwizardAppWithClientsRule<Config> appRule =
            new DropwizardAppWithClientsRule<>(App.class, resourceFilePath(TestConstants.AUTO_INDEX_APP_CONFIG));

    @ClassRule
    public static final StroomAuthenticationRule authRule =
            new StroomAuthenticationRule(WireMockConfiguration.options().port(TestConstants.TEST_AUTH_PORT));

    @ClassRule
    public static final ElasticTestIndexRule stroomIndexRule = ElasticTestIndexRule
            .forIndex(AutoIndexDocRefResourceIT.class, ElasticIndexDocRefServiceImpl.STROOM_INDEX_NAME)
            .httpUrl(TestConstants.LOCAL_ELASTIC_HTTP_HOST)
            .build();

    @Rule
    public final DeleteFromTableRule<Config> clearDbRule = DeleteFromTableRule.withApp(appRule)
            .table(AutoIndexDocRefEntity.TABLE_NAME)
            .table(AutoIndexTracker.TABLE_NAME)
            .build();

    @ClassRule
    public static TemporaryFolder temporaryFolder;

    public AutoIndexDocRefResourceIT() {
        super(AutoIndexDocRefEntity.TYPE,
                AutoIndexDocRefEntity.class,
                appRule,
                authRule);
    }

    @Override
    protected AutoIndexDocRefEntity createPopulatedEntity() {
        return new AutoIndexDocRefEntity.Builder()
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
    protected Map<String, String> exportValues(final AutoIndexDocRefEntity entity) {
        final Map<String, String> values = new HashMap<>();

        values.put(AutoIndexDocRefEntity.RAW_DOC_REF_TYPE.getName(), entity.getRawDocRef().getType());
        values.put(AutoIndexDocRefEntity.RAW_DOC_REF_UUID.getName(), entity.getRawDocRef().getUuid());
        values.put(AutoIndexDocRefEntity.RAW_DOC_REF_NAME.getName(), entity.getRawDocRef().getName());

        values.put(AutoIndexDocRefEntity.INDEX_DOC_REF_TYPE.getName(), entity.getIndexDocRef().getType());
        values.put(AutoIndexDocRefEntity.INDEX_DOC_REF_UUID.getName(), entity.getIndexDocRef().getUuid());
        values.put(AutoIndexDocRefEntity.INDEX_DOC_REF_NAME.getName(), entity.getIndexDocRef().getName());

        return values;
    }
}
