package stroom.autoindex;

import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import org.junit.ClassRule;
import stroom.autoindex.animals.AnimalTestData;
import stroom.autoindex.animals.app.AnimalApp;
import stroom.autoindex.animals.app.AnimalConfig;
import stroom.autoindex.animals.app.AnimalDocRefEntity;
import stroom.datasource.api.v2.DataSource;
import stroom.elastic.test.ElasticTestIndexRule;
import stroom.query.api.v2.DocRef;
import stroom.query.api.v2.ExpressionOperator;
import stroom.query.api.v2.OffsetRange;
import stroom.query.api.v2.SearchRequest;
import stroom.query.elastic.service.ElasticIndexDocRefServiceImpl;
import stroom.query.testing.DropwizardAppWithClientsRule;
import stroom.query.testing.QueryResourceIT;
import stroom.query.testing.StroomAuthenticationRule;
import stroom.testdata.FlatFileTestDataRule;

import java.util.UUID;

import static io.dropwizard.testing.ResourceHelpers.resourceFilePath;

public class AutoIndexQueryResourceIT extends QueryResourceIT<AutoIndexDocRefEntity, Config> {

    public static final String LOCAL_ELASTIC_HTTP_HOST = "localhost:19200";

    @ClassRule
    public static final DropwizardAppWithClientsRule<Config> autoIndexAppRule =
            new DropwizardAppWithClientsRule<>(App.class, resourceFilePath("autoindex/config.yml"));

    @ClassRule
    public static final StroomAuthenticationRule autoIndexAuthRule =
            new StroomAuthenticationRule(WireMockConfiguration.options().port(11080), AutoIndexDocRefEntity.TYPE);

    @ClassRule
    public static final DropwizardAppWithClientsRule<AnimalConfig> animalsAppRule =
            new DropwizardAppWithClientsRule<>(AnimalApp.class, resourceFilePath("animal/config.yml"));

    @ClassRule
    public static final StroomAuthenticationRule animalsAuthRule =
            new StroomAuthenticationRule(WireMockConfiguration.options().port(10080), AnimalDocRefEntity.TYPE);

    @ClassRule
    public static final FlatFileTestDataRule testDataRule = FlatFileTestDataRule.withTempDirectory()
            .testDataGenerator(AnimalTestData.build())
            .build();

    @ClassRule
    public static final ElasticTestIndexRule stroomIndexRule = ElasticTestIndexRule
            .forIndex(AutoIndexQueryResourceIT.class, ElasticIndexDocRefServiceImpl.STROOM_INDEX_NAME)
            .httpUrl(LOCAL_ELASTIC_HTTP_HOST)
            .build();

    public AutoIndexQueryResourceIT() {
        super(AutoIndexDocRefEntity.class, AutoIndexDocRefEntity.TYPE, autoIndexAppRule, autoIndexAuthRule);
    }

    @Override
    protected SearchRequest getValidSearchRequest(final DocRef docRef,
                                                  final ExpressionOperator expressionOperator,
                                                  final OffsetRange offsetRange) {
        return null;
    }

    @Override
    protected void assertValidDataSource(final DataSource dataSource) {

    }

    @Override
    protected AutoIndexDocRefEntity getValidEntity(final DocRef docRef) {
        return new AutoIndexDocRefEntity.Builder()
                .wrappedDataSourceURL(String.format("http://localhost:%d/", animalsAppRule.getLocalPort()))
                .wrappedDocRefUuid(UUID.randomUUID().toString())
                .wrappedDocRefType(AnimalDocRefEntity.TYPE)
                .build();
    }
}
