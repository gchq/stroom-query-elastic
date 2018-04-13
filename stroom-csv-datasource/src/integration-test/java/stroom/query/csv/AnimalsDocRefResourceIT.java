package stroom.query.csv;

import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import org.junit.ClassRule;
import stroom.query.testing.DocRefResourceIT;
import stroom.query.testing.DropwizardAppWithClientsRule;
import stroom.query.testing.StroomAuthenticationRule;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static io.dropwizard.testing.ResourceHelpers.resourceFilePath;

public class AnimalsDocRefResourceIT extends DocRefResourceIT<CsvDocRefEntity, CsvConfig> {

    @ClassRule
    public static final DropwizardAppWithClientsRule<CsvConfig> appRule =
            new DropwizardAppWithClientsRule<>(AnimalsApp.class, resourceFilePath(TestConstants.APP_CONFIG));

    @ClassRule
    public static final StroomAuthenticationRule authRule =
            new StroomAuthenticationRule(WireMockConfiguration.options().port(TestConstants.TEST_AUTH_PORT));

    public AnimalsDocRefResourceIT() {
        super(CsvDocRefEntity.TYPE,
                CsvDocRefEntity.class,
                appRule,
                authRule);
    }

    @Override
    protected CsvDocRefEntity createPopulatedEntity() {
        return new CsvDocRefEntity.Builder()
                .dataDirectory(UUID.randomUUID().toString())
                .build();
    }

    @Override
    protected Map<String, String> exportValues(CsvDocRefEntity docRefEntity) {
        final Map<String, String> values = new HashMap<>();
        values.put(CsvDocRefEntity.DATA_DIRECTORY, docRefEntity.getDataDirectory());
        return values;
    }
}
