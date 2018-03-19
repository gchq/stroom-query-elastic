package stroom.autoindex.animals;

import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import org.junit.ClassRule;
import stroom.autoindex.TestConstants;
import stroom.autoindex.animals.app.AnimalApp;
import stroom.autoindex.animals.app.AnimalConfig;
import stroom.autoindex.animals.app.AnimalDocRefEntity;
import stroom.query.testing.DocRefResourceIT;
import stroom.query.testing.DropwizardAppWithClientsRule;
import stroom.query.testing.StroomAuthenticationRule;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static io.dropwizard.testing.ResourceHelpers.resourceFilePath;

public class AnimalsDocRefResourceIT extends DocRefResourceIT<AnimalDocRefEntity, AnimalConfig> {

    @ClassRule
    public static final DropwizardAppWithClientsRule<AnimalConfig> appRule =
            new DropwizardAppWithClientsRule<>(AnimalApp.class, resourceFilePath(TestConstants.ANIMALS_APP_CONFIG));

    @ClassRule
    public static final StroomAuthenticationRule authRule =
            new StroomAuthenticationRule(
                    WireMockConfiguration.options().port(TestConstants.TEST_AUTH_PORT),
                    AnimalDocRefEntity.TYPE);

    public AnimalsDocRefResourceIT() {
        super(AnimalDocRefEntity.class,
                appRule,
                authRule);
    }

    @Override
    protected AnimalDocRefEntity createPopulatedEntity() {
        return new AnimalDocRefEntity.Builder()
                .dataDirectory(UUID.randomUUID().toString())
                .build();
    }

    @Override
    protected Map<String, String> exportValues(AnimalDocRefEntity docRefEntity) {
        final Map<String, String> values = new HashMap<>();
        values.put(AnimalDocRefEntity.DATA_DIRECTORY, docRefEntity.getDataDirectory());
        return values;
    }
}
