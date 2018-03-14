package stroom.autoindex.animals;

import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import org.junit.ClassRule;
import stroom.autoindex.animals.app.AnimalDocRefEntity;
import stroom.autoindex.animals.app.AnimalApp;
import stroom.autoindex.animals.app.AnimalConfig;
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
            new DropwizardAppWithClientsRule<>(AnimalApp.class, resourceFilePath("animal/config.yml"));

    @ClassRule
    public static StroomAuthenticationRule authRule =
            new StroomAuthenticationRule(WireMockConfiguration.options().port(10080), AnimalDocRefEntity.TYPE);

    public AnimalsDocRefResourceIT() {
        super(AnimalDocRefEntity.class,
                appRule,
                authRule);
    }

    @Override
    protected AnimalDocRefEntity createPopulatedEntity() {
        return new AnimalDocRefEntity.Builder()
                .species(UUID.randomUUID().toString())
                .build();
    }

    @Override
    protected Map<String, String> exportValues(AnimalDocRefEntity docRefEntity) {
        final Map<String, String> values = new HashMap<>();
        values.put(AnimalDocRefEntity.SPECIES, docRefEntity.getSpecies());
        return values;
    }
}
