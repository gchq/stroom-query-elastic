package stroom.autoindex;

import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import org.eclipse.jetty.http.HttpStatus;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import stroom.autoindex.animals.AnimalTestData;
import stroom.autoindex.animals.app.AnimalApp;
import stroom.autoindex.animals.app.AnimalConfig;
import stroom.autoindex.animals.app.AnimalDocRefEntity;
import stroom.autoindex.animals.app.AnimalSighting;
import stroom.autoindex.app.App;
import stroom.autoindex.app.Config;
import stroom.autoindex.indexing.IndexJob;
import stroom.autoindex.service.AutoIndexDocRefEntity;
import stroom.autoindex.tracker.AutoIndexTracker;
import stroom.elastic.test.ElasticTestIndexRule;
import stroom.query.api.v2.DocRef;
import stroom.query.audit.authorisation.DocumentPermission;
import stroom.query.audit.client.DocRefResourceHttpClient;
import stroom.query.audit.client.QueryResourceHttpClient;
import stroom.query.audit.model.DocRefEntity;
import stroom.query.audit.rest.AuditedDocRefResourceImpl;
import stroom.query.audit.security.ServiceUser;
import stroom.query.elastic.model.ElasticIndexDocRefEntity;
import stroom.query.elastic.service.ElasticIndexDocRefServiceImpl;
import stroom.query.testing.DropwizardAppWithClientsRule;
import stroom.query.testing.FifoLogbackRule;
import stroom.query.testing.StroomAuthenticationRule;
import stroom.testdata.FlatFileTestDataRule;

import javax.ws.rs.core.Response;
import java.util.UUID;

import static io.dropwizard.testing.ResourceHelpers.resourceFilePath;
import static org.junit.Assert.assertEquals;
import static stroom.autoindex.TestConstants.TEST_SERVICE_USER;
import static stroom.query.testing.FifoLogbackRule.containsAllOf;

/**
 * Running Auto Index tests involves a lot of moving parts,
 * this abstract class sets up all of the moving parts required to conduct tests on the auto indexing application.
 * It includes the underlying raw and indexed data sources, test data for the raw source, various clients for connecting
 * to the REST API's, and the auth/audit log rules.
 */
public abstract class AbstractAutoIndexIntegrationTest {

    /**
     * Same auth rule across all 3 applications
     */
    @ClassRule
    public static final StroomAuthenticationRule authRule =
            new StroomAuthenticationRule(WireMockConfiguration.options().port(TestConstants.TEST_AUTH_PORT));

    protected static ServiceUser serviceUser;

    /**
     * The auto index application, it's client, and a rule to clear the doc ref database table
     */
    @ClassRule
    public static final DropwizardAppWithClientsRule<Config> autoIndexAppRule =
            new DropwizardAppWithClientsRule<>(App.class, resourceFilePath(TestConstants.AUTO_INDEX_APP_CONFIG_NO_INDEXING));

    private static final DocRefResourceHttpClient<AutoIndexDocRefEntity> autoIndexDocRefClient =
            new DocRefResourceHttpClient<>(TestConstants.AUTO_INDEX_APP_HOST_NO_INDEXING);

    protected static QueryResourceHttpClient autoIndexQueryClient =
            new QueryResourceHttpClient(TestConstants.AUTO_INDEX_APP_HOST_NO_INDEXING);

    public static final InitialiseJooqDbRule initialiseJooqDbRule = InitialiseJooqDbRule
            .withDataSourceFactory(() -> autoIndexAppRule.getConfiguration().getDataSourceFactory())
            .tableToClear(AutoIndexDocRefEntity.TABLE_NAME)
            .tableToClear(AutoIndexTracker.TRACKER_WINDOW_TABLE_NAME)
            .tableToClear(AutoIndexTracker.TIMELINE_BOUNDS_TABLE_NAME)
            .tableToClear(IndexJob.TABLE_NAME)
            .build();


    @Rule
    public final InitialiseJooqDbRule initialiseJooqDbRulePerTest = initialiseJooqDbRule;

    /**
     * The Elastic application, and it's client
     */
    @ClassRule
    public static final DropwizardAppWithClientsRule<stroom.query.elastic.config.Config> elasticAppRule =
            new DropwizardAppWithClientsRule<>(stroom.query.elastic.App.class, resourceFilePath(TestConstants.ELASTIC_APP_CONFIG));

    private static DocRefResourceHttpClient<ElasticIndexDocRefEntity> elasticDocRefClient =
            new DocRefResourceHttpClient<>(TestConstants.ELASTIC_APP_HOST);

    /**
     * The animals application, and it's client
     */
    @ClassRule
    public static final DropwizardAppWithClientsRule<AnimalConfig> animalsAppRule =
            new DropwizardAppWithClientsRule<>(AnimalApp.class, resourceFilePath(TestConstants.ANIMALS_APP_CONFIG));

    private static DocRefResourceHttpClient<AnimalDocRefEntity> animalDocRefClient =
            new DocRefResourceHttpClient<>(TestConstants.ANIMAL_APP_HOST);

    /**
     * Underlying test data for use by the animals application
     */
    @ClassRule
    public static final FlatFileTestDataRule testDataRule = FlatFileTestDataRule.withTempDirectory()
            .testDataGenerator(AnimalTestData.build())
            .numberOfFiles(10)
            .build();

    /**
     * This elastic index is used for the storage of Elastic Index doc refs
     */
    @ClassRule
    public static final ElasticTestIndexRule docRefElasticIndexRule = ElasticTestIndexRule
            .forIndex(AutoIndexQueryResourceIT.class, ElasticIndexDocRefServiceImpl.STROOM_INDEX_NAME)
            .httpUrl(TestConstants.LOCAL_ELASTIC_HTTP_HOST)
            .build();

    /**
     * This elastic index is used for the storage of Elastic Index doc refs
     */
    @ClassRule
    public static final ElasticTestIndexRule dataElasticIndexRule = ElasticTestIndexRule
            .forIndex(AutoIndexQueryResourceIT.class, TestConstants.TEST_DATA_INDEX)
            .httpUrl(TestConstants.LOCAL_ELASTIC_HTTP_HOST)
            .build();

    @Rule
    public FifoLogbackRule auditLogRule = new FifoLogbackRule();

    @BeforeClass
    public static void beforeAbstractClass() {
        serviceUser = authRule.authenticatedUser(TEST_SERVICE_USER);
    }

    protected class EntityWithDocRef<DOC_REF_ENTITY extends DocRefEntity> {
        private final DOC_REF_ENTITY entity;
        private final DocRef docRef;

        private EntityWithDocRef(final DOC_REF_ENTITY entity,
                                 final DocRef docRef) {
            this.entity = entity;
            this.docRef = docRef;
        }

        public DOC_REF_ENTITY getEntity() {
            return entity;
        }

        public DocRef getDocRef() {
            return docRef;
        }
    }

    /**
     * Creates the various doc refs required to support a single Auto Index.
     * It gives the admin user full access to all doc refs.
     * @return The AutoIndexDocRefEntity created, tied to the raw and indexed doc refs below it.
     */
    protected EntityWithDocRef<AutoIndexDocRefEntity> createAutoIndex() {
        final DocRef animalDocRef = createDocument(new AnimalDocRefEntity.Builder()
                .uuid(UUID.randomUUID().toString())
                .name(UUID.randomUUID().toString())
                .dataDirectory(testDataRule.getFolder().getAbsolutePath())
                .build());

        final DocRef elasticDocRef = createDocument(new ElasticIndexDocRefEntity.Builder()
                .uuid(UUID.randomUUID().toString())
                .name(UUID.randomUUID().toString())
                .indexedType(TestConstants.TEST_INDEXED_TYPE)
                .indexName(TestConstants.TEST_DATA_INDEX)
                .build());

        final AutoIndexDocRefEntity autoIndexDocRefEntity = new AutoIndexDocRefEntity.Builder()
                .uuid(UUID.randomUUID().toString())
                .name(UUID.randomUUID().toString())
                .rawDocRef(animalDocRef)
                .indexDocRef(elasticDocRef)
                .timeFieldName(AnimalSighting.STREAM_ID)
                .indexWindow(AnimalTestData.WINDOW_AMOUNT)
                .build();

        final DocRef autoIndexDocRef = createDocument(autoIndexDocRefEntity);

        return new EntityWithDocRef<>(autoIndexDocRefEntity, autoIndexDocRef);
    }

    /**
     * Creates a single document in one of the doc ref applications under test.
     * The arguments passed in determine which is being used.
     * @param docRefEntity The entity to create
     * @param <DOC_REF_ENTITY> The class of the doc ref entity, it will be use to determine which TYPE and CLIENT to use.
     * @return The Doc Ref of the document created
     */
    protected <DOC_REF_ENTITY extends DocRefEntity>
    DocRef createDocument(final DOC_REF_ENTITY docRefEntity) {
        final String docRefType;
        final DocRefResourceHttpClient<DOC_REF_ENTITY> docRefClient;

        if (docRefEntity instanceof AutoIndexDocRefEntity) {
            docRefType = AutoIndexDocRefEntity.TYPE;
            @SuppressWarnings("unchecked")
            DocRefResourceHttpClient<DOC_REF_ENTITY> c = (DocRefResourceHttpClient<DOC_REF_ENTITY>) autoIndexDocRefClient;
            docRefClient = c;
        } else if (docRefEntity instanceof AnimalDocRefEntity) {
            docRefType = AnimalDocRefEntity.TYPE;
            @SuppressWarnings("unchecked")
            DocRefResourceHttpClient<DOC_REF_ENTITY> c = (DocRefResourceHttpClient<DOC_REF_ENTITY>) animalDocRefClient;
            docRefClient = c;
        } else if (docRefEntity instanceof ElasticIndexDocRefEntity) {
            docRefType = ElasticIndexDocRefEntity.TYPE;
            @SuppressWarnings("unchecked")
            DocRefResourceHttpClient<DOC_REF_ENTITY> c = (DocRefResourceHttpClient<DOC_REF_ENTITY>) elasticDocRefClient;
            docRefClient = c;
        } else {
            final String msg = String.format("Cannot create documents for this class %s", docRefEntity.getClass());
            throw new IllegalArgumentException(msg);
        }

        // Generate UUID's for the doc ref and it's parent folder
        final String parentFolderUuid = UUID.randomUUID().toString();
        final DocRef docRef = new DocRef.Builder()
                .uuid(docRefEntity.getUuid())
                .type(docRefType)
                .name(docRefEntity.getName())
                .build();

        // Ensure admin user can create the document in the folder
        authRule.permitAdminUser()
                .createInFolder(parentFolderUuid)
                .docRefType(docRefType)
                .done();

        // Create a doc ref to hang the search from
        final Response createResponse = docRefClient.createDocument(
                authRule.adminUser(),
                docRef.getUuid(),
                docRef.getName(),
                parentFolderUuid);
        assertEquals(HttpStatus.OK_200, createResponse.getStatus());
        createResponse.close();

        // Give admin all the roles required to manipulate the document and it's underlying data
        authRule.permitAdminUser()
                .docRef(docRef)
                .permission(DocumentPermission.READ)
                .permission(DocumentPermission.UPDATE)
                .done();

        final Response updateIndexResponse =
                docRefClient.update(authRule.adminUser(),
                        docRef.getUuid(),
                        docRefEntity);
        assertEquals(HttpStatus.OK_200, updateIndexResponse.getStatus());
        updateIndexResponse.close();

        auditLogRule.check()
                .thereAreAtLeast(2)
                .containsOrdered(containsAllOf(AuditedDocRefResourceImpl.CREATE_DOC_REF, docRef.getUuid()))
                .containsOrdered(containsAllOf(AuditedDocRefResourceImpl.UPDATE_DOC_REF, docRef.getUuid()));

        return docRef;
    }
}
