package stroom.autoindex;

import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import org.eclipse.jetty.http.HttpStatus;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.autoindex.animals.AnimalTestData;
import stroom.autoindex.animals.AnimalsQueryResourceIT;
import stroom.autoindex.animals.app.AnimalApp;
import stroom.autoindex.animals.app.AnimalConfig;
import stroom.autoindex.animals.app.AnimalDocRefEntity;
import stroom.autoindex.animals.app.AnimalSighting;
import stroom.datasource.api.v2.DataSource;
import stroom.datasource.api.v2.DataSourceField;
import stroom.elastic.test.ElasticTestIndexRule;
import stroom.query.api.v2.*;
import stroom.query.audit.authorisation.DocumentPermission;
import stroom.query.audit.client.DocRefResourceHttpClient;
import stroom.query.audit.client.QueryResourceHttpClient;
import stroom.query.audit.model.DocRefEntity;
import stroom.query.audit.rest.AuditedDocRefResourceImpl;
import stroom.query.audit.rest.AuditedQueryResourceImpl;
import stroom.query.elastic.hibernate.ElasticIndexDocRefEntity;
import stroom.query.elastic.service.ElasticIndexDocRefServiceImpl;
import stroom.query.testing.DropwizardAppWithClientsRule;
import stroom.query.testing.FifoLogbackRule;
import stroom.query.testing.StroomAuthenticationRule;
import stroom.testdata.FlatFileTestDataRule;

import javax.ws.rs.core.Response;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static io.dropwizard.testing.ResourceHelpers.resourceFilePath;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static stroom.query.testing.FifoLogbackRule.containsAllOf;

public class AutoIndexQueryResourceIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(AutoIndexQueryResourceIT.class);

    /**
     * Same auth rule across all 3 applications
     */
    @ClassRule
    public static final StroomAuthenticationRule authRule =
            new StroomAuthenticationRule(
                    WireMockConfiguration.options().port(TestConstants.TEST_AUTH_PORT),
                    AutoIndexDocRefEntity.TYPE);

    /**
     * The auto index application, it's client, and a rule to clear the doc ref database table
     */
    @ClassRule
    public static final DropwizardAppWithClientsRule<Config> autoIndexAppRule =
            new DropwizardAppWithClientsRule<>(App.class, resourceFilePath(TestConstants.AUTO_INDEX_APP_CONFIG));

    private static DocRefResourceHttpClient<AutoIndexDocRefEntity> autoIndexDocRefClient =
            new DocRefResourceHttpClient<>(TestConstants.AUTO_INDEX_APP_HOST);

    private static QueryResourceHttpClient autoIndexQueryClient =
            new QueryResourceHttpClient(TestConstants.AUTO_INDEX_APP_HOST);

    @Rule
    public final DeleteFromTableRule<Config> clearDbRule = DeleteFromTableRule.withApp(autoIndexAppRule)
            .table(AutoIndexDocRefEntity.TABLE_NAME)
            .build();

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
            .build();

    /**
     * This elastic index is used for the storage of Auto Index doc refs
     */
    @ClassRule
    public static final ElasticTestIndexRule stroomIndexRule = ElasticTestIndexRule
            .forIndex(AutoIndexQueryResourceIT.class, ElasticIndexDocRefServiceImpl.STROOM_INDEX_NAME)
            .httpUrl(TestConstants.LOCAL_ELASTIC_HTTP_HOST)
            .build();

    @Rule
    public FifoLogbackRule auditLogRule = new FifoLogbackRule();

    private class EntityWithDocRef<DOC_REF_ENTITY extends DocRefEntity> {
        private final DOC_REF_ENTITY entity;
        private final DocRef docRef;

        private EntityWithDocRef(final DOC_REF_ENTITY entity,
                                 final DocRef docRef) {
            this.entity = entity;
            this.docRef = docRef;
        }
    }

    @Test
    public void testGetDataSource() {
        final EntityWithDocRef<AutoIndexDocRefEntity> autoIndex = createAutoIndex();

        final Response response = autoIndexQueryClient.getDataSource(authRule.adminUser(), autoIndex.docRef);
        assertEquals(HttpStatus.OK_200, response.getStatus());
        final DataSource result = response.readEntity(DataSource.class);

        final Set<String> resultFieldNames = result.getFields().stream()
                .map(DataSourceField::getName)
                .collect(Collectors.toSet());

        assertTrue(resultFieldNames.contains(AnimalSighting.SPECIES));
        assertTrue(resultFieldNames.contains(AnimalSighting.LOCATION));
        assertTrue(resultFieldNames.contains(AnimalSighting.OBSERVER));
        assertTrue(resultFieldNames.contains(AnimalSighting.TIME));

        // Create the 3 doc refs, get data source, which will cause the raw data source to be queried
        auditLogRule.check()
                .thereAreAtLeast(2)
                .containsOrdered(containsAllOf(AuditedQueryResourceImpl.GET_DATA_SOURCE, autoIndex.entity.getRawDocRef().getUuid()))
                .containsOrdered(containsAllOf(AuditedQueryResourceImpl.GET_DATA_SOURCE, autoIndex.docRef.getUuid()));
    }

    @Test
    public void testSearch() {
        final EntityWithDocRef<AutoIndexDocRefEntity> autoIndex = createAutoIndex();

        final OffsetRange offset = new OffsetRange.Builder()
                .length(100L)
                .offset(0L)
                .build();
        final String testObserver = "alpha";
        final LocalDateTime testMaxDate = LocalDateTime.of(2017, 1, 1, 0, 0, 0);
        final ExpressionOperator expressionOperator = new ExpressionOperator.Builder(ExpressionOperator.Op.AND)
                .addTerm(AnimalSighting.OBSERVER, ExpressionTerm.Condition.CONTAINS, testObserver)
                .addTerm(AnimalSighting.TIME,
                        ExpressionTerm.Condition.LESS_THAN,
                        testMaxDate.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                )
                .build();

        final SearchRequest searchRequest = AnimalsQueryResourceIT
                .getTestSearchRequest(autoIndex.docRef, expressionOperator, offset);

        final Response response = autoIndexQueryClient.search(authRule.adminUser(), searchRequest);
        assertEquals(HttpStatus.OK_200, response.getStatus());

        final SearchResponse searchResponse = response.readEntity(SearchResponse.class);

        final Set<AnimalSighting> resultsSet = new HashSet<>();

        assertTrue("No results seen", searchResponse.getResults().size() > 0);
        for (final Result result : searchResponse.getResults()) {
            assertTrue(result instanceof FlatResult);

            final FlatResult flatResult = (FlatResult) result;
            flatResult.getValues().stream()
                    .map(o -> new AnimalSighting.Builder()
                            .species(o.get(3).toString())
                            .location(o.get(4).toString())
                            .observer(o.get(5).toString())
                            .time(LocalDateTime.parse(o.get(6).toString(), DateTimeFormatter.ISO_LOCAL_DATE_TIME))
                            .build())
                    .forEach(resultsSet::add);
        }

        // Check that the returned data matches the conditions
        resultsSet.stream().map(AnimalSighting::getObserver)
                .forEach(o -> assertEquals(testObserver, o));
        resultsSet.stream().map(AnimalSighting::getTime)
                .forEach(t -> assertTrue(testMaxDate.isAfter(t)));

        LOGGER.info("Results from Search {}", resultsSet.size());
        resultsSet.stream()
                .map(Object::toString)
                .forEach(LOGGER::info);

        // Create the 3 doc refs, search, which will cause the raw data source to be queried
        auditLogRule.check()
                .thereAreAtLeast(2)
                .containsOrdered(containsAllOf(AuditedQueryResourceImpl.QUERY_SEARCH, autoIndex.entity.getRawDocRef().getUuid()))
                .containsOrdered(containsAllOf(AuditedQueryResourceImpl.QUERY_SEARCH, autoIndex.docRef.getUuid()));
    }

    /**
     * Creates the various doc refs required to support a single Auto Index.
     * It gives the admin user full access to all doc refs.
     * @return The AutoIndexDocRefEntity created, tied to the raw and indexed doc refs below it.
     */
    private EntityWithDocRef<AutoIndexDocRefEntity> createAutoIndex() {
        final DocRef animalDocRef = createDocument(new AnimalDocRefEntity.Builder()
                        .uuid(UUID.randomUUID().toString())
                        .name(UUID.randomUUID().toString())
                        .dataDirectory(testDataRule.getFolder().getAbsolutePath())
                        .build());

        final DocRef elasticDocRef = createDocument(new ElasticIndexDocRefEntity.Builder()
                        .uuid(UUID.randomUUID().toString())
                        .name(UUID.randomUUID().toString())
                        .indexedType(UUID.randomUUID().toString())
                        .indexName(UUID.randomUUID().toString())
                        .build());

        final AutoIndexDocRefEntity autoIndexDocRefEntity = new AutoIndexDocRefEntity.Builder()
                .uuid(UUID.randomUUID().toString())
                .name(UUID.randomUUID().toString())
                .rawDocRef(animalDocRef)
                .indexDocRef(elasticDocRef)
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
    private <DOC_REF_ENTITY extends DocRefEntity>
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
        authRule.givePermission(authRule.adminUser(), new DocRef.Builder()
                        .type(DocumentPermission.FOLDER)
                        .uuid(parentFolderUuid)
                        .build(),
                DocumentPermission.CREATE.getTypedPermission(docRefType));

        // Create a doc ref to hang the search from
        final Response createResponse = docRefClient.createDocument(
                authRule.adminUser(),
                docRef.getUuid(),
                docRef.getName(),
                parentFolderUuid);
        assertEquals(HttpStatus.OK_200, createResponse.getStatus());
        createResponse.close();

        // Give admin all the roles required to manipulate the document and it's underlying data
        authRule.givePermission(authRule.adminUser(), new DocRef.Builder()
                        .type(docRefType)
                        .uuid(docRef.getUuid())
                        .build(),
                DocumentPermission.READ.getName());
        authRule.givePermission(authRule.adminUser(), new DocRef.Builder()
                        .type(docRefType)
                        .uuid(docRef.getUuid())
                        .build(),
                DocumentPermission.UPDATE.getName());

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
