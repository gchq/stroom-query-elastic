package stroom.query.elastic.auth;

import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpStatus;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientResponse;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.datasource.api.v2.DataSource;
import stroom.datasource.api.v2.DataSourceField;
import stroom.query.api.v2.DocRef;
import stroom.query.api.v2.ExpressionOperator;
import stroom.query.api.v2.ExpressionTerm;
import stroom.query.api.v2.Field;
import stroom.query.api.v2.FlatResult;
import stroom.query.api.v2.OffsetRange;
import stroom.query.api.v2.Query;
import stroom.query.api.v2.ResultRequest;
import stroom.query.api.v2.SearchRequest;
import stroom.query.api.v2.SearchResponse;
import stroom.query.api.v2.TableSettings;
import stroom.query.audit.authorisation.DocumentPermission;
import stroom.query.audit.rest.AuditedDocRefResourceImpl;
import stroom.query.audit.rest.AuditedQueryResourceImpl;
import stroom.query.elastic.App;
import stroom.query.elastic.ShakespeareLine;
import stroom.query.elastic.config.Config;
import stroom.query.elastic.hibernate.ElasticIndexDocRefEntity;
import stroom.query.elastic.service.ElasticDocRefServiceImpl;
import stroom.query.testing.DropwizardAppWithClientsRule;
import stroom.query.testing.QueryResourceIT;
import stroom.query.testing.StroomAuthenticationRule;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static io.dropwizard.testing.ResourceHelpers.resourceFilePath;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static stroom.query.testing.FifoLogbackRule.containsAllOf;

public class ElasticQueryResourceIT extends QueryResourceIT<ElasticIndexDocRefEntity, Config> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticQueryResourceIT.class);

    @ClassRule
    public static final DropwizardAppWithClientsRule<Config> appRule =
            new DropwizardAppWithClientsRule<>(App.class, resourceFilePath("config_auth.yml"));

    @ClassRule
    public static StroomAuthenticationRule authRule =
            new StroomAuthenticationRule(WireMockConfiguration.options().port(10080), ElasticIndexDocRefEntity.TYPE);

    public ElasticQueryResourceIT() {
        super(ElasticIndexDocRefEntity.class,
                ElasticIndexDocRefEntity.TYPE,
                appRule,
                authRule);
    }

    // This Config is for the valid elastic index, the index and doc ref will be created
    // as part of the class setup
    private static final String DATA_INDEX_NAME = "shakespeare";
    private static final String DATA_INDEXED_TYPE = "line";

    private static final int ELASTIC_HTTP_PORT = 19200;
    private static final String ELASTIC_DATA_FILE = "elastic/shakespeare.json";
    private static final String ELASTIC_DATA_MAPPINGS_FULL_FILE = "elastic/shakespeare.mappings.json";
    
    @BeforeClass
    public static void beforeQueryClass() {
        try {
            final Client httpClient = ClientBuilder.newClient(new ClientConfig().register(ClientResponse.class));

            final String ND_JSON = "application/x-ndjson";
            final ClassLoader classLoader = ElasticQueryResourceIT.class.getClassLoader();

            // Talk directly to Elastic Search to create a fresh populated index for us to connect to

            // Delete any existing 'stroom doc ref' index
            final String elasticDocRefIndexUrl = String.format("http://localhost:%d/%s", ELASTIC_HTTP_PORT, ElasticDocRefServiceImpl.STROOM_INDEX_NAME);
            httpClient.target(elasticDocRefIndexUrl)
                    .request()
                    .header("Content-Type", ND_JSON)
                    .delete(); // response may be 404 if first time run

            // Delete any existing 'data' index
            final String elasticDataIndexUrl = String.format("http://localhost:%d/%s", ELASTIC_HTTP_PORT, DATA_INDEX_NAME);
            httpClient.target(elasticDataIndexUrl)
                    .request()
                    .header("Content-Type", ND_JSON)
                    .delete(); // response may be 404 if first time run

            // Create index with mappings
            final String mappingsJson = IOUtils.toString(classLoader.getResourceAsStream(ELASTIC_DATA_MAPPINGS_FULL_FILE));
            final Response createMappingsResponse = httpClient
                    .target(elasticDataIndexUrl)
                    .request()
                    .header("accept", MediaType.APPLICATION_JSON)
                    .header("Content-Type", ND_JSON)
                    .put(Entity.json(mappingsJson));
            assertEquals(String.format("Failed with %s", createMappingsResponse.readEntity(String.class)),
                    HttpStatus.SC_OK,
                    createMappingsResponse.getStatus());

            // Post Data
            final String dataJson = IOUtils.toString(classLoader.getResourceAsStream(ELASTIC_DATA_FILE));
            final String putDataUrl = String.format("http://localhost:%d/%s/_bulk?pretty", ELASTIC_HTTP_PORT, DATA_INDEXED_TYPE);
            final Response putDataResponse = httpClient
                    .target(putDataUrl)
                    .request()
                    .header("accept", MediaType.APPLICATION_JSON)
                    .header("Content-Type", ND_JSON)
                    .put(Entity.json(dataJson));
            assertEquals(String.format("Failed with %s", putDataResponse.readEntity(String.class)),
                    HttpStatus.SC_OK,
                    putDataResponse.getStatus());

        } catch (Exception e) {
            LOGGER.error("Could not create index config", e);
            fail(e.getLocalizedMessage());
        }
    }

    @Override
    protected ElasticIndexDocRefEntity getValidEntity(final DocRef docRef) {
        return new ElasticIndexDocRefEntity.Builder()
                .uuid(docRef.getUuid())
                .name(UUID.randomUUID().toString())
                .indexName(DATA_INDEX_NAME)
                .indexedType(DATA_INDEXED_TYPE)
                .build();
    }

    @Override
    protected SearchRequest getValidSearchRequest(final DocRef docRef,
                                                  final ExpressionOperator expressionOperator,
                                                  final OffsetRange offsetRange) {
        final String queryKey = UUID.randomUUID().toString();
        return new SearchRequest.Builder()
                .query(new Query.Builder()
                        .dataSource(docRef)
                        .expression(expressionOperator)
                        .build())
                .key(queryKey)
                .dateTimeLocale("en-gb")
                .incremental(true)
                .addResultRequests(new ResultRequest.Builder()
                        .fetch(ResultRequest.Fetch.ALL)
                        .resultStyle(ResultRequest.ResultStyle.FLAT)
                        .componentId("componentId")
                        .requestedRange(offsetRange)
                        .addMappings(new TableSettings.Builder()
                                .queryId(queryKey)
                                .extractValues(false)
                                .showDetail(false)
                                .addFields(new Field.Builder()
                                        .name(ShakespeareLine.PLAY_NAME)
                                        .expression("${" + ShakespeareLine.PLAY_NAME + "}")
                                        .build())
                                .addFields(new Field.Builder()
                                        .name(ShakespeareLine.LINE_ID)
                                        .expression("${" + ShakespeareLine.LINE_ID + "}")
                                        .build())
                                .addFields(new Field.Builder()
                                        .name(ShakespeareLine.SPEECH_NUMBER)
                                        .expression("${" + ShakespeareLine.SPEECH_NUMBER + "}")
                                        .build())
                                .addFields(new Field.Builder()
                                        .name(ShakespeareLine.SPEAKER)
                                        .expression("${" + ShakespeareLine.SPEAKER + "}")
                                        .build())
                                .addFields(new Field.Builder()
                                        .name(ShakespeareLine.TEXT_ENTRY)
                                        .expression("${" + ShakespeareLine.TEXT_ENTRY + "}")
                                        .build())
                                .addMaxResults(10)
                                .build())
                        .build())
                .build();
    }

    @Override
    protected void assertValidDataSource(final DataSource dataSource) {
        final Set<String> resultFieldNames = dataSource.getFields().stream()
                .map(DataSourceField::getName)
                .collect(Collectors.toSet());

        assertTrue(resultFieldNames.contains(ShakespeareLine.LINE_ID));
        assertTrue(resultFieldNames.contains(ShakespeareLine.PLAY_NAME));
        assertTrue(resultFieldNames.contains(ShakespeareLine.SPEAKER));
        assertTrue(resultFieldNames.contains(ShakespeareLine.SPEECH_NUMBER));
        assertTrue(resultFieldNames.contains(ShakespeareLine.TEXT_ENTRY));
    }

    /**
     * This test is used to check the behaviour of the system if you request data/info about
     * a completely non existent doc ref.
     */
    @Test
    public void testGetDataSourceMissingDocRef() {

        // Create a random index config that is not registered with the system
        final DocRef elasticIndexConfig = new DocRef.Builder()
                .uuid(UUID.randomUUID().toString())
                .type(ElasticIndexDocRefEntity.TYPE)
                .name("DoesNotExist")
                .build();
        authRule.giveDocumentPermission(authRule.adminUser(), elasticIndexConfig.getUuid(), DocumentPermission.READ);

        final Response response = queryClient.getDataSource(authRule.adminUser(), elasticIndexConfig);

        assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatus());

        // Get
        auditLogRule.check()
                .thereAreAtLeast(1)
                .containsOrdered(containsAllOf(AuditedQueryResourceImpl.GET_DATA_SOURCE, elasticIndexConfig.getUuid()));
    }

    @Test
    public void testSearchValid() {
        final DocRef docRef = createDocument();

        final String SPEAKER_TERM = "WARWICK";
        final String TEXT_TERM = "pluck";
        final ExpressionOperator speakerFinder = new ExpressionOperator.Builder(ExpressionOperator.Op.AND)
                .addTerm(ShakespeareLine.SPEAKER, ExpressionTerm.Condition.EQUALS, SPEAKER_TERM)
                .addTerm(ShakespeareLine.TEXT_ENTRY, ExpressionTerm.Condition.CONTAINS, TEXT_TERM)
                .build();

        final SearchRequest searchRequest = getValidSearchRequest(docRef, speakerFinder, null);

        final Response response = queryClient.search(authRule.adminUser(), searchRequest);

        assertEquals(HttpStatus.SC_OK, response.getStatus());

        final SearchResponse searchResponse = response.readEntity(SearchResponse.class);

        final List<ShakespeareLine> lines = searchResponse.getResults().stream()
                .map(r -> (FlatResult) r)
                .map(FlatResult::getValues)
                .flatMap(Collection::stream)
                .map(r -> new ShakespeareLine.Builder()
                        .playName(r.get(3).toString())
                        .lineId(r.get(4).toString())
                        .speechNumber(Integer.parseInt(r.get(5).toString()))
                        .speaker(r.get(6).toString())
                        .textEntry(r.get(7).toString())
                        .build())
                .collect(Collectors.toList());

        assertEquals(1, lines.size());
        assertEquals("4178", lines.get(0).getLineId());

        // Create DocRef, Update Index, Get

        auditLogRule.check()
                .thereAreAtLeast(3)
                .containsOrdered(containsAllOf(AuditedDocRefResourceImpl.CREATE_DOC_REF, docRef.getUuid()))
                .containsOrdered(containsAllOf(AuditedDocRefResourceImpl.UPDATE_DOC_REF, docRef.getUuid()))
                .containsOrdered(containsAllOf(AuditedQueryResourceImpl.QUERY_SEARCH,
                        docRef.getUuid(),
                        ShakespeareLine.SPEAKER,
                        SPEAKER_TERM,
                        ShakespeareLine.TEXT_ENTRY,
                        TEXT_TERM));
    }

    /**
     * This test is used to check the behaviour of the system if you request data/info about
     * a completely non existent doc ref
     */
    @Test
    public void testSearchMissingDocRef() {

        // Create a random index config that is not registered with the system
        final DocRef docRef = new DocRef.Builder()
                .uuid(UUID.randomUUID().toString())
                .type(ElasticIndexDocRefEntity.TYPE)
                .name(UUID.randomUUID().toString())
                .build();

        // Give permission to this non existent document
        authRule.giveDocumentPermission(authRule.adminUser(), docRef.getUuid(), DocumentPermission.READ);

        final String TERM_KEY = "aKey";
        final String TERM_VALUE = "aValue";
        final ExpressionOperator speakerFinder = new ExpressionOperator.Builder(ExpressionOperator.Op.AND)
                .addTerm(TERM_KEY, ExpressionTerm.Condition.EQUALS, TERM_VALUE)
                .build();

        final SearchRequest searchRequest = getValidSearchRequest(docRef, speakerFinder, null);
        final Response response = queryClient.search(authRule.adminUser(), searchRequest);

        assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatus());

        auditLogRule.check()
                .thereAreAtLeast(1)
                .containsOrdered(containsAllOf(AuditedQueryResourceImpl.QUERY_SEARCH,
                        docRef.getUuid(),
                        TERM_KEY,
                        TERM_VALUE));
    }

    /**
     * This test is used to check the behaviour of the system if you request data/info about
     * a docRef that exists, but the index does not exist.
     */
    @Test
    public void testSearchMissingIndex() {

        final DocRef docRef = createDocument(new ElasticIndexDocRefEntity.Builder()
                .uuid(UUID.randomUUID().toString())
                .indexName(UUID.randomUUID())
                .indexedType(UUID.randomUUID())
                .build());

        final String TERM_KEY = "aKey";
        final String TERM_VALUE = "aValue";
        final ExpressionOperator speakerFinder = new ExpressionOperator.Builder(ExpressionOperator.Op.AND)
                .addTerm(TERM_KEY, ExpressionTerm.Condition.EQUALS, TERM_VALUE)
                .build();

        final SearchRequest searchRequest = getValidSearchRequest(docRef, speakerFinder, null);
        final Response response = queryClient.search(authRule.adminUser(), searchRequest);

        assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatus());

        auditLogRule.check()
                .thereAreAtLeast(3)
                .containsOrdered(containsAllOf(AuditedDocRefResourceImpl.CREATE_DOC_REF, docRef.getUuid()))
                .containsOrdered(containsAllOf(AuditedDocRefResourceImpl.UPDATE_DOC_REF, docRef.getUuid()))
                .containsOrdered(containsAllOf(AuditedQueryResourceImpl.QUERY_SEARCH,
                        docRef.getUuid(),
                        TERM_KEY,
                        TERM_VALUE));
    }
}
