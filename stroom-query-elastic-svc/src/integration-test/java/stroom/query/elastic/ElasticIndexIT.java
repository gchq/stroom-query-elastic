package stroom.query.elastic;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import io.dropwizard.testing.junit.DropwizardAppRule;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpStatus;
import org.junit.Before;
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
import stroom.query.api.v2.FlatResult;
import stroom.query.api.v2.ResultRequest;
import stroom.query.api.v2.SearchRequest;
import stroom.query.api.v2.SearchResponse;
import stroom.query.audit.FifoLogbackAppender;
import stroom.query.elastic.hibernate.ElasticIndexConfig;
import stroom.query.elastic.resources.HelloResource;
import stroom.query.elastic.service.ElasticDocRefService;

import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static io.dropwizard.testing.ResourceHelpers.resourceFilePath;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ElasticIndexIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticIndexIT.class);

    // This Config is for the valid elastic index, the index and doc ref will be created
    // as part of the class setup
    private static final String DATA_INDEX_STROOM_NAME = "StroomShakespeare";
    private static final String DATA_INDEX_NAME = "shakespeare";
    private static final String DATA_INDEXED_TYPE = "line";


    private static BiFunction<ElasticIndexConfig, ExpressionOperator, SearchRequest> dataSearchRequest =
            (elasticIndexConfig, expressionOperator) -> {
                final String queryKey = UUID.randomUUID().toString();
                return new SearchRequest.Builder()
                        .query()
                            .dataSource(getDocRef(elasticIndexConfig))
                            .expression(expressionOperator)
                            .end()
                        .key(queryKey)
                        .dateTimeLocale("en-gb")
                        .incremental(true)
                        .addResultRequest()
                            .fetch(ResultRequest.Fetch.ALL)
                            .resultStyle(ResultRequest.ResultStyle.FLAT)
                            .componentId("componentId")
                            .requestedRange(null)
                            .addMapping()
                                .queryId(queryKey)
                                .extractValues(false)
                                .showDetail(false)
                                .addField(ShakespeareLine.PLAY_NAME, "${" + ShakespeareLine.PLAY_NAME + "}").end()
                                .addField(ShakespeareLine.LINE_ID, "${" + ShakespeareLine.LINE_ID + "}").end()
                                .addField(ShakespeareLine.SPEECH_NUMBER, "${" + ShakespeareLine.SPEECH_NUMBER + "}").end()
                                .addField(ShakespeareLine.SPEAKER, "${" + ShakespeareLine.SPEAKER + "}").end()
                                .addField(ShakespeareLine.TEXT_ENTRY, "${" + ShakespeareLine.TEXT_ENTRY + "}").end()
                                .addMaxResults(10)
                                .end()
                            .end()
                        .build();
            };

    private static final DocRef getDocRef(final ElasticIndexConfig elasticIndexConfig) {
        return new DocRef.Builder()
                .uuid(elasticIndexConfig.getUuid())
                .build();
    }

    private static final String LOCALHOST = "localhost";
    private static final int ELASTIC_HTTP_PORT = 9200;
    private static final String ELASTIC_DATA_FILE = "elastic/shakespeare.json";
    private static final String ELASTIC_DATA_MAPPINGS_FULL_FILE = "elastic/shakespeare.mappings.json";

    @ClassRule
    public static final DropwizardAppRule<Config> appRule = new DropwizardAppRule<>(App.class, resourceFilePath("config.yml"));

    private static final com.fasterxml.jackson.databind.ObjectMapper jacksonObjectMapper =
            new com.fasterxml.jackson.databind.ObjectMapper();

    private static String helloUrl;
    private static QueryResourceClient queryClient;
    private static ElasticIndexClient elasticIndexClient;
    private static DocRefResourceClient docRefClient;

    @BeforeClass
    public static void setupClass() {

        int appPort = appRule.getLocalPort();
        helloUrl = String.format("http://%s:%d/hello/v1", LOCALHOST, appPort);
        queryClient = new QueryResourceClient(String.format("http://%s:%d/queryApi/v1", LOCALHOST, appPort));
        elasticIndexClient = new ElasticIndexClient(String.format("http://%s:%d/elasticIndex/v1", LOCALHOST, appPort));
        docRefClient = new DocRefResourceClient(String.format("http://%s:%d/docRefApi/v1", LOCALHOST, appPort));

        Unirest.setObjectMapper(new com.mashape.unirest.http.ObjectMapper() {
            private com.fasterxml.jackson.databind.ObjectMapper jacksonObjectMapper
                    = new com.fasterxml.jackson.databind.ObjectMapper();

            public <T> T readValue(String value, Class<T> valueType) {
                try {
                    return jacksonObjectMapper.readValue(value, valueType);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            public String writeValue(Object value) {
                try {
                    return jacksonObjectMapper.writeValueAsString(value);
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        try {
            final String ND_JSON = "application/x-ndjson";
            final ClassLoader classLoader = ElasticIndexIT.class.getClassLoader();

            // Talk directly to Elastic Search to create a fresh populated index for us to connect to

            // Delete any existing 'stroom doc ref' index
            final String elasticDocRefIndexUrl = String.format("http://%s:%d/%s", LOCALHOST, ELASTIC_HTTP_PORT, ElasticDocRefService.STROOM_INDEX_NAME);
            Unirest
                    .delete(elasticDocRefIndexUrl)
                    .header("Content-Type", ND_JSON)
                    .asString(); // response may be 404 if first time run

            // Delete any existing 'data' index
            final String elasticDataIndexUrl = String.format("http://%s:%d/%s", LOCALHOST, ELASTIC_HTTP_PORT, DATA_INDEX_NAME);
            Unirest
                    .delete(elasticDataIndexUrl)
                    .header("Content-Type", ND_JSON)
                    .asString(); // response may be 404 if first time run

            // Create index with mappings
            final String mappingsJson = IOUtils.toString(classLoader.getResourceAsStream(ELASTIC_DATA_MAPPINGS_FULL_FILE));
            final HttpResponse<String> createMappingsResponse = Unirest
                    .put(elasticDataIndexUrl)
                    .header("accept", MediaType.APPLICATION_JSON)
                    .header("Content-Type", ND_JSON)
                    .body(mappingsJson)
                    .asString();
            assertEquals(String.format("Failed with %s", createMappingsResponse.getBody()),
                    HttpStatus.SC_OK,
                    createMappingsResponse.getStatus());

            // Post Data
            final String dataJson = IOUtils.toString(classLoader.getResourceAsStream(ELASTIC_DATA_FILE));
            final String putDataUrl = String.format("http://%s:%d/%s/_bulk?pretty", LOCALHOST, ELASTIC_HTTP_PORT, DATA_INDEXED_TYPE);
            final HttpResponse<String> putDataResponse = Unirest
                    .put(putDataUrl)
                    .header("accept", MediaType.APPLICATION_JSON)
                    .header("Content-Type", ND_JSON)
                    .body(dataJson)
                    .asString();
            assertEquals(String.format("Failed with %s", createMappingsResponse.getBody()),
                    HttpStatus.SC_OK,
                    putDataResponse.getStatus());

        } catch (Exception e) {
            LOGGER.error("Could not create index config", e);
            fail(e.getLocalizedMessage());
        }
    }

    @Before
    public void beforeTest() {
        FifoLogbackAppender.popLogs();
    }

    private void checkAuditLogs(final int expected) {
        final List<Object> records = FifoLogbackAppender.popLogs();

        LOGGER.info(String.format("Expected %d records, received %d", expected, records.size()));

        assertEquals(expected, records.size());
    }

    @Test
    public void testHello() {
        try {
            final HttpResponse<String> response = Unirest.get(helloUrl).asString();

            assertEquals(HttpStatus.SC_OK, response.getStatus());
            assertEquals(HelloResource.WELCOME_TEXT, response.getBody());
        } catch (UnirestException e) {
            fail(e.getLocalizedMessage());
        }
    }

    @Test
    public void testGetDataSourceValid() throws UnirestException, IOException {
        final ElasticIndexConfig elasticIndexConfig = createDataIndexDocRef();

        final HttpResponse<String> response = queryClient.getDataSource(getDocRef(elasticIndexConfig));

        assertEquals(HttpStatus.SC_OK, response.getStatus());

        final String body = response.getBody();

        LOGGER.info("Data Source Body: " + body);
        final DataSource result = jacksonObjectMapper.readValue(body, DataSource.class);

        final Set<String> resultFieldNames = result.getFields().stream()
                .map(DataSourceField::getName)
                .collect(Collectors.toSet());

        assertTrue(resultFieldNames.contains(ShakespeareLine.LINE_ID));
        assertTrue(resultFieldNames.contains(ShakespeareLine.PLAY_NAME));
        assertTrue(resultFieldNames.contains(ShakespeareLine.SPEAKER));
        assertTrue(resultFieldNames.contains(ShakespeareLine.SPEECH_NUMBER));
        assertTrue(resultFieldNames.contains(ShakespeareLine.TEXT_ENTRY));

        // Create, update, getDataSource
        checkAuditLogs(3);
    }

    /**
     * This test is used to check the behaviour of the system if you request data/info about
     * a completely non existent doc ref.
     */
    @Test
    public void testGetDataSourceMissingDocRef() throws UnirestException {

        // Create a random index config that is not registered with the system
        final DocRef elasticIndexConfig = new DocRef.Builder()
                .uuid(UUID.randomUUID().toString())
                .name("DoesNotExist")
                .build();

        final HttpResponse<String> response = queryClient.getDataSource(elasticIndexConfig);

        assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatus());

        // Get
        checkAuditLogs(1);
    }

    @Test
    public void testSearchValid() throws UnirestException, IOException {
        final ElasticIndexConfig elasticIndexConfig = createDataIndexDocRef();

        final ExpressionOperator speakerFinder = new ExpressionOperator.Builder(ExpressionOperator.Op.AND)
                .addTerm(ShakespeareLine.SPEAKER, ExpressionTerm.Condition.EQUALS, "WARWICK")
                .addTerm(ShakespeareLine.TEXT_ENTRY, ExpressionTerm.Condition.CONTAINS, "pluck")
                .build();

        final SearchRequest searchRequest = dataSearchRequest.apply(elasticIndexConfig, speakerFinder);

        final HttpResponse<String> response = queryClient.search(searchRequest);

        assertEquals(HttpStatus.SC_OK, response.getStatus());

        LOGGER.info("BODY - " + response.getBody());

        final SearchResponse searchResponse = jacksonObjectMapper.readValue(response.getBody(), SearchResponse.class);

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
        checkAuditLogs(3);
    }

    /**
     * This test is used to check the behaviour of the system if you request data/info about
     * a completely non existent doc ref
     */
    @Test
    public void testSearchMissingDocRef() throws UnirestException {

        // Create a random index config that is not registered with the system
        final ElasticIndexConfig elasticIndexConfig = new ElasticIndexConfig.Builder()
                .uuid(UUID.randomUUID())
                .indexName(UUID.randomUUID())
                .indexedType(UUID.randomUUID())
                .build();

        final ExpressionOperator speakerFinder = new ExpressionOperator.Builder(ExpressionOperator.Op.AND)
                .addTerm("aKey", ExpressionTerm.Condition.EQUALS, "aValue")
                .build();

        final SearchRequest searchRequest = dataSearchRequest.apply(elasticIndexConfig, speakerFinder);
        final HttpResponse<String> response = queryClient.search(searchRequest);

        assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatus());


        checkAuditLogs(1);
    }

    /**
     * This test is used to check the behaviour of the system if you request data/info about
     * a docRef that exists, but the index does not exist.
     */
    @Test
    public void testSearchMissingIndex() throws UnirestException {

        final ElasticIndexConfig elasticIndexConfig = new ElasticIndexConfig.Builder()
                .uuid(UUID.randomUUID())
                .indexName(UUID.randomUUID())
                .indexedType(UUID.randomUUID())
                .build();

        docRefClient.createDocument(elasticIndexConfig.getUuid(), elasticIndexConfig.getStroomName());
        elasticIndexClient.update(elasticIndexConfig.getUuid(), elasticIndexConfig);

        final ExpressionOperator speakerFinder = new ExpressionOperator.Builder(ExpressionOperator.Op.AND)
                .addTerm("aKey", ExpressionTerm.Condition.EQUALS, "aValue")
                .build();

        final SearchRequest searchRequest = dataSearchRequest.apply(elasticIndexConfig, speakerFinder);
        final HttpResponse<String> response = queryClient.search(searchRequest);

        assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatus());

        checkAuditLogs(3);
    }

    /**
     * Shortcut function to create another DocRef that references the populated 'data' index.
     * @return The ElasticIndexConfig representing the new DocRef
     */
    private ElasticIndexConfig createDataIndexDocRef() throws UnirestException {
        final ElasticIndexConfig elasticIndexConfig = new ElasticIndexConfig.Builder()
                .uuid(UUID.randomUUID().toString())
                .stroomName(DATA_INDEX_STROOM_NAME)
                .indexName(DATA_INDEX_NAME)
                .indexedType(DATA_INDEXED_TYPE)
                .build();

        final HttpResponse<String> createDocRefResponse = docRefClient.createDocument(elasticIndexConfig.getUuid(), elasticIndexConfig.getStroomName());
        assertEquals(HttpStatus.SC_OK, createDocRefResponse.getStatus());

        final HttpResponse<String> updateIndexResponse =
                elasticIndexClient.update(elasticIndexConfig.getUuid(), elasticIndexConfig);
        assertEquals(HttpStatus.SC_OK, updateIndexResponse.getStatus());

        return elasticIndexConfig;
    }
}
