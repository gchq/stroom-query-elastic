package stroom.query.elastic;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockClassRule;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import io.dropwizard.testing.junit.DropwizardAppRule;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpStatus;
import org.jose4j.jwk.JsonWebKey;
import org.jose4j.jwk.RsaJsonWebKey;
import org.jose4j.jwk.RsaJwkGenerator;
import org.jose4j.jws.AlgorithmIdentifiers;
import org.jose4j.jws.JsonWebSignature;
import org.jose4j.jwt.JwtClaims;
import org.jose4j.lang.JoseException;
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
import stroom.query.api.v2.Field;
import stroom.query.api.v2.FlatResult;
import stroom.query.api.v2.Query;
import stroom.query.api.v2.ResultRequest;
import stroom.query.api.v2.SearchRequest;
import stroom.query.api.v2.SearchResponse;
import stroom.query.api.v2.TableSettings;
import stroom.query.audit.client.DocRefResourceHttpClient;
import stroom.query.audit.logback.FifoLogbackAppender;
import stroom.query.audit.client.QueryResourceHttpClient;
import stroom.query.audit.security.ServiceUser;
import stroom.query.elastic.config.Config;
import stroom.query.elastic.hibernate.ElasticIndexConfig;
import stroom.query.elastic.service.ElasticDocRefServiceImpl;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
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
                        .query(new Query.Builder()
                            .dataSource(getDocRef(elasticIndexConfig))
                            .expression(expressionOperator)
                            .build())
                        .key(queryKey)
                        .dateTimeLocale("en-gb")
                        .incremental(true)
                        .addResultRequests(new ResultRequest.Builder()
                            .fetch(ResultRequest.Fetch.ALL)
                            .resultStyle(ResultRequest.ResultStyle.FLAT)
                            .componentId("componentId")
                            .requestedRange(null)
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
            };

    private static final DocRef getDocRef(final ElasticIndexConfig elasticIndexConfig) {
        return new DocRef.Builder()
                .uuid(elasticIndexConfig.getUuid())
                .build();
    }

    private static final String VALID_USER_NAME = "testSubject";
    private static final String LOCALHOST = "localhost";
    private static final int ELASTIC_HTTP_PORT = 9200;
    private static final String ELASTIC_DATA_FILE = "elastic/shakespeare.json";
    private static final String ELASTIC_DATA_MAPPINGS_FULL_FILE = "elastic/shakespeare.mappings.json";
    private static final String PARENT_FOLDER_UUID = UUID.randomUUID().toString();

    @ClassRule
    public static final DropwizardAppRule<Config> appRule =
            new DropwizardAppRule<>(App.class, resourceFilePath("config.yml"));

    @ClassRule
    public static WireMockClassRule wireMockRule = new WireMockClassRule(
            WireMockConfiguration.options().port(10080));

    private static ServiceUser serviceUser;

    private static final com.fasterxml.jackson.databind.ObjectMapper jacksonObjectMapper =
            new com.fasterxml.jackson.databind.ObjectMapper();

    private static QueryResourceHttpClient queryClient;
    private static DocRefResourceHttpClient docRefClient;
    
    @BeforeClass
    public static void setupClass() {
        final int appPort = appRule.getLocalPort();
        final String host = String.format("http://%s:%d", LOCALHOST, appPort);
        queryClient = new QueryResourceHttpClient(host);
        docRefClient = new DocRefResourceHttpClient(host);

        stubFor(post(urlEqualTo("/api/authorisation/v1/isAuthorised"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", MediaType.TEXT_PLAIN)
                        .withBody("Mock approval for authorisation")
                        .withStatus(200)));

        RsaJsonWebKey jwk;
        try {
            String jwkId = UUID.randomUUID().toString();
            jwk = RsaJwkGenerator.generateJwk(2048);
            jwk.setKeyId(jwkId);
        } catch (Exception e) {
            fail(e.getLocalizedMessage());
            return;
        }

        stubFor(get(urlEqualTo("/testAuthService/publicKey"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", MediaType.APPLICATION_JSON)
                        .withBody(jwk.toJson(JsonWebKey.OutputControlLevel.PUBLIC_ONLY))
                        .withStatus(200)));

        JwtClaims claims = new JwtClaims();
        claims.setIssuer("stroom");  // who creates the token and signs it
        claims.setSubject(VALID_USER_NAME); // the subject/principal is whom the token is about

        final JsonWebSignature jws = new JsonWebSignature();
        jws.setPayload(claims.toJson());
        jws.setAlgorithmHeaderValue(AlgorithmIdentifiers.RSA_USING_SHA256);
        jws.setKey(jwk.getPrivateKey());
        jws.setDoKeyValidation(false);

        try {
            serviceUser = new ServiceUser.Builder()
                    .jwt(jws.getCompactSerialization())
                    .name(VALID_USER_NAME)
                    .build();
        } catch (JoseException e) {
            fail(e.getLocalizedMessage());
        }

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
            final String elasticDocRefIndexUrl = String.format("http://%s:%d/%s", LOCALHOST, ELASTIC_HTTP_PORT, ElasticDocRefServiceImpl.STROOM_INDEX_NAME);
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
    public void testGetDataSourceValid() throws Exception {
        final ElasticIndexConfig elasticIndexConfig = createDataIndexDocRef();

        final Response response = queryClient.getDataSource(serviceUser, getDocRef(elasticIndexConfig));

        assertEquals(HttpStatus.SC_OK, response.getStatus());

        final String body = response.readEntity(String.class);

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
    public void testGetDataSourceMissingDocRef() throws Exception {

        // Create a random index config that is not registered with the system
        final DocRef elasticIndexConfig = new DocRef.Builder()
                .uuid(UUID.randomUUID().toString())
                .name("DoesNotExist")
                .build();

        final Response response = queryClient.getDataSource(serviceUser, elasticIndexConfig);

        assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatus());

        // Get
        checkAuditLogs(1);
    }

    @Test
    public void testSearchValid() throws Exception {
        final ElasticIndexConfig elasticIndexConfig = createDataIndexDocRef();

        final ExpressionOperator speakerFinder = new ExpressionOperator.Builder(ExpressionOperator.Op.AND)
                .addTerm(ShakespeareLine.SPEAKER, ExpressionTerm.Condition.EQUALS, "WARWICK")
                .addTerm(ShakespeareLine.TEXT_ENTRY, ExpressionTerm.Condition.CONTAINS, "pluck")
                .build();

        final SearchRequest searchRequest = dataSearchRequest.apply(elasticIndexConfig, speakerFinder);

        final Response response = queryClient.search(serviceUser, searchRequest);

        assertEquals(HttpStatus.SC_OK, response.getStatus());

        final String body = response.readEntity(String.class);

        LOGGER.info("BODY - " + body);

        final SearchResponse searchResponse = jacksonObjectMapper.readValue(body, SearchResponse.class);

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
    public void testSearchMissingDocRef() throws Exception {

        // Create a random index config that is not registered with the system
        final ElasticIndexConfig elasticIndexConfig = new ElasticIndexConfig.Builder()
                .uuid(UUID.randomUUID().toString())
                .indexName(UUID.randomUUID())
                .indexedType(UUID.randomUUID())
                .build();

        final ExpressionOperator speakerFinder = new ExpressionOperator.Builder(ExpressionOperator.Op.AND)
                .addTerm("aKey", ExpressionTerm.Condition.EQUALS, "aValue")
                .build();

        final SearchRequest searchRequest = dataSearchRequest.apply(elasticIndexConfig, speakerFinder);
        final Response response = queryClient.search(serviceUser, searchRequest);

        assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatus());


        checkAuditLogs(1);
    }

    /**
     * This test is used to check the behaviour of the system if you request data/info about
     * a docRef that exists, but the index does not exist.
     */
    @Test
    public void testSearchMissingIndex() throws Exception {

        final ElasticIndexConfig elasticIndexConfig = new ElasticIndexConfig.Builder()
                .uuid(UUID.randomUUID().toString())
                .indexName(UUID.randomUUID())
                .indexedType(UUID.randomUUID())
                .build();

        docRefClient.createDocument(serviceUser,
                elasticIndexConfig.getUuid(),
                elasticIndexConfig.getName(),
                PARENT_FOLDER_UUID);
        docRefClient.update(serviceUser,
                elasticIndexConfig.getUuid(),
                elasticIndexConfig);

        final ExpressionOperator speakerFinder = new ExpressionOperator.Builder(ExpressionOperator.Op.AND)
                .addTerm("aKey", ExpressionTerm.Condition.EQUALS, "aValue")
                .build();

        final SearchRequest searchRequest = dataSearchRequest.apply(elasticIndexConfig, speakerFinder);
        final Response response = queryClient.search(serviceUser, searchRequest);

        assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatus());

        checkAuditLogs(3);
    }

    /**
     * Shortcut function to create another DocRef that references the populated 'data' index.
     * @return The ElasticIndexConfig representing the new DocRef
     */
    private ElasticIndexConfig createDataIndexDocRef() throws Exception {
        final ElasticIndexConfig elasticIndexConfig = new ElasticIndexConfig.Builder()
                .uuid(UUID.randomUUID().toString())
                .name(DATA_INDEX_STROOM_NAME)
                .indexName(DATA_INDEX_NAME)
                .indexedType(DATA_INDEXED_TYPE)
                .build();

        final Response createDocRefResponse = docRefClient.createDocument(serviceUser,
                elasticIndexConfig.getUuid(),
                elasticIndexConfig.getName(),
                PARENT_FOLDER_UUID);
        assertEquals(HttpStatus.SC_OK, createDocRefResponse.getStatus());

        final Response updateIndexResponse =
                docRefClient.update(serviceUser,
                        elasticIndexConfig.getUuid(),
                        elasticIndexConfig);
        assertEquals(HttpStatus.SC_OK, updateIndexResponse.getStatus());

        return elasticIndexConfig;
    }
}
