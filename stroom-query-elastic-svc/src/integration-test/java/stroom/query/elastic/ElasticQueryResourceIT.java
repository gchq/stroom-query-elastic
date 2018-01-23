package stroom.query.elastic;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpStatus;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientResponse;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.datasource.api.v2.DataSource;
import stroom.datasource.api.v2.DataSourceField;
import stroom.query.api.v2.*;
import stroom.query.audit.authorisation.DocumentPermission;
import stroom.query.audit.client.DocRefResourceHttpClient;
import stroom.query.audit.client.QueryResourceHttpClient;
import stroom.query.audit.logback.FifoLogbackAppender;
import stroom.query.elastic.config.Config;
import stroom.query.elastic.hibernate.ElasticIndexDocRefEntity;
import stroom.query.elastic.service.ElasticDocRefServiceImpl;
import stroom.query.testing.QueryResourceIT;

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

import static io.netty.util.NetUtil.LOCALHOST;
import static org.junit.Assert.*;

public class ElasticQueryResourceIT extends QueryResourceIT<ElasticIndexDocRefEntity, Config, App> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticQueryResourceIT.class);

    public ElasticQueryResourceIT() {
        super(App.class, ElasticIndexDocRefEntity.class, ElasticIndexDocRefEntity.TYPE);
    }

    // This Config is for the valid elastic index, the index and doc ref will be created
    // as part of the class setup
    private static final String DATA_INDEX_NAME = "shakespeare";
    private static final String DATA_INDEXED_TYPE = "line";

    private static final int ELASTIC_HTTP_PORT = 9200;
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
            final String elasticDocRefIndexUrl = String.format("http://%s:%d/%s", LOCALHOST, ELASTIC_HTTP_PORT, ElasticDocRefServiceImpl.STROOM_INDEX_NAME);
            httpClient.target(elasticDocRefIndexUrl)
                    .request()
                    .header("Content-Type", ND_JSON)
                    .delete(); // response may be 404 if first time run

            // Delete any existing 'data' index
            final String elasticDataIndexUrl = String.format("http://%s:%d/%s", LOCALHOST, ELASTIC_HTTP_PORT, DATA_INDEX_NAME);
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
            final String putDataUrl = String.format("http://%s:%d/%s/_bulk?pretty", LOCALHOST, ELASTIC_HTTP_PORT, DATA_INDEXED_TYPE);
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
                .type(getDocRefType())
                .name("DoesNotExist")
                .build();
        giveDocumentPermission(adminUser(), elasticIndexConfig.getUuid(), DocumentPermission.READ);

        final Response response = queryClient.getDataSource(adminUser(), elasticIndexConfig);

        assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatus());

        // Get
        checkAuditLogs(1);
    }

    @Test
    public void testSearchValid() throws Exception {
        final DocRef docRef = createDocument();

        final ExpressionOperator speakerFinder = new ExpressionOperator.Builder(ExpressionOperator.Op.AND)
                .addTerm(ShakespeareLine.SPEAKER, ExpressionTerm.Condition.EQUALS, "WARWICK")
                .addTerm(ShakespeareLine.TEXT_ENTRY, ExpressionTerm.Condition.CONTAINS, "pluck")
                .build();

        final SearchRequest searchRequest = getValidSearchRequest(docRef, speakerFinder, null);

        final Response response = queryClient.search(adminUser(), searchRequest);

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
    public void testSearchMissingDocRef() {

        // Create a random index config that is not registered with the system
        final DocRef docRef = new DocRef.Builder()
                .uuid(UUID.randomUUID().toString())
                .type(getDocRefType())
                .name(UUID.randomUUID().toString())
                .build();

        // Give permission to this non existent document
        giveDocumentPermission(adminUser(), docRef.getUuid(), DocumentPermission.READ);

        final ExpressionOperator speakerFinder = new ExpressionOperator.Builder(ExpressionOperator.Op.AND)
                .addTerm("aKey", ExpressionTerm.Condition.EQUALS, "aValue")
                .build();

        final SearchRequest searchRequest = getValidSearchRequest(docRef, speakerFinder, null);
        final Response response = queryClient.search(adminUser(), searchRequest);

        assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatus());

        checkAuditLogs(1);
    }

    /**
     * This test is used to check the behaviour of the system if you request data/info about
     * a docRef that exists, but the index does not exist.
     */
    @Test
    public void testSearchMissingIndex() throws Exception {

        final DocRef docRef = createDocument(new ElasticIndexDocRefEntity.Builder()
                .uuid(UUID.randomUUID().toString())
                .indexName(UUID.randomUUID())
                .indexedType(UUID.randomUUID())
                .build());

        final ExpressionOperator speakerFinder = new ExpressionOperator.Builder(ExpressionOperator.Op.AND)
                .addTerm("aKey", ExpressionTerm.Condition.EQUALS, "aValue")
                .build();

        final SearchRequest searchRequest = getValidSearchRequest(docRef, speakerFinder, null);
        final Response response = queryClient.search(adminUser(), searchRequest);

        assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatus());

        checkAuditLogs(3);
    }
}
