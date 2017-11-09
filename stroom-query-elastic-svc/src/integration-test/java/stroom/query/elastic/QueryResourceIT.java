package stroom.query.elastic;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import io.dropwizard.testing.junit.DropwizardAppRule;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpStatus;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.datasource.api.v2.DataSource;
import stroom.datasource.api.v2.DataSourceField;
import stroom.query.api.v2.*;
import stroom.query.audit.FifoLogbackAppender;
import stroom.query.elastic.hibernate.ElasticIndexConfig;

import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.dropwizard.testing.ResourceHelpers.resourceFilePath;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class QueryResourceIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryResourceIT.class);

    private static final DocRef ELASTIC_INDEX_DOC_REF = new DocRef.Builder()
            .uuid("4447107f-7343-4357-aff5-d872226678ee")
            .build();

    private static final String LOCALHOST = "localhost";
    private static final int ELASTIC_HTTP_PORT = 9200;
    private static final String INDEX_NAME = "shakespeare";
    private static final String INDEXED_TYPE = "line";
    private static final String ELASTIC_DATA_FILE = "elastic/shakespeare.json";
    private static final String ELASTIC_DATA_MAPPINGS_FULL_FILE = "elastic/shakespeare.mappings.json";

    @ClassRule
    public static final DropwizardAppRule<Config> appRule = new DropwizardAppRule<>(App.class, resourceFilePath("config.yml"));

    private static String queryUrl;
    private static String rawExplorerActionUrl;
    private static Function<String, String> explorerActionUrl = (uuid) ->
            String.format("%s/%s", rawExplorerActionUrl, uuid);

    private static final com.fasterxml.jackson.databind.ObjectMapper jacksonObjectMapper =
            new com.fasterxml.jackson.databind.ObjectMapper();

    private static String getQueryDataSourceUrl() {
        return String.format("%s/dataSource", queryUrl);
    }

    private static String getQuerySearchUrl() {
        return String.format("%s/search", queryUrl);
    }

    private static String getQueryDestroyUrl() {
        return String.format("%s/destroy", queryUrl);
    }

    @BeforeClass
    public static void setupClass() {

        int appPort = appRule.getLocalPort();
        queryUrl = String.format("http://%s:%d/queryApi/v1", LOCALHOST, appPort);
        rawExplorerActionUrl = String.format("http://%s:%d/explorerAction/v1", LOCALHOST, appPort);

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
            final ClassLoader classLoader = QueryResourceIT.class.getClassLoader();

            // Delete existing index
            final String indexUrl = String.format("http://%s:%d/%s", LOCALHOST, ELASTIC_HTTP_PORT, INDEX_NAME);
            Unirest
                    .delete(indexUrl)
                    .header("Content-Type", ND_JSON)
                    .asString(); // response may be 404 if first time run

            // Create index with mappings
            final String mappingsJson = IOUtils.toString(classLoader.getResourceAsStream(ELASTIC_DATA_MAPPINGS_FULL_FILE));
            final HttpResponse<String> createMappingsResponse = Unirest
                    .put(indexUrl)
                    .header("accept", MediaType.APPLICATION_JSON)
                    .header("Content-Type", ND_JSON)
                    .body(mappingsJson)
                    .asString();
            assertEquals(HttpStatus.SC_OK, createMappingsResponse.getStatus());

            // Post Data
            final String dataJson = IOUtils.toString(classLoader.getResourceAsStream(ELASTIC_DATA_FILE));
            final String putDataUrl = String.format("http://%s:%d/%s/_bulk?pretty", LOCALHOST, ELASTIC_HTTP_PORT, INDEX_NAME);
            final HttpResponse<String> putDataResponse = Unirest
                    .put(putDataUrl)
                    .header("accept", MediaType.APPLICATION_JSON)
                    .header("Content-Type", ND_JSON)
                    .body(dataJson)
                    .asString();
            assertEquals(HttpStatus.SC_OK, putDataResponse.getStatus());

            final HttpResponse<String> response = Unirest
                    .post(explorerActionUrl.apply(ELASTIC_INDEX_DOC_REF.getUuid()))
                    .header("accept", MediaType.APPLICATION_JSON)
                    .header("Content-Type", MediaType.APPLICATION_JSON)
                    .body(new ElasticIndexConfig.Builder()
                            .indexName(INDEX_NAME)
                            .indexedType(INDEXED_TYPE)
                            .build())
                    .asString();
            assertEquals(HttpStatus.SC_OK, response.getStatus());
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
    public void testSearch() {
        final ExpressionOperator speakerFinder = new ExpressionOperator.Builder(ExpressionOperator.Op.AND)
                .addTerm(ShakespeareLine.SPEAKER, ExpressionTerm.Condition.EQUALS, "WARWICK")
                .addTerm(ShakespeareLine.TEXT_ENTRY, ExpressionTerm.Condition.CONTAINS, "pluck")
                .build();

        final SearchResponse response = querySearch(speakerFinder);

        final List<ShakespeareLine> lines = response.getResults().stream()
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

        checkAuditLogs(1);
    }


    @Test
    public void testGetDataSource() {
        final DataSource result = getDataSource(ELASTIC_INDEX_DOC_REF);

        final Set<String> resultFieldNames = result.getFields().stream()
                .map(DataSourceField::getName)
                .collect(Collectors.toSet());

        assertTrue(resultFieldNames.contains(ShakespeareLine.LINE_ID));
        assertTrue(resultFieldNames.contains(ShakespeareLine.PLAY_NAME));
        assertTrue(resultFieldNames.contains(ShakespeareLine.SPEAKER));
        assertTrue(resultFieldNames.contains(ShakespeareLine.SPEECH_NUMBER));
        assertTrue(resultFieldNames.contains(ShakespeareLine.TEXT_ENTRY));

        checkAuditLogs(1);
    }

    private DataSource getDataSource(final DocRef docRef) {
        DataSource result = null;

        try {
            final HttpResponse<String> response = Unirest
                    .post(getQueryDataSourceUrl())
                    .header("accept", MediaType.APPLICATION_JSON)
                    .header("Content-Type", MediaType.APPLICATION_JSON)
                    .body(docRef)
                    .asString();

            assertEquals(HttpStatus.SC_OK, response.getStatus());

            final String body = response.getBody();

            LOGGER.info("Data Source Body: " + body);
            result = jacksonObjectMapper.readValue(body, DataSource.class);
        } catch (UnirestException | IOException e) {
            fail(e.getLocalizedMessage());
        }

        return result;
    }

    private SearchResponse querySearch(final ExpressionOperator expressionOperator) {
        SearchResponse result = null;

        try {
            final String queryKey = UUID.randomUUID().toString();
            final SearchRequest request = new SearchRequest.Builder()
                    .query()
                        .dataSource(ELASTIC_INDEX_DOC_REF)
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

            final HttpResponse<String> response = Unirest
                    .post(getQuerySearchUrl())
                    .header("accept", MediaType.APPLICATION_JSON)
                    .header("Content-Type", MediaType.APPLICATION_JSON)
                    .body(request)
                    .asString();

            assertEquals(HttpStatus.SC_OK, response.getStatus());

            LOGGER.info("BODY - " + response.getBody());

            result = jacksonObjectMapper.readValue(response.getBody(), SearchResponse.class);
        } catch (UnirestException | IOException e) {
            fail(e.getLocalizedMessage());

        }

        return result;
    }
}
