package stroom.query.elastic;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import io.dropwizard.testing.junit.DropwizardAppRule;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpStatus;
import org.junit.*;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.datasource.api.v2.DataSource;
import stroom.datasource.api.v2.DataSourceField;
import stroom.query.api.v2.*;
import stroom.query.audit.FifoLogbackAppender;

import javax.ws.rs.core.MediaType;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static io.dropwizard.testing.ResourceHelpers.resourceFilePath;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class QueryResourceIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryResourceIT.class);

    private static final String TEST_DB_FILENAME = "test.db";
    private static final DocRef ELASTIC_INDEX_DOC_REF = new DocRef.Builder()
            .uuid(UUID.randomUUID().toString())
            .name("Test Elastic")
            .type("ElasticIndex")
            .build();

    @ClassRule
    public static final TestRule initialiseDb = (statement, description) -> {
        LOGGER.info("Setting up Test Database");

        if (new File(TEST_DB_FILENAME).delete()) {
            LOGGER.info("Found an existing test database, deleted it");
        }

        try {
            final String sql = IOUtils.toString(QueryResourceIT.class.getClassLoader().getResourceAsStream("initDatabase.sql"));
            final Connection connection = DriverManager.getConnection(String.format("jdbc:sqlite:%s", TEST_DB_FILENAME));
            final java.sql.Statement stmt = connection.createStatement();
            stmt.execute(sql);
            stmt.close();


            final PreparedStatement pstmt = connection.prepareStatement("INSERT INTO ELASTIC_INDEX (UUID, INDEX_NAME, INDEXED_TYPE) VALUES (?, ?, ?)");
            pstmt.setString(1, ELASTIC_INDEX_DOC_REF.getUuid());
            pstmt.setString(2, "shakespeare");
            pstmt.setString(3, "line");
            pstmt.execute();

            connection.close();
        } catch (IOException | SQLException e) {
            e.printStackTrace();
            fail(e.getLocalizedMessage());
        }

        return statement;
    };

    @ClassRule
    public static final DropwizardAppRule<Config> appRule = new DropwizardAppRule<>(App.class, resourceFilePath("config.yml"));

    private static String queryUrl;

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
        queryUrl = "http://localhost:" + appPort + "/queryApi/v1";

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
                .addTerm("speaker", ExpressionTerm.Condition.EQUALS, "KING")
                .addTerm("text_entry", ExpressionTerm.Condition.CONTAINS, "these")
                .build();

        querySearch(speakerFinder);

        checkAuditLogs(1);
    }


    @Test
    public void testGetDataSource() {
        final DataSource result = getDataSource(ELASTIC_INDEX_DOC_REF);

        final Set<String> resultFieldNames = result.getFields().stream()
                .map(DataSourceField::getName)
                .collect(Collectors.toSet());

        LOGGER.info("Field Names: " + resultFieldNames);

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
                        .dataSource("docRefName", UUID.randomUUID().toString(), "docRefType")
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
                            .addField("speaker", "${" + "speaker" + "}").end()
                            .addField("text_entry", "${" + "text_entry" + "}").end()
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
