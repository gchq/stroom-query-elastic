package stroom.query.elastic;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import io.dropwizard.testing.junit.DropwizardAppRule;
import org.apache.http.HttpStatus;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.query.api.v2.*;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import static io.dropwizard.testing.ResourceHelpers.resourceFilePath;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class QueryResourceIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryResourceIT.class);

    @ClassRule
    public static final DropwizardAppRule<Config> appRule = new DropwizardAppRule<>(App.class, resourceFilePath("config.yml"));

    private static String queryUrl;

    private static KafkaAuditTestConsumer kafkaTestConsumer;

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

        queryUrl = "http://localhost:" + appPort + "/elasticQuery/v1";

        kafkaTestConsumer = new KafkaAuditTestConsumer();

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

    @AfterClass
    public static void afterClass() {
        kafkaTestConsumer.close();
    }

    @Before
    public void beforeTest() {
    }

    @After
    public void afterTest() {
        kafkaTestConsumer.commitSync();
    }

    private void checkAuditLogs(final int expected) {
        final List<ConsumerRecord<String, String>> records = kafkaTestConsumer.getRecords(expected);

        for (ConsumerRecord<String, String> record : records) {
            //LOGGER.info(String.format("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value()));
        }

        LOGGER.info(String.format("Expected %d records, received %d", expected, records.size()));

        assertEquals(expected, records.size());
    }

    @Test
    public void testSearch() {
        querySearch(new ExpressionOperator.Builder(ExpressionOperator.Op.AND).build());

        checkAuditLogs(1);
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
                            .addField("MyField", "${" + "MyField" + "}").end()
                            .addMaxResults(10)
                            .end()
                        .end()
                    .build();

            final HttpResponse<String> response = Unirest
                    .post(getQuerySearchUrl())
                    .header("accept", "application/json")
                    .header("Content-Type", "application/json")
                    .body(request)
                    .asString();

            assertEquals(HttpStatus.SC_OK, response.getStatus());

            //result = jacksonObjectMapper.readValue(response.getBody(), SearchResponse.class);
        } catch (UnirestException e) {
            fail(e.getLocalizedMessage());

        }

        return result;
    }
}
