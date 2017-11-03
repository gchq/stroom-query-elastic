package stroom.query.elastic;

import org.apache.commons.io.IOUtils;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Scanner;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ElasticIndexRule implements TestRule {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticIndexRule.class);

    private String hostname;
    private int port;
    private String mappingsFile;
    private String clusterName;
    private String testDataFile;
    private String indexName;
    private String indexedType;

    public ElasticIndexRule hostname(final String value) {
        this.hostname = value;
        return this;
    }

    public ElasticIndexRule port(final int value) {
        this.port = value;
        return this;
    }

    public ElasticIndexRule clusterName(final String value) {
        this.clusterName = value;
        return this;
    }

    public ElasticIndexRule testDataFile(final String value) {
        this.testDataFile = value;
        return this;
    }

    public ElasticIndexRule indexName(final String value) {
        this.indexName = value;
        return this;
    }

    public ElasticIndexRule indexedType(final String value) {
        this.indexedType = value;
        return this;
    }

    public ElasticIndexRule mappingsFile(final String value) {
        this.mappingsFile = value;
        return this;
    }

    @Override
    public Statement apply(final Statement statement, final Description description) {
        assertNotNull("Hostname not set", this.hostname);
        assertTrue("Elastic Port not set", this.port > 0);
        assertNotNull("Cluster Name not set", this.clusterName);
        assertNotNull("Test Data File not set", this.testDataFile);
        assertNotNull("Index Name not set", this.indexName);
        assertNotNull("Indexed Type not set", this.indexedType);
        assertNotNull("Indexed Type not set", this.mappingsFile);

        try {
            final Settings settings = Settings.builder()
                    .put("cluster.name", clusterName).build();

            final TransportClient client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(hostname), port));

            // Clear the index
            LOGGER.info("Clearing the Index");
            try {
                client.admin().indices().prepareDelete(indexName).get();
            } catch (IndexNotFoundException e) {
                LOGGER.info("Index Not Found - Must be Fresh Install");
            }

            final ClassLoader classLoader = QueryResourceIT.class.getClassLoader();

            LOGGER.info("Creating Fresh Index");
            try {
                final String mappingsJson = IOUtils.toString(classLoader.getResourceAsStream(mappingsFile));
                client.admin().indices().prepareCreate(this.indexName)
                        .addMapping(this.indexedType, mappingsJson, XContentType.JSON)
                        .get();
            } catch (IOException e) {
                e.printStackTrace();
                fail(e.getLocalizedMessage());
            }

            // Create the bulk import
            LOGGER.info("Preparing Bulk Ingest");
            final BulkRequestBuilder bulkRequest = client.prepareBulk();

            // Load lines of JSON from resource file
            File file = new File(classLoader.getResource(testDataFile).getFile());

            try (Scanner scanner = new Scanner(file)) {

                while (scanner.hasNextLine()) {
                    String line = scanner.nextLine();
                    if (line.contains("_type")) { // don't ingest the marker types, we just want the data
                        bulkRequest.add(client.prepareIndex(indexName, indexedType).setSource(line, XContentType.JSON));
                    }
                }

                scanner.close();

            } catch (IOException e) {
                e.printStackTrace();
            }

            LOGGER.info("Executing Bulk Ingest");
            BulkResponse bulkResponse = bulkRequest.get();
            if (bulkResponse.hasFailures()) {
                // process failures by iterating through each bulk response item
                LOGGER.warn("Errors Encountered on Bulk Ingest of Test Data: " + bulkResponse.buildFailureMessage());
                fail("Elastic Test Data Ingest Failed");
            }

            client.close();

            LOGGER.info("Elastic Search Populated with Test Data");

            return statement;
        } catch (UnknownHostException e) {
            e.printStackTrace();
            fail(e.getLocalizedMessage());
        }

        return null;
    }
}
