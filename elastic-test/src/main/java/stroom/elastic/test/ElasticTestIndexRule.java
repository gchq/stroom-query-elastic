package stroom.elastic.test;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpStatus;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientResponse;
import org.junit.rules.MethodRule;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static org.junit.Assert.*;

/**
 * This rule can be used to initialise an elastic index for testing.
 * The named index will be deleted so that any subsequent indexing commands from the test
 * are working with a blank slate.
 *
 * The rule can be given mappings and data files (held in class path resources) to initialise the index.
 */
public class ElasticTestIndexRule implements MethodRule, TestRule {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticTestIndexRule.class);

    public static final String ND_JSON = "application/x-ndjson";

    private final String elasticHttpUrl;
    private final String indexName;
    private final String indexUrl;
    private final String mappingsResource;
    private final String dataResource;

    private final Client httpClient = ClientBuilder.newClient(new ClientConfig().register(ClientResponse.class));

    private ElasticTestIndexRule(final Builder builder) {
        this.elasticHttpUrl = builder.elasticHttpUrl;
        this.indexName = builder.indexName;
        this.indexUrl = String.format("http://%s/%s", elasticHttpUrl, indexName);
        this.mappingsResource = builder.mappingsResource;
        this.dataResource = builder.dataResource;

        assertNotNull("Elastic HTTP URL Required", this.elasticHttpUrl);
        assertNotNull("Elastic Index Name Required", this.indexName);
    }

    public String getIndexUrl() {
        return indexUrl;
    }

    @Override
    public Statement apply(final Statement base,
                           final Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                try {
                    before();
                    base.evaluate();
                } finally {
                    after();
                }
            }
        };
    }

    protected void before() {
        try {
            // Delete any existing index with this name
            final Response response = httpClient.target(indexUrl)
                    .request()
                    .header("Content-Type", ND_JSON)
                    .delete(); // response may be 404 if first time run
            response.close();

            final ClassLoader classLoader = ElasticTestIndexRule.class.getClassLoader();

            // if mappings have been defined, preload them
            if (null != mappingsResource) {

                // Create index with mappings
                final String mappingsJson = IOUtils.toString(classLoader.getResourceAsStream(mappingsResource));
                final Response createMappingsResponse = httpClient
                        .target(indexUrl)
                        .request()
                        .header("accept", MediaType.APPLICATION_JSON)
                        .header("Content-Type", ElasticTestIndexRule.ND_JSON)
                        .put(Entity.json(mappingsJson));
                assertEquals(String.format("Failed with %s", createMappingsResponse.readEntity(String.class)),
                        HttpStatus.SC_OK,
                        createMappingsResponse.getStatus());
            }

            // If a data file resource has been given, try to load it
            if (null != dataResource) {
                final String dataJson = IOUtils.toString(classLoader.getResourceAsStream(dataResource));
                final String putDataUrl = String.format("%s/_bulk?pretty", indexUrl);
                final Response putDataResponse = httpClient
                        .target(putDataUrl)
                        .request()
                        .header("accept", MediaType.APPLICATION_JSON)
                        .header("Content-Type", ElasticTestIndexRule.ND_JSON)
                        .put(Entity.json(dataJson));
                assertEquals(String.format("Failed with %s", putDataResponse.readEntity(String.class)),
                        HttpStatus.SC_OK,
                        putDataResponse.getStatus());

                // Elastic takes a little time to actually make data available
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            LOGGER.error("Could not create index config", e);
            fail(e.getLocalizedMessage());
        }
    }

    protected void after() {
        // NOOP
    }

    @Override
    public Statement apply(final Statement base,
                           final FrameworkMethod method,
                           final Object target) {
        return apply(base, null);
    }

    public static Builder forIndex(final String indexName) {
        return new Builder(indexName);
    }

    public static class Builder {
        private String elasticHttpUrl;
        private final String indexName;
        private String mappingsResource;
        private String dataResource;

        public Builder(final String indexName) {
            this.indexName = indexName;
        }

        public Builder httpUrl(final String value) {
            this.elasticHttpUrl = value;
            return this;
        }

        public Builder mappingsResource(final String value) {
            this.mappingsResource = value;
            return this;
        }

        public Builder dataResource(final String value) {
            this.dataResource = value;
            return this;
        }

        public ElasticTestIndexRule build() {
            return new ElasticTestIndexRule(this);
        }
    }
}
