package stroom.autoindex;

/**
 * A series of configuration values, must be kept in sync with the configuration resources
 */
public interface TestConstants {
    String ELASTIC_APP_CONFIG = "elastic/config.yml";
    String ANIMALS_APP_CONFIG = "animal/config.yml";
    String AUTO_INDEX_APP_CONFIG = "autoindex/config.yml";
    String AUTO_INDEX_APP_CONFIG_NO_INDEXING = "autoindex/config_no_indexing.yml";

    String LOCAL_ELASTIC_HTTP_HOST = "localhost:19200";
    String AUTO_INDEX_APP_HOST_WITH_INDEXING = "http://localhost:9899";
    String AUTO_INDEX_APP_HOST_NO_INDEXING = "http://localhost:29899";
    String ANIMAL_APP_HOST = "http://localhost:8899";
    String ELASTIC_APP_HOST = "http://localhost:18299";

    int TEST_AUTH_PORT = 10080;
}
