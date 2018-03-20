package stroom.autoindex;

/**
 * A series of configuration values, must be kept in sync with the configuration resources
 */
public interface TestConstants {
    String ELASTIC_APP_CONFIG = "elastic/config.yml";
    String ANIMALS_APP_CONFIG = "animal/config.yml";
    String AUTO_INDEX_APP_CONFIG = "autoindex/config.yml";

    String LOCAL_ELASTIC_HTTP_HOST = "localhost:19200";
    String AUTO_INDEX_APP_HOST = "http://localhost:9899";
    String ANIMAL_APP_HOST = "http://localhost:8899";
    String ELASTIC_APP_HOST = "http://localhost:18299";

    int TEST_AUTH_PORT = 10080;
}
