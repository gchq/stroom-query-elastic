package stroom.query.elastic.hibernate;

import java.util.Optional;

public interface ElasticIndexConfigService {
    String STROOM_INDEX_NAME = "stroom";
    String DOC_REF_INDEXED_TYPE = "docref";

    Optional<ElasticIndexConfig> set(String uuid, ElasticIndexConfig update);
    Optional<ElasticIndexConfig> get(String uuid);
    void remove(String uuid);
}
