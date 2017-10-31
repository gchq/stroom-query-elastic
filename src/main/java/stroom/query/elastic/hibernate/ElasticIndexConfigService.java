package stroom.query.elastic.hibernate;

import stroom.query.api.v2.DocRef;

import java.util.Optional;

public interface ElasticIndexConfigService {
    Optional<ElasticIndexConfig> get(DocRef docRef);
}
