package stroom.query.elastic.service;

import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.query.api.v2.DocRef;
import stroom.query.api.v2.DocRefInfo;
import stroom.query.audit.ExportDTO;
import stroom.query.elastic.hibernate.ElasticIndexConfig;
import stroom.util.shared.QueryApiException;

import javax.inject.Inject;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class ElasticDocRefServiceImpl implements ElasticDocRefService {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticDocRefServiceImpl.class);

    private final TransportClient client;

    @Inject
    public ElasticDocRefServiceImpl(final TransportClient client) {
        this.client = client;
    }

    @Override
    public List<ElasticIndexConfig> getAll() throws QueryApiException {
        return null;
    }

    @Override
    public Optional<ElasticIndexConfig> get(final String uuid) throws QueryApiException {
        try {
            final GetResponse searchResponse = client
                    .prepareGet(STROOM_INDEX_NAME, DOC_REF_INDEXED_TYPE, uuid)
                    .get();

            if (searchResponse.isExists() && !searchResponse.isSourceEmpty()) {
                final Object stroomName = searchResponse.getSource().get(ElasticIndexConfig.STROOM_NAME);
                final Object indexName = searchResponse.getSource().get(ElasticIndexConfig.INDEX_NAME);
                final Object indexedType = searchResponse.getSource().get(ElasticIndexConfig.INDEXED_TYPE);

                if ((null != indexName) && (null != indexedType)) {
                    try {
                        final GetIndexResponse getIndexResponse = client.admin().indices()
                                .prepareGetIndex()
                                .addIndices(indexName.toString())
                                .addTypes(indexedType.toString())
                                .get();

                        LOGGER.info("Mappings Returned " + getIndexResponse);
                    } catch (Exception e) {
                        LOGGER.warn(String.format("Could not get mappings for index %s", indexName));
                    }
                }

                return Optional.of(new ElasticIndexConfig.Builder()
                        .uuid(uuid)
                        .stroomName(stroomName)
                        .indexName(indexName)
                        .indexedType(indexedType)
                        .build());
            } else {
                return Optional.empty();
            }
        } catch (Exception e) {
            LOGGER.warn("Could not update index config", e);
            throw new QueryApiException(e);
        }
    }

    @Override
    public Optional<DocRefInfo> getInfo(String uuid) throws QueryApiException {
        try {
            final GetResponse searchResponse = client
                    .prepareGet(STROOM_INDEX_NAME, DOC_REF_INDEXED_TYPE, uuid)
                    .get();

            if (searchResponse.isExists() && !searchResponse.isSourceEmpty()) {
                final Object stroomName = searchResponse.getSource().get(ElasticIndexConfig.STROOM_NAME);

                return Optional.of(new DocRefInfo.Builder()
                        .docRef(new DocRef.Builder()
                                .uuid(uuid)
                                .name((stroomName != null) ? stroomName.toString() : null)
                                .build())
                        .build());
            } else {
                return Optional.empty();
            }
        } catch (Exception e) {
            LOGGER.warn("Could not update index config", e);
            throw new QueryApiException(e);
        }
    }

    @Override
    public Optional<ElasticIndexConfig> createDocument(final String uuid,
                                                       final String name) throws QueryApiException {
        try {
            client.prepareIndex(STROOM_INDEX_NAME, DOC_REF_INDEXED_TYPE, uuid)
                    .setSource(jsonBuilder()
                            .startObject()
                            .field(ElasticIndexConfig.STROOM_NAME, name)
                            .endObject()
                    )
                    .get();

            return Optional.of(new ElasticIndexConfig.Builder()
                    .uuid(uuid)
                    .stroomName(name)
                    .build());
        } catch (IOException e) {
            LOGGER.warn("Could not update index config", e);
            throw new QueryApiException(e);
        }
    }

    @Override
    public Optional<ElasticIndexConfig> update(final String uuid,
                                               final ElasticIndexConfig update) throws QueryApiException {
        try {
            client.prepareUpdate(STROOM_INDEX_NAME, DOC_REF_INDEXED_TYPE, uuid)
                    .setDoc(jsonBuilder()
                            .startObject()
                            .field(ElasticIndexConfig.INDEX_NAME, update.getIndexName())
                            .field(ElasticIndexConfig.INDEXED_TYPE, update.getIndexedType())
                            .endObject()
                    )
                    .get();
        } catch (IOException e) {
            LOGGER.warn("Could not update index config", e);
            throw new QueryApiException(e);
        }

        return get(uuid);
    }

    @Override
    public Optional<ElasticIndexConfig> copyDocument(final String originalUuid,
                                                     final String copyUuid) throws QueryApiException {
        try {
            final GetResponse searchResponse = client
                    .prepareGet(STROOM_INDEX_NAME, DOC_REF_INDEXED_TYPE, originalUuid)
                    .get();
            if (searchResponse.isExists() && !searchResponse.isSourceEmpty()) {
                final String stroomName = searchResponse.getSource().get(ElasticIndexConfig.STROOM_NAME).toString();
                final String indexName = searchResponse.getSource().get(ElasticIndexConfig.INDEX_NAME).toString();
                final String indexedType = searchResponse.getSource().get(ElasticIndexConfig.INDEXED_TYPE).toString();

                client.prepareIndex(STROOM_INDEX_NAME, DOC_REF_INDEXED_TYPE, copyUuid)
                        .setSource(jsonBuilder()
                                .startObject()
                                .field(ElasticIndexConfig.STROOM_NAME, stroomName)
                                .field(ElasticIndexConfig.INDEX_NAME, indexName)
                                .field(ElasticIndexConfig.INDEXED_TYPE, indexedType)
                                .endObject()
                        )
                        .get();

                return get(copyUuid);
            } else {
                return Optional.empty();
            }
        } catch (IOException e) {
            LOGGER.warn("Could not update index config", e);
            throw new QueryApiException(e);
        }
    }

    @Override
    public Optional<ElasticIndexConfig> documentMoved(final String uuid) throws QueryApiException {
        // two grapes? Who cares?!
        return get(uuid);
    }

    @Override
    public Optional<ElasticIndexConfig> documentRenamed(final String uuid,
                                                        final String name) throws QueryApiException {
        try {
            client.prepareUpdate(STROOM_INDEX_NAME, DOC_REF_INDEXED_TYPE, uuid)
                    .setDoc(jsonBuilder()
                            .startObject()
                            .field(ElasticIndexConfig.STROOM_NAME, name)
                            .endObject()
                    )
                    .get();
        } catch (IOException e) {
            LOGGER.warn("Could not update index config", e);
            throw new QueryApiException(e);
        }

        return get(uuid);
    }

    @Override
    public void deleteDocument(final String uuid) throws QueryApiException {
        client.prepareDelete(STROOM_INDEX_NAME, DOC_REF_INDEXED_TYPE, uuid).get();
    }

    @Override
    public ExportDTO exportDocument(final String uuid) throws QueryApiException {
        final Optional<ElasticIndexConfig> index = get(uuid);

        if (index.isPresent()) {
            final ElasticIndexConfig indexConfig = index.get();

            return ExportDTO
                    .withValue(ElasticIndexConfig.INDEX_NAME, indexConfig.getIndexName())
                    .value(ElasticIndexConfig.INDEXED_TYPE, indexConfig.getIndexedType())
                    .build();
        } else {
            return ExportDTO.withMessage(String.format("Could not find document with %s", uuid)).build();
        }

    }

    @Override
    public Optional<ElasticIndexConfig> importDocument(final String uuid,
                                                       final String name,
                                                       final Boolean confirmed,
                                                       final Map<String, String> dataMap) throws QueryApiException {
        if (confirmed) {
            final Optional<ElasticIndexConfig> index = createDocument(uuid, name);

            if (index.isPresent()) {
                final ElasticIndexConfig indexConfig = index.get();
                indexConfig.setIndexName(dataMap.get(ElasticIndexConfig.INDEX_NAME));
                indexConfig.setIndexedType(dataMap.get(ElasticIndexConfig.INDEXED_TYPE));
                return update(uuid, indexConfig);
            } else {
                return Optional.empty();
            }
        } else {
            final Optional<ElasticIndexConfig> existing = get(uuid);
            if (existing.isPresent()) {
                return Optional.empty();
            } else {
                return Optional.of(new ElasticIndexConfig.Builder()
                        .uuid(uuid)
                        .stroomName(name)
                        .indexName(dataMap.get(ElasticIndexConfig.INDEX_NAME))
                        .indexedType(dataMap.get(ElasticIndexConfig.INDEXED_TYPE))
                        .build());
            }
        }
    }
}
