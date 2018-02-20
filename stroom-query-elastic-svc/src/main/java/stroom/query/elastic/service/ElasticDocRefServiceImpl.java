package stroom.query.elastic.service;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.IndexNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.query.api.v2.DocRef;
import stroom.query.api.v2.DocRefInfo;
import stroom.query.audit.ExportDTO;
import stroom.query.audit.model.DocRefEntity;
import stroom.query.audit.security.ServiceUser;
import stroom.query.audit.service.DocRefService;
import stroom.query.elastic.hibernate.ElasticIndexDocRefEntity;

import javax.inject.Inject;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class ElasticDocRefServiceImpl implements DocRefService<ElasticIndexDocRefEntity> {
    public static final String STROOM_INDEX_NAME = "stroom";
    public static final String DOC_REF_INDEXED_TYPE = "docref";

    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticDocRefServiceImpl.class);

    private final TransportClient client;

    @Inject
    public ElasticDocRefServiceImpl(final TransportClient client) {
        this.client = client;
    }

    @Override
    public String getType() {
        return "ElasticIndex";
    }

    @Override
    public List<ElasticIndexDocRefEntity> getAll(final ServiceUser user) throws Exception {
        return null;
    }

    @Override
    public Optional<ElasticIndexDocRefEntity> get(final ServiceUser user,
                                                  final String uuid) throws Exception {
        try {
            final GetResponse searchResponse = client
                    .prepareGet(STROOM_INDEX_NAME, DOC_REF_INDEXED_TYPE, uuid)
                    .get();

            if (searchResponse.isExists() && !searchResponse.isSourceEmpty()) {
                final Object stroomName = searchResponse.getSource().get(DocRefEntity.NAME);
                final Object indexName = searchResponse.getSource().get(ElasticIndexDocRefEntity.INDEX_NAME);
                final Object indexedType = searchResponse.getSource().get(ElasticIndexDocRefEntity.INDEXED_TYPE);
                final Object createUser = searchResponse.getSource().get(DocRefEntity.CREATE_USER);
                final Object createTime = searchResponse.getSource().get(DocRefEntity.CREATE_TIME);
                final Object updateUser = searchResponse.getSource().get(DocRefEntity.UPDATE_USER);
                final Object updateTime = searchResponse.getSource().get(DocRefEntity.UPDATE_TIME);

                if ((null != indexName) && (null != indexedType)) {
                    try {
                        client.admin().indices()
                                .prepareGetIndex()
                                .addIndices(indexName.toString())
                                .addTypes(indexedType.toString())
                                .get();
                    } catch (Exception e) {
                        LOGGER.debug(String.format("Could not get mappings for index %s", indexName));
                    }
                }

                return Optional.of(new ElasticIndexDocRefEntity.Builder()
                        .uuid(uuid)
                        .name((stroomName != null) ? stroomName.toString() : null)
                        .indexName(indexName)
                        .indexedType(indexedType)
                        .createUser((createUser != null) ? createUser.toString() : null)
                        .createTime((createTime != null) ? Long.valueOf(createTime.toString()) : null)
                        .updateUser((updateUser != null) ? updateUser.toString() : null)
                        .updateTime((updateTime != null) ? Long.valueOf(updateTime.toString()) : null)
                        .build());
            } else {
                return Optional.empty();
            }
        } catch (IndexNotFoundException e) {
            return Optional.empty();
        } catch (Exception e) {
            LOGGER.warn("Could not update index config", e);
            throw new Exception(e);
        }
    }

    @Override
    public Optional<DocRefInfo> getInfo(final ServiceUser user,
                                        final String uuid) throws Exception {
        try {
            final GetResponse searchResponse = client
                    .prepareGet(STROOM_INDEX_NAME, DOC_REF_INDEXED_TYPE, uuid)
                    .get();

            if (searchResponse.isExists() && !searchResponse.isSourceEmpty()) {
                final Object name = searchResponse.getSource().get(DocRefEntity.NAME);
                final Object createUser = searchResponse.getSource().get(DocRefEntity.CREATE_USER);
                final Object createTime = searchResponse.getSource().get(DocRefEntity.CREATE_TIME);
                final Object updateUser = searchResponse.getSource().get(DocRefEntity.UPDATE_USER);
                final Object updateTime = searchResponse.getSource().get(DocRefEntity.UPDATE_TIME);

                return Optional.of(new DocRefInfo.Builder()
                        .docRef(new DocRef.Builder()
                                .uuid(uuid)
                                .name((name != null) ? name.toString() : null)
                                .build())
                        .createUser((createUser != null) ? createUser.toString() : null)
                        .createTime((createTime != null) ? Long.valueOf(createTime.toString()) : null)
                        .updateUser((updateUser != null) ? updateUser.toString() : null)
                        .updateTime((updateTime != null) ? Long.valueOf(updateTime.toString()) : null)
                        .build());
            } else {
                return Optional.empty();
            }
        } catch (IndexNotFoundException e) {
            return Optional.empty();
        } catch (Exception e) {
            LOGGER.warn("Could not update index config", e);
            throw new Exception(e);
        }
    }

    @Override
    public Optional<ElasticIndexDocRefEntity> createDocument(final ServiceUser user,
                                                       final String uuid,
                                                       final String name) throws Exception {
        try {
            final Long now = System.currentTimeMillis();

            client.prepareIndex(STROOM_INDEX_NAME, DOC_REF_INDEXED_TYPE, uuid)
                    .setSource(jsonBuilder()
                            .startObject()
                            .field(DocRefEntity.NAME, name)
                            .field(DocRefEntity.CREATE_TIME, now)
                            .field(DocRefEntity.CREATE_USER, user.getName())
                            .field(DocRefEntity.UPDATE_TIME, now)
                            .field(DocRefEntity.UPDATE_USER, user.getName())
                            .endObject()
                    )
                    .get();

            return Optional.of(new ElasticIndexDocRefEntity.Builder()
                    .uuid(uuid)
                    .name(name)
                    .build());
        } catch (IOException e) {
            LOGGER.warn("Could not update index config", e);
            throw new Exception(e);
        }
    }

    @Override
    public Optional<ElasticIndexDocRefEntity> update(final ServiceUser user,
                                               final String uuid,
                                               final ElasticIndexDocRefEntity update) throws Exception {
        try {
            final Long now = System.currentTimeMillis();

            client.prepareUpdate(STROOM_INDEX_NAME, DOC_REF_INDEXED_TYPE, uuid)
                    .setDoc(jsonBuilder()
                            .startObject()
                            .field(ElasticIndexDocRefEntity.INDEX_NAME, update.getIndexName())
                            .field(ElasticIndexDocRefEntity.INDEXED_TYPE, update.getIndexedType())
                            .field(DocRefEntity.UPDATE_TIME, now)
                            .field(DocRefEntity.UPDATE_USER, user.getName())
                            .endObject()
                    )
                    .get();
        } catch (IndexNotFoundException e) {
            return Optional.empty();
        } catch (IOException e) {
            LOGGER.warn("Could not update index config", e);
            throw new Exception(e);
        }

        return get(user, uuid);
    }

    @Override
    public Optional<ElasticIndexDocRefEntity> copyDocument(final ServiceUser user,
                                                     final String originalUuid,
                                                     final String copyUuid) throws Exception {
        try {
            final GetResponse searchResponse = client
                    .prepareGet(STROOM_INDEX_NAME, DOC_REF_INDEXED_TYPE, originalUuid)
                    .get();
            if (searchResponse.isExists() && !searchResponse.isSourceEmpty()) {
                final Long now = System.currentTimeMillis();
                final String stroomName = searchResponse.getSource().get(DocRefEntity.NAME).toString();
                final Object indexName = searchResponse.getSource().get(ElasticIndexDocRefEntity.INDEX_NAME);
                final Object indexedType = searchResponse.getSource().get(ElasticIndexDocRefEntity.INDEXED_TYPE);

                client.prepareIndex(STROOM_INDEX_NAME, DOC_REF_INDEXED_TYPE, copyUuid)
                        .setSource(jsonBuilder()
                                .startObject()
                                .field(DocRefEntity.NAME, stroomName)
                                .field(ElasticIndexDocRefEntity.INDEX_NAME, indexName)
                                .field(ElasticIndexDocRefEntity.INDEXED_TYPE, indexedType)
                                .field(DocRefEntity.CREATE_TIME, now)
                                .field(DocRefEntity.CREATE_USER, user.getName())
                                .field(DocRefEntity.UPDATE_TIME, now)
                                .field(DocRefEntity.UPDATE_USER, user.getName())
                                .endObject()
                        )
                        .get();

                return get(user, copyUuid);
            } else {
                return Optional.empty();
            }
        } catch (IndexNotFoundException e) {
            return Optional.empty();
        } catch (IOException e) {
            LOGGER.warn("Could not update index config", e);
            throw new Exception(e);
        }
    }

    @Override
    public Optional<ElasticIndexDocRefEntity> moveDocument(final ServiceUser user,
                                                     final String uuid) throws Exception {
        // two grapes? Who cares?!
        return get(user, uuid);
    }

    @Override
    public Optional<ElasticIndexDocRefEntity> renameDocument(final ServiceUser user,
                                                       final String uuid,
                                                       final String name) throws Exception {
        try {
            final Long now = System.currentTimeMillis();

            client.prepareUpdate(STROOM_INDEX_NAME, DOC_REF_INDEXED_TYPE, uuid)
                    .setDoc(jsonBuilder()
                            .startObject()
                            .field(DocRefEntity.NAME, name)
                            .field(DocRefEntity.UPDATE_TIME, now)
                            .field(DocRefEntity.UPDATE_USER, user.getName())
                            .endObject()
                    )
                    .get();
        } catch (IndexNotFoundException e) {
            return Optional.empty();
        } catch (IOException e) {
            LOGGER.warn("Could not update index config", e);
            throw new Exception(e);
        }

        return get(user, uuid);
    }

    @Override
    public Optional<Boolean> deleteDocument(final ServiceUser user,
                                            final String uuid) throws Exception {
        client.prepareDelete(STROOM_INDEX_NAME, DOC_REF_INDEXED_TYPE, uuid).get();
        return Optional.of(Boolean.TRUE);
    }

    @Override
    public ExportDTO exportDocument(final ServiceUser user,
                                    final String uuid) throws Exception {
        return get(user, uuid)
                .map(d -> ExportDTO
                        .withValue(ElasticIndexDocRefEntity.INDEX_NAME, d.getIndexName())
                        .value(ElasticIndexDocRefEntity.INDEXED_TYPE, d.getIndexedType())
                        .value(DocRefEntity.NAME, d.getName())
                        .build())
                .orElse(ExportDTO.withMessage(String.format("Could not find document with %s", uuid)).build());
    }

    @Override
    public Optional<ElasticIndexDocRefEntity> importDocument(final ServiceUser user,
                                                       final String uuid,
                                                       final String name,
                                                       final Boolean confirmed,
                                                       final Map<String, String> dataMap) throws Exception {
        if (confirmed) {
            final Optional<ElasticIndexDocRefEntity> index = createDocument(user, uuid, name);

            if (index.isPresent()) {
                final ElasticIndexDocRefEntity indexConfig = index.get();
                indexConfig.setIndexName(dataMap.get(ElasticIndexDocRefEntity.INDEX_NAME));
                indexConfig.setIndexedType(dataMap.get(ElasticIndexDocRefEntity.INDEXED_TYPE));
                return update(user, uuid, indexConfig);
            } else {
                return Optional.empty();
            }
        } else {
            final Optional<ElasticIndexDocRefEntity> existing = get(user, uuid);
            if (existing.isPresent()) {
                return Optional.empty();
            } else {
                return Optional.of(new ElasticIndexDocRefEntity.Builder()
                        .uuid(uuid)
                        .name(name)
                        .indexName(dataMap.get(ElasticIndexDocRefEntity.INDEX_NAME))
                        .indexedType(dataMap.get(ElasticIndexDocRefEntity.INDEXED_TYPE))
                        .build());
            }
        }
    }
}
