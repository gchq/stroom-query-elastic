package stroom.query.elastic.service;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.IndexNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.query.api.v2.DocRef;
import stroom.query.api.v2.DocRefInfo;
import stroom.query.audit.ExportDTO;
import stroom.query.audit.model.DocRefEntity;
import stroom.query.audit.service.DocRefService;
import stroom.query.audit.service.QueryApiException;
import stroom.query.elastic.model.ElasticIndexDocRefEntity;
import stroom.security.ServiceUser;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public abstract class AbstractElasticDocRefServiceImpl<T extends ElasticIndexDocRefEntity,
        T_BUILDER extends ElasticIndexDocRefEntity.BaseBuilder<T, ?>> implements DocRefService<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractElasticDocRefServiceImpl.class);

    public static final String STROOM_INDEX_NAME = "stroom";
    public static final String DOC_REF_INDEXED_TYPE = "docref";
    public static final String DOC_REF_ROLE = "DOC_REF_ROLE";

    private final TransportClient client;

    public AbstractElasticDocRefServiceImpl(final TransportClient client) {
        this.client = client;
    }

    @Override
    public List<T> getAll(final ServiceUser user) throws QueryApiException {
        return null;
    }

    private static final Function<String, Object> NULL_SOURCE = (n) -> null;
    private T_BUILDER build() {
        return build(NULL_SOURCE);
    }

    protected abstract T_BUILDER build(Function<String, Object> source);
    protected abstract void iterateFieldNames(Consumer<String> consumer);
    protected abstract void exportValues(T instance, BiConsumer<String, String> consumer);

    @Override
    public Optional<T> get(final ServiceUser user,
                           final String uuid) throws QueryApiException {
        try {
            final GetResponse searchResponse = client
                        .prepareGet(STROOM_INDEX_NAME, DOC_REF_INDEXED_TYPE, uuid)
                    .get();

            if (searchResponse.isExists() && !searchResponse.isSourceEmpty()) {
                final Map<String, Object> source = searchResponse.getSource();
                final Object stroomName = source.get(DocRefEntity.NAME);
                final Object createUser = source.get(DocRefEntity.CREATE_USER);
                final Object createTime = source.get(DocRefEntity.CREATE_TIME);
                final Object updateUser = source.get(DocRefEntity.UPDATE_USER);
                final Object updateTime = source.get(DocRefEntity.UPDATE_TIME);
                final Object indexName = source.get(ElasticIndexDocRefEntity.INDEX_NAME);
                final Object indexedType = source.get(ElasticIndexDocRefEntity.INDEXED_TYPE);

                return Optional.of(build(source::get)
                        .uuid(uuid)
                        .indexName(indexName)
                        .indexedType(indexedType)
                        .name((stroomName != null) ? stroomName.toString() : null)
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
            throw new QueryApiException(e);
        }
    }

    @Override
    public Optional<DocRefInfo> getInfo(final ServiceUser user,
                                        final String uuid) throws QueryApiException {
        try {
            final GetResponse searchResponse = client
                    .prepareGet(STROOM_INDEX_NAME, DOC_REF_INDEXED_TYPE, uuid)
                    .get();

            if (searchResponse.isExists() && !searchResponse.isSourceEmpty()) {
                final Map<String, Object> source = searchResponse.getSource();
                final Object name = source.get(DocRefEntity.NAME);
                final Object createUser = source.get(DocRefEntity.CREATE_USER);
                final Object createTime = source.get(DocRefEntity.CREATE_TIME);
                final Object updateUser = source.get(DocRefEntity.UPDATE_USER);
                final Object updateTime = source.get(DocRefEntity.UPDATE_TIME);

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
            throw new QueryApiException(e);
        }
    }

    @Override
    public Optional<T> createDocument(final ServiceUser user,
                                                       final String uuid,
                                                       final String name) throws QueryApiException {
        try {
            final Long now = System.currentTimeMillis();

            client.prepareIndex(STROOM_INDEX_NAME, DOC_REF_INDEXED_TYPE, uuid)
                    .setSource(jsonBuilder()
                            .startObject()
                            .field(DOC_REF_ROLE, getType())
                            .field(DocRefEntity.NAME, name)
                            .field(DocRefEntity.CREATE_TIME, now)
                            .field(DocRefEntity.CREATE_USER, user.getName())
                            .field(DocRefEntity.UPDATE_TIME, now)
                            .field(DocRefEntity.UPDATE_USER, user.getName())
                            .endObject()
                    )
                    .get();

            return Optional.of(build()
                    .uuid(uuid)
                    .name(name)
                    .build());
        } catch (IOException e) {
            LOGGER.warn("Could not update index config", e);
            throw new QueryApiException(e);
        }
    }

    @Override
    public Optional<T> update(final ServiceUser user,
                              final String uuid,
                              final T update) throws QueryApiException {
        try {
            final Long now = System.currentTimeMillis();

            final XContentBuilder json = jsonBuilder()
                    .startObject()
                    .field(DocRefEntity.UPDATE_TIME, now)
                    .field(DocRefEntity.UPDATE_USER, user.getName())
                    .field(ElasticIndexDocRefEntity.INDEX_NAME, update.getIndexName())
                    .field(ElasticIndexDocRefEntity.INDEXED_TYPE, update.getIndexedType());
            exportValues(update, (name, value) -> {
                try {
                    json.field(name, value);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });

            client.prepareUpdate(STROOM_INDEX_NAME, DOC_REF_INDEXED_TYPE, uuid)
                    .setDoc(json.endObject())
                    .get();
        } catch (IndexNotFoundException e) {
            return Optional.empty();
        } catch (IOException | RuntimeException e) {
            LOGGER.warn("Could not update index config", e);
            throw new QueryApiException(e);
        }

        return get(user, uuid);
    }

    @Override
    public Optional<T> copyDocument(final ServiceUser user,
                                    final String originalUuid,
                                    final String copyUuid) throws QueryApiException {
        try {
            final GetResponse searchResponse = client
                    .prepareGet(STROOM_INDEX_NAME, DOC_REF_INDEXED_TYPE, originalUuid)
                    .get();
            if (searchResponse.isExists() && !searchResponse.isSourceEmpty()) {
                final Map<String, Object> source = searchResponse.getSource();
                final Long now = System.currentTimeMillis();
                final String stroomName = source.get(DocRefEntity.NAME).toString();

                final XContentBuilder json = jsonBuilder()
                        .startObject()
                        .field(DOC_REF_ROLE, getType())
                        .field(DocRefEntity.NAME, stroomName)
                        .field(DocRefEntity.CREATE_TIME, now)
                        .field(DocRefEntity.CREATE_USER, user.getName())
                        .field(DocRefEntity.UPDATE_TIME, now)
                        .field(DocRefEntity.UPDATE_USER, user.getName())
                        .field(ElasticIndexDocRefEntity.INDEX_NAME, source.get(ElasticIndexDocRefEntity.INDEX_NAME))
                        .field(ElasticIndexDocRefEntity.INDEXED_TYPE, source.get(ElasticIndexDocRefEntity.INDEXED_TYPE));

                iterateFieldNames(n -> {
                    try {
                        json.field(n, source.get(n));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });

                client.prepareIndex(STROOM_INDEX_NAME, DOC_REF_INDEXED_TYPE, copyUuid)
                        .setSource(json.endObject())
                        .get();

                return get(user, copyUuid);
            } else {
                return Optional.empty();
            }
        } catch (IndexNotFoundException e) {
            return Optional.empty();
        } catch (IOException | RuntimeException e) {
            LOGGER.warn("Could not update index config", e);
            throw new QueryApiException(e);
        }
    }

    @Override
    public Optional<T> moveDocument(final ServiceUser user,
                                    final String uuid) throws QueryApiException {
        // two grapes? Who cares?!
        return get(user, uuid);
    }

    @Override
    public Optional<T> renameDocument(final ServiceUser user,
                                      final String uuid,
                                      final String name) throws QueryApiException {
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
            throw new QueryApiException(e);
        }

        return get(user, uuid);
    }

    @Override
    public Optional<Boolean> deleteDocument(final ServiceUser user,
                                            final String uuid) throws QueryApiException {
        client.prepareDelete(STROOM_INDEX_NAME, DOC_REF_INDEXED_TYPE, uuid).get();
        return Optional.of(Boolean.TRUE);
    }

    @Override
    public ExportDTO exportDocument(final ServiceUser user,
                                    final String uuid) throws QueryApiException {
        return get(user, uuid)
                .map(d -> {
                    ExportDTO.Builder b = ExportDTO
                            .withValue(DocRefEntity.NAME, d.getName())
                            .value(ElasticIndexDocRefEntity.INDEX_NAME, d.getIndexName())
                            .value(ElasticIndexDocRefEntity.INDEXED_TYPE, d.getIndexedType());
                    exportValues(d, b::value);
                    return b.build();
                })
                .orElse(ExportDTO.withMessage(String.format("Could not find document with %s", uuid)).build());
    }

    @Override
    public Optional<T> importDocument(final ServiceUser user,
                                                       final String uuid,
                                                       final String name,
                                                       final Boolean confirmed,
                                                       final Map<String, String> dataMap) throws QueryApiException {
        if (confirmed) {
            final Optional<T> index = createDocument(user, uuid, name);

            if (index.isPresent()) {
                final T indexConfig = build(dataMap::get)
                        .original(index.get())
                        .build();
                return update(user, uuid, indexConfig);
            } else {
                return Optional.empty();
            }
        } else {
            final Optional<T> existing = get(user, uuid);
            if (existing.isPresent()) {
                return Optional.empty();
            } else {
                return Optional.of(build(dataMap::get)
                        .uuid(uuid)
                        .name(name)
                        .build());
            }
        }
    }
}
