package stroom.query.csv;

import stroom.query.audit.ExportDTO;
import stroom.query.audit.model.DocRefEntity;
import stroom.security.ServiceUser;
import stroom.query.audit.service.DocRefService;
import stroom.query.audit.service.QueryApiException;

import java.util.*;

public class CsvDocRefServiceImpl implements DocRefService<CsvDocRefEntity> {
    private static Map<String, CsvDocRefEntity> data = new HashMap<>();

    public static void eraseAllData() {
        data.clear();
    }

    @Override
    public String getType() {
        return CsvDocRefEntity.TYPE;
    }

    @Override
    public List<CsvDocRefEntity> getAll(final ServiceUser user) throws QueryApiException {
        return new ArrayList<>(data.values());
    }

    @Override
    public Optional<CsvDocRefEntity> get(final ServiceUser user,
                                         final String uuid) throws QueryApiException {
        return Optional.ofNullable(data.get(uuid));
    }

    @Override
    public Optional<CsvDocRefEntity> createDocument(final ServiceUser user,
                                                    final String uuid,
                                                    final String name) throws QueryApiException {
        final Long now = System.currentTimeMillis();
        data.put(uuid, new CsvDocRefEntity.Builder()
                .uuid(uuid)
                .name(name)
                .createUser(user.getName())
                .createTime(now)
                .updateUser(user.getName())
                .updateTime(now)
                .build());

        return get(user, uuid);
    }

    @Override
    public Optional<CsvDocRefEntity> update(final ServiceUser user,
                                            final String uuid,
                                            final CsvDocRefEntity updatedConfig) throws QueryApiException {
        return get(user, uuid)
                .map(d -> new CsvDocRefEntity.Builder(d)
                        .updateTime(System.currentTimeMillis())
                        .updateUser(user.getName())
                        .dataDirectory(updatedConfig.getDataDirectory())
                        .build());
    }

    @Override
    public Optional<CsvDocRefEntity> copyDocument(final ServiceUser user,
                                                  final String originalUuid,
                                                  final String copyUuid) throws QueryApiException {
        final CsvDocRefEntity existing = data.get(originalUuid);
        if (null != existing) {
            createDocument(user, copyUuid, existing.getName());
            return update(user, copyUuid, existing);
        } else {
            return Optional.empty();
        }
    }

    @Override
    public Optional<CsvDocRefEntity> moveDocument(final ServiceUser user,
                                                  final String uuid) throws QueryApiException {
        return get(user, uuid);
    }

    @Override
    public Optional<CsvDocRefEntity> renameDocument(final ServiceUser user,
                                                    final String uuid,
                                                    final String name) throws QueryApiException {
        return get(user, uuid)
                .map(d -> new CsvDocRefEntity.Builder(d)
                        .updateTime(System.currentTimeMillis())
                        .updateUser(user.getName())
                        .name(name)
                        .build());
    }

    @Override
    public Optional<Boolean> deleteDocument(final ServiceUser user,
                                            final String uuid) throws QueryApiException {
        if (data.containsKey(uuid)) {
            data.remove(uuid);
            return Optional.of(Boolean.TRUE);
        } else {
            return Optional.of(Boolean.FALSE);
        }
    }

    @Override
    public ExportDTO exportDocument(final ServiceUser user,
                                    final String uuid) throws QueryApiException {
        return get(user, uuid)
                .map(d -> new ExportDTO.Builder()
                        .value(DocRefEntity.NAME, d.getName())
                        .value(CsvDocRefEntity.DATA_DIRECTORY, d.getDataDirectory())
                        .build())
                .orElse(new ExportDTO.Builder()
                        .message(String.format("Could not find test doc ref: %s", uuid))
                        .build());
    }

    @Override
    public Optional<CsvDocRefEntity> importDocument(final ServiceUser user,
                                                    final String uuid,
                                                    final String name,
                                                    final Boolean confirmed,
                                                    final Map<String, String> dataMap) throws QueryApiException {
        if (confirmed) {
            final Optional<CsvDocRefEntity> index = createDocument(user, uuid, name);

            if (index.isPresent()) {
                final CsvDocRefEntity indexConfig = index.get();
                indexConfig.setDataDirectory(dataMap.get(CsvDocRefEntity.DATA_DIRECTORY));
                return update(user, uuid, indexConfig);
            } else {
                return Optional.empty();
            }
        } else {
            return get(user, uuid)
                    .map(d -> Optional.<CsvDocRefEntity>empty())
                    .orElse(Optional.of(new CsvDocRefEntity.Builder()
                            .uuid(uuid)
                            .name(name)
                            .dataDirectory(dataMap.get(CsvDocRefEntity.DATA_DIRECTORY))
                            .build()));
        }
    }
}
