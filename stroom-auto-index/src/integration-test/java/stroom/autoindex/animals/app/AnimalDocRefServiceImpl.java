package stroom.autoindex.animals.app;

import stroom.query.audit.ExportDTO;
import stroom.query.audit.model.DocRefEntity;
import stroom.query.audit.security.ServiceUser;
import stroom.query.audit.service.DocRefService;

import java.util.*;

public class AnimalDocRefServiceImpl implements DocRefService<AnimalDocRefEntity> {
    private static Map<String, AnimalDocRefEntity> data = new HashMap<>();

    public static void eraseAllData() {
        data.clear();
    }

    @Override
    public String getType() {
        return AnimalDocRefEntity.TYPE;
    }

    @Override
    public List<AnimalDocRefEntity> getAll(final ServiceUser user) throws Exception {
        return new ArrayList<>(data.values());
    }

    @Override
    public Optional<AnimalDocRefEntity> get(final ServiceUser user,
                                          final String uuid) throws Exception {
        return Optional.ofNullable(data.get(uuid));
    }

    @Override
    public Optional<AnimalDocRefEntity> createDocument(final ServiceUser user,
                                                     final String uuid,
                                                     final String name) throws Exception {
        final Long now = System.currentTimeMillis();
        data.put(uuid, new AnimalDocRefEntity.Builder()
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
    public Optional<AnimalDocRefEntity> update(final ServiceUser user,
                                             final String uuid,
                                             final AnimalDocRefEntity updatedConfig) throws Exception {
        return get(user, uuid)
                .map(d -> new AnimalDocRefEntity.Builder(d)
                        .updateTime(System.currentTimeMillis())
                        .updateUser(user.getName())
                        .species(updatedConfig.getSpecies())
                        .build());
    }

    @Override
    public Optional<AnimalDocRefEntity> copyDocument(final ServiceUser user,
                                                   final String originalUuid,
                                                   final String copyUuid) throws Exception {
        final AnimalDocRefEntity existing = data.get(originalUuid);
        if (null != existing) {
            createDocument(user, copyUuid, existing.getName());
            return update(user, copyUuid, existing);
        } else {
            return Optional.empty();
        }
    }

    @Override
    public Optional<AnimalDocRefEntity> moveDocument(final ServiceUser user,
                                                   final String uuid) throws Exception {
        return get(user, uuid);
    }

    @Override
    public Optional<AnimalDocRefEntity> renameDocument(final ServiceUser user,
                                                     final String uuid,
                                                     final String name) throws Exception {
        return get(user, uuid)
                .map(d -> new AnimalDocRefEntity.Builder(d)
                        .updateTime(System.currentTimeMillis())
                        .updateUser(user.getName())
                        .name(name)
                        .build());
    }

    @Override
    public Optional<Boolean> deleteDocument(final ServiceUser user,
                                            final String uuid) throws Exception {
        if (data.containsKey(uuid)) {
            data.remove(uuid);
            return Optional.of(Boolean.TRUE);
        } else {
            return Optional.of(Boolean.FALSE);
        }
    }

    @Override
    public ExportDTO exportDocument(final ServiceUser user,
                                    final String uuid) throws Exception {
        return get(user, uuid)
                .map(d -> new ExportDTO.Builder()
                        .value(DocRefEntity.NAME, d.getName())
                        .value(AnimalDocRefEntity.SPECIES, d.getSpecies())
                        .build())
                .orElse(new ExportDTO.Builder()
                        .message(String.format("Could not find test doc ref: %s", uuid))
                        .build());
    }

    @Override
    public Optional<AnimalDocRefEntity> importDocument(final ServiceUser user,
                                                     final String uuid,
                                                     final String name,
                                                     final Boolean confirmed,
                                                     final Map<String, String> dataMap) throws Exception {
        if (confirmed) {
            final Optional<AnimalDocRefEntity> index = createDocument(user, uuid, name);

            if (index.isPresent()) {
                final AnimalDocRefEntity indexConfig = index.get();
                indexConfig.setSpecies(dataMap.get(AnimalDocRefEntity.SPECIES));
                return update(user, uuid, indexConfig);
            } else {
                return Optional.empty();
            }
        } else {
            return get(user, uuid)
                    .map(d -> Optional.<AnimalDocRefEntity>empty())
                    .orElse(Optional.of(new AnimalDocRefEntity.Builder()
                            .uuid(uuid)
                            .name(name)
                            .species(dataMap.get(AnimalDocRefEntity.SPECIES))
                            .build()));
        }
    }
}
