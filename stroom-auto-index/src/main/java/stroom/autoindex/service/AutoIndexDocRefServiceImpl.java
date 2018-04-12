package stroom.autoindex.service;

import org.jooq.DSLContext;
import org.jooq.types.ULong;
import stroom.query.api.v2.DocRef;
import stroom.query.jooq.DocRefServiceJooqImpl;

import javax.inject.Inject;

public class AutoIndexDocRefServiceImpl
        extends DocRefServiceJooqImpl<AutoIndexDocRefEntity> {

    @Inject
    public AutoIndexDocRefServiceImpl(final DSLContext database) {
        super(AutoIndexDocRefEntity.TYPE,
                dataMap -> new AutoIndexDocRefEntity.Builder()
                        .timeFieldName(dataMap.getValue(AutoIndexDocRefEntity.TIME_FIELD_NAME_FIELD).orElse(null))
                        .indexWindow(dataMap.getValue(AutoIndexDocRefEntity.INDEXING_WINDOW_FIELD)
                                .orElse(ULong.valueOf(1L)).longValue())
                        .rawDocRef(new DocRef.Builder()
                                .type(dataMap.getValue(AutoIndexDocRefEntity.RAW_DOC_REF_TYPE).orElse(null))
                                .uuid(dataMap.getValue(AutoIndexDocRefEntity.RAW_DOC_REF_UUID).orElse(null))
                                .name(dataMap.getValue(AutoIndexDocRefEntity.RAW_DOC_REF_NAME).orElse(null))
                                .build())
                        .indexDocRef(new DocRef.Builder()
                                .type(dataMap.getValue(AutoIndexDocRefEntity.INDEX_DOC_REF_TYPE).orElse(null))
                                .uuid(dataMap.getValue(AutoIndexDocRefEntity.INDEX_DOC_REF_UUID).orElse(null))
                                .name(dataMap.getValue(AutoIndexDocRefEntity.INDEX_DOC_REF_NAME).orElse(null))
                                .build()),
                (entity, consumer) -> {
                    consumer.setValue(AutoIndexDocRefEntity.INDEXING_WINDOW_FIELD, ULong.valueOf(entity.getIndexWindow()));
                    consumer.setValue(AutoIndexDocRefEntity.TIME_FIELD_NAME_FIELD, entity.getTimeFieldName());

                    consumer.setValue(AutoIndexDocRefEntity.RAW_DOC_REF_TYPE, entity.getRawDocRef().getType());
                    consumer.setValue(AutoIndexDocRefEntity.RAW_DOC_REF_UUID, entity.getRawDocRef().getUuid());
                    consumer.setValue(AutoIndexDocRefEntity.RAW_DOC_REF_NAME, entity.getRawDocRef().getName());

                    consumer.setValue(AutoIndexDocRefEntity.INDEX_DOC_REF_TYPE, entity.getIndexDocRef().getType());
                    consumer.setValue(AutoIndexDocRefEntity.INDEX_DOC_REF_UUID, entity.getIndexDocRef().getUuid());
                    consumer.setValue(AutoIndexDocRefEntity.INDEX_DOC_REF_NAME, entity.getIndexDocRef().getName());
                },
                AutoIndexDocRefEntity.class,
                database);
    }
}
