package stroom.autoindex;

import org.jooq.Field;
import stroom.query.api.v2.DocRef;
import stroom.query.audit.model.DocRefEntity;
import stroom.query.jooq.DocRefJooqEntity;
import stroom.query.jooq.JooqEntity;

import static org.jooq.impl.DSL.field;

/**
 * This doc ref encapsulates an auto index configuration between two doc refs.
 *
 * The Raw datasource will contain all the available data, it will potentially be slow to query
 * The Index datasource will be gradually populated from the raw datasource, and used for
 * querying data within the populated tracker windows.
 */
@JooqEntity(tableName=AutoIndexDocRefEntity.TABLE_NAME)
public class AutoIndexDocRefEntity extends DocRefJooqEntity {
    public static final String TABLE_NAME = "auto_index_doc_ref";

    private static final String DOC_REF_TYPE = "type";
    public static final String TYPE = "AutoIndex";

    private static final String RAW_PREFIX = "raw_";
    private static final String INDEX_PREFIX = "index_";

    public static final Field<String> RAW_DOC_REF_TYPE = field(RAW_PREFIX + DOC_REF_TYPE, String.class);
    public static final Field<String> RAW_DOC_REF_UUID = field(RAW_PREFIX + DocRefEntity.UUID, String.class);
    public static final Field<String> RAW_DOC_REF_NAME = field(RAW_PREFIX + DocRefEntity.NAME, String.class);

    public static final Field<String> INDEX_DOC_REF_TYPE = field(INDEX_PREFIX + DOC_REF_TYPE, String.class);
    public static final Field<String> INDEX_DOC_REF_UUID = field(INDEX_PREFIX + DocRefEntity.UUID, String.class);
    public static final Field<String> INDEX_DOC_REF_NAME = field(INDEX_PREFIX + DocRefEntity.NAME, String.class);

    /**
     * This is the Doc Ref of the slow data source
     */
    private DocRef rawDocRef;

    /**
     * This is the Doc Ref of the fast data source
     */
    private DocRef indexDocRef;

    public DocRef getRawDocRef() {
        return rawDocRef;
    }

    public DocRef getIndexDocRef() {
        return indexDocRef;
    }

    public static final class Builder extends BaseBuilder<AutoIndexDocRefEntity, Builder> {

        public Builder() {
            this(new AutoIndexDocRefEntity());
        }

        public Builder(final AutoIndexDocRefEntity instance) {
            super(instance);
        }

        public Builder rawDocRef(final DocRef docRef) {
            this.instance.rawDocRef = docRef;
            return self();
        }

        public Builder indexDocRef(final DocRef docRef) {
            this.instance.indexDocRef = docRef;
            return self();
        }

        public Builder self() {
            return this;
        }
    }
}
