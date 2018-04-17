package stroom.autoindex.service;

import org.jooq.Field;
import org.jooq.types.ULong;
import stroom.query.api.v2.DocRef;
import stroom.query.audit.model.DocRefEntity;
import stroom.query.jooq.DocRefJooqEntity;
import stroom.query.jooq.JooqEntity;

import java.io.Serializable;
import java.util.Objects;

import static org.jooq.impl.DSL.field;

/**
 * This doc ref encapsulates an auto index configuration between two doc refs.
 *
 * The Raw datasource will contain all the available data, it will potentially be slow to query
 * The Index datasource will be gradually populated from the raw datasource, and used for
 * querying data within the populated tracker windows.
 */
@JooqEntity(tableName=AutoIndexDocRefEntity.TABLE_NAME)
public class AutoIndexDocRefEntity extends DocRefJooqEntity implements Serializable {
    public static final String TABLE_NAME = "auto_index_doc_ref";

    private static final String DOC_REF_TYPE = "type";
    public static final String TYPE = "AutoIndex";

    private static final String RAW_PREFIX = "raw_";
    private static final String INDEX_PREFIX = "index_";

    private static final String TIME_FIELD_NAME = "timeField";
    private static final String INDEXING_WINDOW = "indexWindow";

    public static final Field<String> RAW_DOC_REF_TYPE = field(RAW_PREFIX + DOC_REF_TYPE, String.class);
    public static final Field<String> RAW_DOC_REF_UUID = field(RAW_PREFIX + DocRefEntity.UUID, String.class);
    public static final Field<String> RAW_DOC_REF_NAME = field(RAW_PREFIX + DocRefEntity.NAME, String.class);

    public static final Field<String> INDEX_DOC_REF_TYPE = field(INDEX_PREFIX + DOC_REF_TYPE, String.class);
    public static final Field<String> INDEX_DOC_REF_UUID = field(INDEX_PREFIX + DocRefEntity.UUID, String.class);
    public static final Field<String> INDEX_DOC_REF_NAME = field(INDEX_PREFIX + DocRefEntity.NAME, String.class);

    public static final Field<String> TIME_FIELD_NAME_FIELD = field(TIME_FIELD_NAME, String.class);
    public static final Field<ULong> INDEXING_WINDOW_FIELD = field(INDEXING_WINDOW, ULong.class);

    /**
     * This is the Doc Ref of the slow data source
     */
    private DocRef rawDocRef;

    /**
     * This is the Doc Ref of the fast data source
     */
    private DocRef indexDocRef;

    /**
     * This is the name of the field in the data source that is used to bound the time
     */
    private String timeFieldName;

    /**
     * This is the amount of time to index at a time, it will govern the size/increments of the time windows.
     * Default is one day
     */
    private Long indexWindow = 1L;

    public DocRef getRawDocRef() {
        return rawDocRef;
    }

    public DocRef getIndexDocRef() {
        return indexDocRef;
    }

    public String getTimeFieldName() {
        return timeFieldName;
    }

    public Long getIndexWindow() {
        return indexWindow;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AutoIndexDocRefEntity{");
        sb.append("rawDocRef=").append(rawDocRef);
        sb.append(", indexDocRef=").append(indexDocRef);
        sb.append(", timeFieldName='").append(timeFieldName).append('\'');
        sb.append(", indexWindow=").append(indexWindow);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        AutoIndexDocRefEntity that = (AutoIndexDocRefEntity) o;
        return indexWindow.equals(that.indexWindow) &&
                Objects.equals(rawDocRef, that.rawDocRef) &&
                Objects.equals(indexDocRef, that.indexDocRef) &&
                Objects.equals(timeFieldName, that.timeFieldName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), rawDocRef, indexDocRef, timeFieldName, indexWindow);
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

        public Builder timeFieldName(final String value) {
            this.instance.timeFieldName = value;
            return this;
        }

        public Builder indexWindow(final Long value) {
            this.instance.indexWindow = value;
            return this;
        }

        public Builder self() {
            return this;
        }
    }
}
