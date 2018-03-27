package stroom.autoindex.service;

import org.jooq.Field;
import org.jooq.types.UInteger;
import org.jooq.types.ULong;
import stroom.query.api.v2.DocRef;
import stroom.query.audit.model.DocRefEntity;
import stroom.query.jooq.DocRefJooqEntity;
import stroom.query.jooq.JooqEntity;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    private static final String TIME_FIELD_NAME = "timeField";
    private static final String INDEXING_WINDOW_AMOUNT = "indexWindowAmount";
    private static final String INDEXING_WINDOW_UNIT = "indexWindowUnit";

    public static final Field<String> RAW_DOC_REF_TYPE = field(RAW_PREFIX + DOC_REF_TYPE, String.class);
    public static final Field<String> RAW_DOC_REF_UUID = field(RAW_PREFIX + DocRefEntity.UUID, String.class);
    public static final Field<String> RAW_DOC_REF_NAME = field(RAW_PREFIX + DocRefEntity.NAME, String.class);

    public static final Field<String> INDEX_DOC_REF_TYPE = field(INDEX_PREFIX + DOC_REF_TYPE, String.class);
    public static final Field<String> INDEX_DOC_REF_UUID = field(INDEX_PREFIX + DocRefEntity.UUID, String.class);
    public static final Field<String> INDEX_DOC_REF_NAME = field(INDEX_PREFIX + DocRefEntity.NAME, String.class);

    public static final Field<String> TIME_FIELD_NAME_FIELD = field(TIME_FIELD_NAME, String.class);
    public static final Field<Integer> INDEXING_WINDOW_AMOUNT_FIELD = field(INDEXING_WINDOW_AMOUNT, Integer.class);
    public static final Field<String> INDEXING_WINDOW_UNIT_FIELD = field(INDEXING_WINDOW_UNIT, String.class);

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
    private int indexWindowAmount = 1;

    /**
     * This is the units of the amount of time to index.
     */
    private ChronoUnit indexWindowUnit = ChronoUnit.DAYS;

    public DocRef getRawDocRef() {
        return rawDocRef;
    }

    public DocRef getIndexDocRef() {
        return indexDocRef;
    }

    public String getTimeFieldName() {
        return timeFieldName;
    }

    public int getIndexWindowAmount() {
        return indexWindowAmount;
    }

    public ChronoUnit getIndexWindowUnit() {
        return indexWindowUnit;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AutoIndexDocRefEntity{");
        sb.append("rawDocRef=").append(rawDocRef);
        sb.append(", indexDocRef=").append(indexDocRef);
        sb.append(", timeFieldName='").append(timeFieldName).append('\'');
        sb.append(", indexWindowAmount=").append(indexWindowAmount);
        sb.append(", indexWindowUnit=").append(indexWindowUnit);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        AutoIndexDocRefEntity that = (AutoIndexDocRefEntity) o;
        return indexWindowAmount == that.indexWindowAmount &&
                Objects.equals(rawDocRef, that.rawDocRef) &&
                Objects.equals(indexDocRef, that.indexDocRef) &&
                Objects.equals(timeFieldName, that.timeFieldName) &&
                indexWindowUnit == that.indexWindowUnit;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), rawDocRef, indexDocRef, timeFieldName, indexWindowAmount, indexWindowUnit);
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

        public Builder indexWindowAmount(final int value) {
            this.instance.indexWindowAmount = value;
            return this;
        }

        final static Set<ChronoUnit> allowedValues = Stream.of(
                ChronoUnit.YEARS,
                ChronoUnit.MONTHS,
                ChronoUnit.DAYS,
                ChronoUnit.HOURS,
                ChronoUnit.MINUTES
        ).collect(Collectors.toSet());

        public Builder indexWindowUnits(final ChronoUnit value) {
            if (!allowedValues.contains(value)) {
                throw new IllegalArgumentException(
                        String.format("Index Window Unit of %s not allowed, must be one of %s",
                                value, allowedValues));
            }

            this.instance.indexWindowUnit = value;
            return this;
        }

        public Builder self() {
            return this;
        }
    }
}
