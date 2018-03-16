package stroom.autoindex;

import stroom.query.elastic.hibernate.ElasticIndexDocRefEntity;

import java.util.Objects;
import java.util.Optional;

/**
 * This doc ref encapsulates a wrapped query resource.
 */
public class AutoIndexDocRefEntity extends ElasticIndexDocRefEntity {
    public static final String TYPE = "AutoIndex";

    public static final String WRAPPED_DOC_REF_TYPE = "wrappedType";
    public static final String WRAPPED_DOC_REF_UUID = "wrappedUuid";
    public static final String WRAPPED_DATASOURCE_URL = "datasourceUrl";

    /**
     * This is the URL of the wrapped data source
     */
    private String wrappedDataSourceURL;

    /**
     * This is the Doc Ref of the wrapped type
     */
    private String wrappedDocRefType;

    /**
     * This is the UUID of the specific wrapped Doc Ref.
     */
    private String wrappedDocRefUuid;

    public String getWrappedDataSourceURL() {
        return wrappedDataSourceURL;
    }

    public String getWrappedDocRefType() {
        return wrappedDocRefType;
    }

    public String getWrappedDocRefUuid() {
        return wrappedDocRefUuid;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        AutoIndexDocRefEntity that = (AutoIndexDocRefEntity) o;
        return Objects.equals(wrappedDataSourceURL, that.wrappedDataSourceURL) &&
                Objects.equals(wrappedDocRefType, that.wrappedDocRefType) &&
                Objects.equals(wrappedDocRefUuid, that.wrappedDocRefUuid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), wrappedDataSourceURL, wrappedDocRefType, wrappedDocRefUuid);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AutoIndexDocRefEntity{");
        sb.append("super=").append(super.toString()).append(", ");
        sb.append("url='").append(wrappedDataSourceURL).append('\'');
        sb.append("docRefType='").append(wrappedDocRefType).append('\'');
        sb.append("docRefUuid='").append(wrappedDocRefUuid).append('\'');
        sb.append('}');
        return sb.toString();
    }



    public static final class Builder extends ElasticIndexDocRefEntity.BaseBuilder<AutoIndexDocRefEntity, Builder> {

        public Builder() {
            this(new AutoIndexDocRefEntity());
        }

        public Builder(final AutoIndexDocRefEntity instance) {
            super(instance);
        }

        public Builder wrappedDataSourceURL(final Object value) {
            this.instance.wrappedDataSourceURL = Optional.ofNullable(value).map(Object::toString).orElse(null);
            return self();
        }

        public Builder wrappedDocRefType(final Object value) {
            this.instance.wrappedDocRefType = Optional.ofNullable(value).map(Object::toString).orElse(null);
            return self();
        }

        public Builder wrappedDocRefUuid(final Object value) {
            this.instance.wrappedDocRefUuid = Optional.ofNullable(value).map(Object::toString).orElse(null);
            return self();
        }

        public Builder self() {
            return this;
        }
    }
}
