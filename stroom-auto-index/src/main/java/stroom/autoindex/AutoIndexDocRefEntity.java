package stroom.autoindex;

import stroom.query.elastic.hibernate.ElasticIndexDocRefEntity;

import java.util.Objects;
import java.util.Optional;

/**
 * This doc ref encapsulates a wrapped query resource.
 */
public class AutoIndexDocRefEntity extends ElasticIndexDocRefEntity {
    public static final String TYPE = "AutoIndex";

    public static final String WRAPPED_DATASOURCE_URL = "dsUrl";

    /**
     * This is the URL of the wrapped data source
     */
    private String wrappedDataSourceURL;

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AutoIndexDocRefEntity{");
        sb.append("super=").append(super.toString()).append(", ");
        sb.append("wrappedDataSourceURL='").append(wrappedDataSourceURL).append('\'');
        sb.append('}');
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        AutoIndexDocRefEntity that = (AutoIndexDocRefEntity) o;
        return Objects.equals(wrappedDataSourceURL, that.wrappedDataSourceURL);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), wrappedDataSourceURL);
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

        public Builder self() {
            return this;
        }
    }
}
