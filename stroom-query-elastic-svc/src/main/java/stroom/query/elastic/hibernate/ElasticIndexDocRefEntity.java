package stroom.query.elastic.hibernate;

import stroom.query.audit.model.DocRefEntity;

import java.util.Optional;

public class ElasticIndexDocRefEntity extends DocRefEntity {

    public static final String TYPE = "ElasticIndex";
    public static final String INDEX_NAME = "INDEX_NAME";
    public static final String INDEXED_TYPE = "INDEXED_TYPE";

    private String indexName;

    private String indexedType;

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(final String value) {
        this.indexName = value;
    }

    public String getIndexedType() {
        return indexedType;
    }

    public void setIndexedType(final String value) {
        this.indexedType = value;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ElasticIndexConfig{");
        sb.append("super='").append(super.toString()).append('\'');
        sb.append(", indexName='").append(indexName).append('\'');
        sb.append(", indexedType='").append(indexedType).append('\'');
        sb.append('}');
        return sb.toString();
    }



    public static abstract class BaseBuilder<T extends ElasticIndexDocRefEntity, CHILD_CLASS extends BaseBuilder<T, ?>>
            extends DocRefEntity.BaseBuilder<T, CHILD_CLASS> {

        public BaseBuilder(final T instance) {
            super(instance);
        }

        public CHILD_CLASS indexName(final Object value) {
            this.instance.setIndexName(Optional.ofNullable(value).map(Object::toString).orElse(null));
            return self();
        }

        public CHILD_CLASS indexedType(final Object value) {
            this.instance.setIndexedType(Optional.ofNullable(value).map(Object::toString).orElse(null));
            return self();
        }
    }

    public static final class Builder extends BaseBuilder<ElasticIndexDocRefEntity, Builder> {
        public Builder(final ElasticIndexDocRefEntity instance) {
            super(instance);
        }

        public Builder() {
            this(new ElasticIndexDocRefEntity());
        }

        public Builder self() {
            return this;
        }
    }
}
