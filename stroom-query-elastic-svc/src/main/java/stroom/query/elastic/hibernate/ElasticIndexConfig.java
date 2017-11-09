package stroom.query.elastic.hibernate;

public class ElasticIndexConfig {
    public static final String UUID = "UUID";
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
        sb.append("indexName='").append(indexName).append('\'');
        sb.append(", indexedType='").append(indexedType).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public static class Builder {
        private final ElasticIndexConfig instance;

        public Builder() {
            this.instance = new ElasticIndexConfig();
        }

        public Builder indexName(final Object value) {
            this.instance.indexName = (value != null) ? value.toString() : null;;
            return this;
        }

        public Builder indexedType(final Object value) {
            this.instance.indexedType = (value != null) ? value.toString() : null;;
            return this;
        }

        public ElasticIndexConfig build() {
            return instance;
        }
    }
}
