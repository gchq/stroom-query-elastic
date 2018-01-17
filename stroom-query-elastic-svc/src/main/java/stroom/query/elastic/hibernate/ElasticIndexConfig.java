package stroom.query.elastic.hibernate;

import stroom.query.audit.service.DocRefEntity;

public class ElasticIndexConfig extends DocRefEntity {
    public static final String INDEX_NAME = "INDEX_NAME";
    public static final String INDEXED_TYPE = "INDEXED_TYPE";

    public static final String DEFAULT_STR = "";

    private String indexName;

    private String indexedType;

    private String mappingsJson;

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

    /**
     * This will operate as a read only field, if the user wants to modify the index itself, it
     * is best done through Kibana
     * @return The current state of the mappings in JSON
     */
    public String getMappingsJson() {
        return mappingsJson;
    }

    public void setMappingsJson(String mappingsJson) {
        this.mappingsJson = mappingsJson;
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ElasticIndexConfig{");
        sb.append("super='").append(super.toString()).append('\'');
        sb.append(", indexName='").append(indexName).append('\'');
        sb.append(", indexedType='").append(indexedType).append('\'');
        sb.append(", mappingsJson='").append(mappingsJson).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public static class Builder extends DocRefEntity.Builder<ElasticIndexConfig, Builder> {

        public Builder() {
            super(new ElasticIndexConfig());
        }

        public Builder indexName(final Object value) {
            this.instance.indexName = (value != null) ? value.toString() : null;
            return self();
        }

        public Builder indexedType(final Object value) {
            this.instance.indexedType = (value != null) ? value.toString() : null;
            return self();
        }

        public Builder mappingsJson(final String value) {
            this.instance.mappingsJson = value;
            return self();
        }

        public ElasticIndexConfig build() {
            return instance;
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
