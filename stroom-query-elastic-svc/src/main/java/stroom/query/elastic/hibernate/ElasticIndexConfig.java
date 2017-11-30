package stroom.query.elastic.hibernate;

public class ElasticIndexConfig {
    public static final String STROOM_NAME = "STROOM_NAME";
    public static final String INDEX_NAME = "INDEX_NAME";
    public static final String INDEXED_TYPE = "INDEXED_TYPE";

    public static final String DEFAULT_STR = "";

    private String uuid;

    private String stroomName;

    private String indexName;

    private String indexedType;

    private String mappingsJson;

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getStroomName() {
        return stroomName;
    }

    public void setStroomName(String stroomName) {
        this.stroomName = stroomName;
    }

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
        sb.append("uuid='").append(uuid).append('\'');
        sb.append(", stroomName='").append(stroomName).append('\'');
        sb.append(", indexName='").append(indexName).append('\'');
        sb.append(", indexedType='").append(indexedType).append('\'');
        sb.append(", mappingsJson='").append(mappingsJson).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public static class Builder {
        private final ElasticIndexConfig instance;

        public Builder() {
            this.instance = new ElasticIndexConfig();
        }

        public Builder uuid(final Object value) {
            this.instance.uuid = (value != null) ? value.toString() : null;
            return this;
        }

        public Builder stroomName(final Object value) {
            this.instance.stroomName = (value != null) ? value.toString() : null;
            return this;
        }

        public Builder indexName(final Object value) {
            this.instance.indexName = (value != null) ? value.toString() : null;
            return this;
        }

        public Builder indexedType(final Object value) {
            this.instance.indexedType = (value != null) ? value.toString() : null;
            return this;
        }

        public Builder mappingsJson(final String value) {
            this.instance.mappingsJson = value;
            return this;
        }

        public ElasticIndexConfig build() {
            return instance;
        }
    }
}
