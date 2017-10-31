package stroom.query.elastic.hibernate;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "ELASTIC_INDEX")
public class ElasticIndexConfig {
    public static final String UUID = "UUID";
    public static final String INDEX_NAME = "INDEX_NAME";
    public static final String INDEXED_TYPE = "INDEXED_TYPE";

    private String uuid;

    private String indexName;

    private String indexedType;

    @Id
    @Column(name = UUID)
    public String getUUID() {
        return uuid;
    }

    public void setUUID(final String value) {
        this.uuid = value;
    }

    @Column(name = INDEX_NAME)
    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(final String value) {
        this.indexName = value;
    }

    @Column(name = INDEXED_TYPE)
    public String getIndexedType() {
        return indexedType;
    }

    public void setIndexedType(final String value) {
        this.indexedType = value;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ElasticIndexConfig{");
        sb.append("uuid='").append(uuid).append('\'');
        sb.append(", indexName='").append(indexName).append('\'');
        sb.append(", indexedType='").append(indexedType).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
