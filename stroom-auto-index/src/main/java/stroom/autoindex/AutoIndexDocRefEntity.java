package stroom.autoindex;

import stroom.query.api.v2.DocRef;
import stroom.query.elastic.hibernate.ElasticIndexDocRefEntity;

/**
 * This doc ref encapsulates an auto index configuration between two doc refs.
 *
 * The Raw datasource will contain all the available data, it will potentially be slow to query
 * The Index datasource will be gradually populated from the raw datasource, and used for
 * querying data within the populated tracker windows.
 */
public class AutoIndexDocRefEntity extends ElasticIndexDocRefEntity {
    public static final String DOC_REF_TYPE = "type";
    public static final String TYPE = "AutoIndex";

    public static final String RAW_PREFIX = "raw-";
    public static final String INDEX_PREFIX = "index-";

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

    public static final class Builder extends ElasticIndexDocRefEntity.BaseBuilder<AutoIndexDocRefEntity, Builder> {

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
