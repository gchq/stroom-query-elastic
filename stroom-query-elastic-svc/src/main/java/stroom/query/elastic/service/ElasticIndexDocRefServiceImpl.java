package stroom.query.elastic.service;

import org.elasticsearch.client.transport.TransportClient;
import stroom.query.elastic.hibernate.ElasticIndexDocRefEntity;

import javax.inject.Inject;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

public class ElasticIndexDocRefServiceImpl
        extends AbstractElasticDocRefServiceImpl<ElasticIndexDocRefEntity, ElasticIndexDocRefEntity.Builder> {

    @Inject
    public ElasticIndexDocRefServiceImpl(final TransportClient client) {
        super(client);
    }

    @Override
    public String getType() {
        return "ElasticIndex";
    }

    @Override
    protected ElasticIndexDocRefEntity.Builder build(final Function<String, Object> source) {
        return new ElasticIndexDocRefEntity.Builder();
    }

    @Override
    protected void iterateFieldNames(final Consumer<String> consumer) {
    }

    @Override
    protected void exportValues(final ElasticIndexDocRefEntity instance,
                                final BiConsumer<String, String> consumer) {
    }

}
