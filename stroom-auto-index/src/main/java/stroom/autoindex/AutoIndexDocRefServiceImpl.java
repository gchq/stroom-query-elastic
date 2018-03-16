package stroom.autoindex;

import org.elasticsearch.client.transport.TransportClient;
import stroom.query.elastic.service.AbstractElasticDocRefServiceImpl;

import javax.inject.Inject;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;


public class AutoIndexDocRefServiceImpl
        extends AbstractElasticDocRefServiceImpl<AutoIndexDocRefEntity, AutoIndexDocRefEntity.Builder> {

    @Inject
    public AutoIndexDocRefServiceImpl(final TransportClient client) {
        super(client);
    }

    @Override
    public String getType() {
        return AutoIndexDocRefEntity.TYPE;
    }

    @Override
    protected AutoIndexDocRefEntity.Builder build(final Function<String, Object> source) {
        return new AutoIndexDocRefEntity.Builder()
                .wrappedDataSourceURL(source.apply(AutoIndexDocRefEntity.WRAPPED_DATASOURCE_URL))
                .wrappedDocRefType(source.apply(AutoIndexDocRefEntity.WRAPPED_DOC_REF_TYPE))
                .wrappedDocRefUuid(source.apply(AutoIndexDocRefEntity.WRAPPED_DOC_REF_UUID));
    }

    @Override
    protected void iterateFieldNames(final Consumer<String> consumer) {
        consumer.accept(AutoIndexDocRefEntity.WRAPPED_DATASOURCE_URL);
        consumer.accept(AutoIndexDocRefEntity.WRAPPED_DOC_REF_TYPE);
        consumer.accept(AutoIndexDocRefEntity.WRAPPED_DOC_REF_UUID);
    }

    @Override
    protected void exportValues(final AutoIndexDocRefEntity instance,
                                final BiConsumer<String, String> consumer) {
        consumer.accept(AutoIndexDocRefEntity.WRAPPED_DATASOURCE_URL, instance.getWrappedDataSourceURL());
        consumer.accept(AutoIndexDocRefEntity.WRAPPED_DOC_REF_TYPE, instance.getWrappedDocRefType());
        consumer.accept(AutoIndexDocRefEntity.WRAPPED_DOC_REF_UUID, instance.getWrappedDocRefUuid());
    }
}
