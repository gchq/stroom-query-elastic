package stroom.autoindex;

import org.elasticsearch.client.transport.TransportClient;
import stroom.query.audit.model.DocRefEntity;
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
                .wrappedDataSourceURL(source.apply(AutoIndexDocRefEntity.WRAPPED_DATASOURCE_URL));
    }

    @Override
    protected void iterateFieldNames(Consumer<String> consumer) {

    }

    @Override
    protected void exportValues(AutoIndexDocRefEntity instance, BiConsumer<String, String> consumer) {

    }
}
