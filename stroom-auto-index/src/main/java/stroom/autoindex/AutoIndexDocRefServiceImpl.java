package stroom.autoindex;

import org.elasticsearch.client.transport.TransportClient;
import stroom.query.api.v2.DocRef;
import stroom.query.audit.model.DocRefEntity;
import stroom.query.elastic.service.AbstractElasticDocRefServiceImpl;

import javax.inject.Inject;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;


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
        // The builders require strings, so we need to convert potentially null objects to strings
        final Function<String, String> objToString = (s) -> Optional.ofNullable(source.apply(s))
                .map(Object::toString).orElse(null);

        // Generate doc refs for each of the prefix values
        final Map<String, DocRef> docRefs =
                Stream.of(AutoIndexDocRefEntity.RAW_PREFIX, AutoIndexDocRefEntity.INDEX_PREFIX).map(prefix -> {
            final String type = objToString.apply(prefix + AutoIndexDocRefEntity.DOC_REF_TYPE);
            final String uuid = objToString.apply(prefix + DocRefEntity.UUID);
            final String name = objToString.apply(prefix + DocRefEntity.NAME);

            final DocRef docRef = new DocRef.Builder()
                    .type(type)
                    .uuid(uuid)
                    .name(name)
                    .build();

            return new AbstractMap.SimpleEntry<>(prefix, docRef);
        }).collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));

        // Now create the auto index doc ref with the wrapped doc refs
        return new AutoIndexDocRefEntity.Builder()
                .rawDocRef(docRefs.get(AutoIndexDocRefEntity.RAW_PREFIX))
                .indexDocRef(docRefs.get(AutoIndexDocRefEntity.INDEX_PREFIX));
    }

    @Override
    protected void iterateFieldNames(final Consumer<String> consumer) {
        Stream.of(AutoIndexDocRefEntity.RAW_PREFIX, AutoIndexDocRefEntity.INDEX_PREFIX)
                .map(prefix -> Stream.of(DocRefEntity.NAME, DocRefEntity.UUID, AutoIndexDocRefEntity.DOC_REF_TYPE)
                        .map(prop -> prefix + prop))
                .flatMap(l -> l)
                .forEach(consumer);
    }

    @Override
    protected void exportValues(final AutoIndexDocRefEntity instance,
                                final BiConsumer<String, String> consumer) {
        Stream.of(
                new AbstractMap.SimpleEntry<>(AutoIndexDocRefEntity.RAW_PREFIX, instance.getRawDocRef()),
                new AbstractMap.SimpleEntry<>(AutoIndexDocRefEntity.INDEX_PREFIX, instance.getIndexDocRef())
        )
                .forEach(entry -> {
                    consumer.accept(entry.getKey() + DocRefEntity.UUID, entry.getValue().getUuid());
                    consumer.accept(entry.getKey() + DocRefEntity.NAME, entry.getValue().getName());
                    consumer.accept(entry.getKey() + AutoIndexDocRefEntity.DOC_REF_TYPE, entry.getValue().getType());
                });
    }
}
