package stroom.autoindex;

import stroom.autoindex.app.Config;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class QueryClientCache<T> implements Function<String, Optional<T>> {

    private final Function<String, T> supplier;
    private final Config configuration;
    private final ConcurrentHashMap<String, T> cache =
            new ConcurrentHashMap<>();

    public QueryClientCache(final Config configuration,
                                final Function<String, T> supplier) {
        this.configuration = configuration;
        this.supplier = supplier;
    }

    @Override
    public Optional<T> apply(final String type) {
        return Optional.ofNullable(configuration.getQueryResourceUrlsByType())
                .map(m -> m.get(type))
                .map(url -> cache.computeIfAbsent(url, supplier));
    }
}
