package stroom.autoindex.animals.app;

import stroom.datasource.api.v2.DataSource;
import stroom.datasource.api.v2.DataSourceField;
import stroom.query.api.v2.*;
import stroom.query.audit.security.ServiceUser;
import stroom.query.audit.service.DocRefService;
import stroom.query.audit.service.QueryService;

import javax.inject.Inject;
import java.util.Optional;

public class AnimalQueryServiceImpl implements QueryService {
    private final DocRefService<AnimalDocRefEntity> docRefService;

    @Inject
    public AnimalQueryServiceImpl(final DocRefService<AnimalDocRefEntity> docRefService) {
        this.docRefService = docRefService;
    }

    @Override
    public Optional<DataSource> getDataSource(final ServiceUser user,
                                              final DocRef docRef) throws Exception {
        final Optional<AnimalDocRefEntity> docRefEntity = docRefService.get(user, docRef.getUuid());

        if (!docRefEntity.isPresent()) {
            return Optional.empty();
        }

        return Optional.of(new DataSource.Builder()
                .addFields(new DataSourceField.Builder()
                        .type(DataSourceField.DataSourceFieldType.FIELD)
                        .name(AnimalDocRefEntity.SPECIES)
                        .queryable(true)
                        .addConditions(
                                ExpressionTerm.Condition.EQUALS,
                                ExpressionTerm.Condition.IN,
                                ExpressionTerm.Condition.IN_DICTIONARY)
                        .build())
                .build());
    }

    @Override
    public Optional<SearchResponse> search(final ServiceUser user,
                                           final SearchRequest request) throws Exception {
        final String dataSourceUuid = request.getQuery().getDataSource().getUuid();

        final Optional<AnimalDocRefEntity> docRefEntity = docRefService.get(user, dataSourceUuid);

        if (!docRefEntity.isPresent()) {
            return Optional.empty();
        }

        return Optional.of(new SearchResponse.TableResultBuilder()
                // Yeah whatever...
                .build());
    }

    @Override
    public Boolean destroy(final ServiceUser user,
                           final QueryKey queryKey) throws Exception {
        return Boolean.TRUE;
    }

    @Override
    public Optional<DocRef> getDocRefForQueryKey(final ServiceUser user,
                                                 final QueryKey queryKey) throws Exception {
        return Optional.empty();
    }
}
