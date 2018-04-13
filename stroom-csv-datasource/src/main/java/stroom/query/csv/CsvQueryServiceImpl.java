package stroom.query.csv;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.dashboard.expression.v1.FieldIndexMap;
import stroom.datasource.api.v2.DataSource;
import stroom.datasource.api.v2.DataSourceField;
import stroom.query.api.v2.*;
import stroom.query.audit.CriteriaStore;
import stroom.query.audit.security.ServiceUser;
import stroom.query.audit.service.DocRefService;
import stroom.query.audit.service.QueryApiException;
import stroom.query.audit.service.QueryService;
import stroom.query.common.v2.*;

import javax.inject.Inject;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CsvQueryServiceImpl implements QueryService {
    private static final Logger LOGGER = LoggerFactory.getLogger(CsvQueryServiceImpl.class);

    private final DocRefService<CsvDocRefEntity> docRefService;
    private final CsvFieldSupplier fieldSupplier;

    @Inject
    @SuppressWarnings("unchecked")
    public CsvQueryServiceImpl(final DocRefService docRefService,
                               final CsvFieldSupplier fieldSupplier) {
        this.docRefService = (DocRefService<CsvDocRefEntity>) docRefService;
        this.fieldSupplier = fieldSupplier;
    }

    @Override
    public Optional<DataSource> getDataSource(final ServiceUser user,
                                              final DocRef docRef) throws QueryApiException {
        final Optional<CsvDocRefEntity> docRefEntity = docRefService.get(user, docRef.getUuid());

        if (!docRefEntity.isPresent()) {
            return Optional.empty();
        }

        final DataSource.Builder builder = new DataSource.Builder();

        fieldSupplier.getFields().map(csvField -> {
            DataSourceField field = null;

            switch (csvField.getType()) {
                case DATE_FIELD:
                    field = new DataSourceField.Builder()
                            .type(DataSourceField.DataSourceFieldType.DATE_FIELD)
                            .name(csvField.getName())
                            .queryable(true)
                            .addConditions(
                                    ExpressionTerm.Condition.BETWEEN,
                                    ExpressionTerm.Condition.LESS_THAN,
                                    ExpressionTerm.Condition.LESS_THAN_OR_EQUAL_TO,
                                    ExpressionTerm.Condition.GREATER_THAN,
                                    ExpressionTerm.Condition.GREATER_THAN_OR_EQUAL_TO)
                            .build();
                    break;
                case NUMERIC_FIELD:
                    field = new DataSourceField.Builder()
                            .type(DataSourceField.DataSourceFieldType.NUMERIC_FIELD)
                            .name(csvField.getName())
                            .queryable(true)
                            .addConditions(
                                    ExpressionTerm.Condition.EQUALS,
                                    ExpressionTerm.Condition.BETWEEN,
                                    ExpressionTerm.Condition.LESS_THAN_OR_EQUAL_TO,
                                    ExpressionTerm.Condition.LESS_THAN,
                                    ExpressionTerm.Condition.GREATER_THAN_OR_EQUAL_TO,
                                    ExpressionTerm.Condition.GREATER_THAN)
                            .build();
                    break;
                case FIELD:
                    field = new DataSourceField.Builder()
                            .type(DataSourceField.DataSourceFieldType.FIELD)
                            .name(csvField.getName())
                            .queryable(true)
                            .addConditions(
                                    ExpressionTerm.Condition.EQUALS,
                                    ExpressionTerm.Condition.IN)
                            .build();
                    break;
                case ID:
                    field = new DataSourceField.Builder()
                            .type(DataSourceField.DataSourceFieldType.ID)
                            .name(csvField.getName())
                            .queryable(true)
                            .addConditions(
                                    ExpressionTerm.Condition.EQUALS,
                                    ExpressionTerm.Condition.IN)
                            .build();
                    break;
            }

            return field;
        }).forEach(builder::addFields);

        return Optional.of(builder.build());
    }

    @Override
    public Optional<SearchResponse> search(final ServiceUser user,
                                           final SearchRequest request) throws QueryApiException {
        final String dataSourceUuid = request.getQuery().getDataSource().getUuid();

        final Optional<CsvDocRefEntity> docRefEntity = docRefService.get(user, dataSourceUuid);

        if (!docRefEntity.isPresent()) {
            return Optional.empty();
        }

        LOGGER.info("Searching for Animal Sightings in Doc Ref {}", docRefEntity);

        final List<Map<String, Object>> results;
        try {
            results = Files.walk(Paths.get(docRefEntity.get().getDataDirectory()))
                    .filter(p -> !Files.isDirectory(p))
                    .map(file -> findInFile(file,
                            request.getQuery().getExpression())
                    )
                    .flatMap(l -> l)
                    .map(CsvDataRow::getData)
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new QueryApiException("Couldn't walk the files in the data directory", e);
        }

        final SearchResponse searchResponse = projectResults(request, results);

        return Optional.of(searchResponse);
    }

    private Stream<CsvDataRow> findInFile(final Path file,
                                          final ExpressionOperator expressionOperator) {
        final List<CsvDataRow> animalSightings = new ArrayList<>();

        LOGGER.debug("File Found {}", file);

        try (final Stream<String> stream = Files.lines(file)) {

            stream.map(l -> l.split(","))
                    .filter(p -> p.length == fieldSupplier.count())
                    .skip(1) // header row
                    .map(p -> {
                        final CsvDataRow row = new CsvDataRow();
                        fieldSupplier.getFields()
                                .forEach(f -> row.withField(f, p[f.getPosition()]));
                        return row;
                    })
                    .filter(as -> filterCsv(as.getData(), expressionOperator))
                    .forEach(animalSightings::add);

        } catch (IOException e) {
            LOGGER.error("Could not read file", e);
        }

        return animalSightings.stream();
    }

    private boolean filterCsv(final Map<String, Object> data,
                              final ExpressionItem item) {
        if (!item.enabled()) {
            return true;
        }

        if (item instanceof ExpressionTerm) {
            final ExpressionTerm term = (ExpressionTerm) item;
            final Object dataValue = data.get(term.getField());

            switch (term.getCondition()) {
                case EQUALS: {
                    if (dataValue instanceof String) {
                        return dataValue.equals(term.getValue());
                    } else if (dataValue instanceof Long) {
                        return dataValue.equals(Long.valueOf(term.getValue()));
                    }
                }
                case CONTAINS: {
                    if (dataValue instanceof String) {
                        return ((String) dataValue).contains(term.getValue());
                    }
                }
                case BETWEEN: {
                    if (dataValue instanceof LocalDateTime) {
                        final LocalDateTime localDate = (LocalDateTime) dataValue;
                        final String[] parts = term.getValue().split(",");
                        if (parts.length == 2) {
                            final LocalDateTime from = LocalDateTime.parse(parts[0], DateTimeFormatter.ISO_LOCAL_DATE_TIME);
                            final LocalDateTime to = LocalDateTime.parse(parts[1], DateTimeFormatter.ISO_LOCAL_DATE_TIME);

                            return localDate.isAfter(from) && localDate.isBefore(to);
                        }
                    } else if (dataValue instanceof Long) {
                        final Long localLong = (Long) dataValue;
                        final String[] parts = term.getValue().split(",");
                        if (parts.length == 2) {
                            final Long from = Long.valueOf(parts[0]);
                            final Long to = Long.valueOf(parts[1]);

                            return (localLong >= from) && (localLong < to);
                        }
                    }

                    break;
                }
                case GREATER_THAN:{
                    if (dataValue instanceof LocalDateTime) {
                        final LocalDateTime localDate = (LocalDateTime) dataValue;
                        final LocalDateTime from = LocalDateTime.parse(term.getValue(), DateTimeFormatter.ISO_LOCAL_DATE_TIME);
                        return localDate.isAfter(from);
                    } else if (dataValue instanceof Long) {
                        return (Long) dataValue > Long.valueOf(term.getValue());
                    }
                }
                case GREATER_THAN_OR_EQUAL_TO: {
                    if (dataValue instanceof LocalDateTime) {
                        final LocalDateTime localDate = (LocalDateTime) dataValue;
                        final LocalDateTime from = LocalDateTime.parse(term.getValue(), DateTimeFormatter.ISO_LOCAL_DATE_TIME);
                        return localDate.isAfter(from) || localDate.equals(from);
                    } else if (dataValue instanceof Long) {
                        return (Long) dataValue >= Long.valueOf(term.getValue());
                    }
                }
                case LESS_THAN: {
                    if (dataValue instanceof LocalDateTime) {
                        final LocalDateTime localDate = (LocalDateTime) dataValue;
                        final LocalDateTime to = LocalDateTime.parse(term.getValue(), DateTimeFormatter.ISO_LOCAL_DATE_TIME);
                        return localDate.isBefore(to);
                    } else if (dataValue instanceof Long) {
                        return (Long) dataValue < Long.valueOf(term.getValue());
                    }
                }
                case LESS_THAN_OR_EQUAL_TO: {
                    if (dataValue instanceof LocalDateTime) {
                        final LocalDateTime localDate = (LocalDateTime) dataValue;
                        final LocalDateTime to = LocalDateTime.parse(term.getValue(), DateTimeFormatter.ISO_LOCAL_DATE_TIME);
                        return localDate.isBefore(to) || localDate.equals(to);
                    } else if (dataValue instanceof Long) {
                        return (Long) dataValue <= Long.valueOf(term.getValue());
                    }
                }
                case IN: {
                    if (dataValue instanceof String) {
                        return Stream.of(term.getValue().split(","))
                                .filter(dataValue::equals).count() > 0L;
                    }
                }
            }

        } else if (item instanceof ExpressionOperator) {
            final ExpressionOperator operator = (ExpressionOperator) item;

            switch (operator.getOp()) {
                case AND:
                    return operator.getChildren().stream()
                            .map(o -> filterCsv(data, o))
                            .reduce(true, (a, b) -> a && b);
                case OR:
                return operator.getChildren().stream()
                        .map(o -> filterCsv(data, o))
                        .reduce(false, (a, b) -> a || b);
                case NOT:
                    return !operator.getChildren().stream()
                            .map(o -> filterCsv(data, o))
                            .reduce(true, (a, b) -> a && b);
                default:
                    // Fall through to null if there aren't any children
                    break;
            }
        }

        return false;
    }

    @Override
    public Boolean destroy(final ServiceUser user,
                           final QueryKey queryKey) {
        return Boolean.TRUE;
    }

    @Override
    public Optional<DocRef> getDocRefForQueryKey(final ServiceUser user,
                                                 final QueryKey queryKey) {
        return Optional.empty();
    }



    // TODO I copied this from 'stats', but can't make head or tail of it to try and move it into somewhere more sensible
    private SearchResponse projectResults(final SearchRequest searchRequest,
                                          final List<Map<String, Object>> results) {

        // TODO: possibly the mapping from the componentId to the coprocessorsettings map is a bit odd.
        final CoprocessorSettingsMap coprocessorSettingsMap = CoprocessorSettingsMap.create(searchRequest);

        final Map<CoprocessorSettingsMap.CoprocessorKey, Coprocessor> coprocessorMap = new HashMap<>();

        // TODO: Mapping to this is complicated! it'd be nice not to have to do this.
        final FieldIndexMap fieldIndexMap = new FieldIndexMap(true);

        // Compile all of the result component options to optimise pattern matching etc.
        if (coprocessorSettingsMap.getMap() != null) {
            for (final Map.Entry<CoprocessorSettingsMap.CoprocessorKey, CoprocessorSettings> entry : coprocessorSettingsMap.getMap().entrySet()) {
                final CoprocessorSettingsMap.CoprocessorKey coprocessorId = entry.getKey();
                final CoprocessorSettings coprocessorSettings = entry.getValue();

                // Create a parameter map.
                final Map<String, String> paramMap;
                if (searchRequest.getQuery().getParams() != null) {
                    paramMap = searchRequest.getQuery().getParams().stream()
                            .collect(Collectors.toMap(Param::getKey, Param::getValue));
                } else {
                    paramMap = Collections.emptyMap();
                }

                if (coprocessorSettings instanceof TableCoprocessorSettings) {
                    final TableCoprocessorSettings tableCoprocessorSettings = (TableCoprocessorSettings) coprocessorSettings;
                    final Coprocessor coprocessor = new TableCoprocessor(tableCoprocessorSettings,
                            fieldIndexMap,
                            paramMap);

                    coprocessorMap.put(coprocessorId, coprocessor);
                }
            }
        }

        //TODO TableCoprocessor is doing a lot of work to pre-process and aggregate the datas

        for (final Map<String, Object> criteriaDataPoint : results) {
            String[] dataArray = new String[fieldIndexMap.size()];

            //TODO should probably drive this off a new fieldIndexMap.getEntries() method or similar
            //then we only loop round fields we car about
            criteriaDataPoint.forEach((key, value) -> {

                int posInDataArray = fieldIndexMap.get(key);
                //if the fieldIndexMap returns -1 the field has not been requested
                if (posInDataArray != -1) {
                    dataArray[posInDataArray] = value.toString();
                }
            });

            coprocessorMap.entrySet().forEach(coprocessor -> {
                coprocessor.getValue().receive(dataArray);
            });
        }

        // TODO putting things into a payload and taking them out again is a waste of time in this case. We could use a queue instead and that'd be fine.
        //TODO: 'Payload' is a cluster specific name - what lucene ships back from a node.
        // Produce payloads for each coprocessor.
        Map<CoprocessorSettingsMap.CoprocessorKey, Payload> payloadMap = null;
        if (coprocessorMap != null && coprocessorMap.size() > 0) {
            for (final Map.Entry<CoprocessorSettingsMap.CoprocessorKey, Coprocessor> entry : coprocessorMap.entrySet()) {
                final Payload payload = entry.getValue().createPayload();
                if (payload != null) {
                    if (payloadMap == null) {
                        payloadMap = new HashMap<>();
                    }
                    payloadMap.put(entry.getKey(), payload);
                }
            }
        }

        // Construct the store
        final List<Integer> storeSize = Collections.singletonList(results.size());
        CriteriaStore store = new CriteriaStore(storeSize, new StoreSize(storeSize),
                coprocessorSettingsMap,
                payloadMap);

        // defaultMaxResultsSizes could be obtained from the StatisticsStore but at this point that object is ephemeral.
        // It seems a little pointless to put it into the StatisticsStore only to get it out again so for now
        // we'll just get it straight from the config.

        final SearchResponseCreator searchResponseCreator = new SearchResponseCreator(store);

        return searchResponseCreator.create(searchRequest);
    }
}
