package stroom.query.elastic.resources;

import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.dashboard.expression.v1.FieldIndexMap;
import stroom.datasource.api.v2.DataSource;
import stroom.datasource.api.v2.DataSourceField;
import stroom.query.api.v2.*;
import stroom.query.common.v2.*;
import stroom.query.elastic.hibernate.ElasticIndexConfig;
import stroom.query.elastic.hibernate.ElasticIndexConfigService;
import stroom.query.elastic.store.ElasticStore;
import stroom.util.shared.HasTerminate;

import javax.inject.Inject;
import javax.ws.rs.core.Response;
import java.util.*;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

public class QueryResourceImpl implements QueryResource {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryResourceImpl.class);

    private final TransportClient client;
    private final ElasticIndexConfigService elasticIndexConfigService;

    @Inject
    public QueryResourceImpl(final TransportClient client,
                             final ElasticIndexConfigService elasticIndexConfigService) {
        this.client = client;
        this.elasticIndexConfigService = elasticIndexConfigService;
    }

    @Override
    public Response getDataSource(final DocRef docRef) {
        LOGGER.info("Getting Data Source for DocRef: " + docRef);

        final Optional<ElasticIndexConfig> elasticIndexConfigO = elasticIndexConfigService.get(docRef);

        if (!elasticIndexConfigO.isPresent()) {
            return Response.noContent().build();
        }

        final ElasticIndexConfig elasticIndexConfig = elasticIndexConfigO.get();

        LOGGER.info("Found Elastic Config!" + elasticIndexConfig);

        try {
            final List<DataSourceField> fields = new ArrayList<>();

            Future<GetFieldMappingsResponse> fieldMappings = client.admin().indices()
                    .getFieldMappings(new GetFieldMappingsRequest()
                            .indices(elasticIndexConfig.getIndexName())
                            .types(elasticIndexConfig.getIndexedType())
                            .fields("*"));

            final GetFieldMappingsResponse response = fieldMappings.get();

            response.mappings().forEach((index, stringMapMap) -> {
                stringMapMap.forEach((type, stringFieldMappingMetaDataMap) -> {
                    stringFieldMappingMetaDataMap.forEach((fieldName, fieldMappingMetaData) -> {
                        fieldMappingMetaData.sourceAsMap().forEach((fieldNameAgain, meta) -> {
                            if (meta instanceof Map) {
                                final Map<?, ?> metaAsMap = (Map) meta;
                                metaAsMap.forEach((metaProp, metaPropValue) -> {
                                    final DataSourceField dataSourceField = new DataSourceField(
                                            DataSourceField.DataSourceFieldType.FIELD,
                                            fieldName,
                                            true,
                                            Arrays.asList(
                                                    ExpressionTerm.Condition.EQUALS,
                                                    ExpressionTerm.Condition.CONTAINS,
                                                    ExpressionTerm.Condition.GREATER_THAN,
                                                    ExpressionTerm.Condition.GREATER_THAN_OR_EQUAL_TO,
                                                    ExpressionTerm.Condition.LESS_THAN,
                                                    ExpressionTerm.Condition.LESS_THAN_OR_EQUAL_TO,
                                                    ExpressionTerm.Condition.BETWEEN,
                                                    ExpressionTerm.Condition.IN
                                            ));
                                    fields.add(dataSourceField);
                                });
                            }
                        });
                    });
                });
            });

            LOGGER.info("Response " + response);

            return Response.ok(new DataSource(fields)).build();

        } catch (Exception e) {
            LOGGER.warn("Could not query the datasource for field mappings", e);
            return Response.serverError().build();
        }
    }

    @Override
    public Response search(final SearchRequest request) {
        final Optional<ElasticIndexConfig> elasticIndexConfigO = elasticIndexConfigService.get(request.getQuery().getDataSource());

        if (!elasticIndexConfigO.isPresent()) {
            return Response.noContent().build();
        }

        final ElasticIndexConfig elasticIndexConfig = elasticIndexConfigO.get();

        final QueryBuilder elasticQuery = getQuery(request.getQuery().getExpression());

        org.elasticsearch.action.search.SearchResponse response = client
                .prepareSearch(elasticIndexConfig.getIndexName())
                .setTypes(elasticIndexConfig.getIndexedType())
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(elasticQuery)
                .get();

        LOGGER.info("Found " + response);

        for (final SearchHit hit : response.getHits().getHits()) {
            LOGGER.info("Hit " + hit);
            hit.getSource().forEach((s, objects) -> {
                LOGGER.info("Hit Field " + s + " - "  + objects);
            });
        }

        final List<Map<String, Object>> hits = Arrays.stream(response.getHits().getHits())
                .map(SearchHit::getSource)
                .collect(Collectors.toList());

        final stroom.query.api.v2.SearchResponse searchResponse = projectResults(request, hits);

        return Response.ok(searchResponse).build();
    }

    @Override
    public Response destroy(QueryKey queryKey) {

        return Response
                .ok(Boolean.TRUE)
                .build();
    }

    private QueryBuilder getQuery(final ExpressionItem item) {
        if (!item.enabled()) {
            return null;
        }

        if (item instanceof ExpressionTerm) {
            final ExpressionTerm term = (ExpressionTerm) item;

            switch (term.getCondition()) {
                case EQUALS: {
                    return QueryBuilders.termQuery(term.getField(), term.getValue());
                }
                case CONTAINS: {
                    return QueryBuilders.termQuery(term.getField(), term.getValue());
                }
                case BETWEEN: {
                    final String[] parts = term.getValue().split(",");
                    if (parts.length == 2) {
                        final String from = parts[0];
                        final String to = parts[1];
                        return QueryBuilders.rangeQuery(term.getField()).from(from).to(to);
                    }
                    break;
                }
                case GREATER_THAN: {
                    return QueryBuilders.rangeQuery(term.getField()).gt(term.getValue());
                }
                case GREATER_THAN_OR_EQUAL_TO: {
                    return QueryBuilders.rangeQuery(term.getField()).gte(term.getValue());
                }
                case LESS_THAN: {
                    return QueryBuilders.rangeQuery(term.getField()).lt(term.getValue());
                }
                case LESS_THAN_OR_EQUAL_TO: {
                    return QueryBuilders.rangeQuery(term.getField()).lte(term.getValue());
                }
                case IN: {
                    final String[] parts = term.getValue().split(",");

                    // Compose an effective 'or' statement
                    final BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
                    Arrays.stream(parts)
                            .forEach(p -> boolQuery.should(QueryBuilders.termQuery(term.getField(), p)));
                    return boolQuery;
                }
                case IN_DICTIONARY: {
                    // Not sure how to handle this yet
                }

            }

        } else if (item instanceof ExpressionOperator) {
            final ExpressionOperator operator = (ExpressionOperator) item;

            final BiConsumer<BoolQueryBuilder, QueryBuilder> consumer;
            switch (operator.getOp()) {
                case AND:
                    consumer = BoolQueryBuilder::must;
                    break;
                case OR:
                    consumer = BoolQueryBuilder::should;
                    break;
                case NOT:
                    consumer = BoolQueryBuilder::mustNot;
                    break;
                default:
                    consumer = BoolQueryBuilder::must; // shouldn't really get here
                    break;
            }

            // Construct the combined term
            final BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
            operator.getChildren().stream()
                    .map(this::getQuery)
                    .filter(Objects::nonNull)
                    .forEach(queryBuilder -> consumer.accept(boolQuery, queryBuilder));
            return boolQuery;
        }

        return null;
    }

    // TODO I copied this from 'stats', but can't make head or tail of it to try and move it into somewhere more sensible
    private stroom.query.api.v2.SearchResponse projectResults(final SearchRequest searchRequest,
                                                              final List<Map<String, Object>> hits) {

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
                    final HasTerminate taskMonitor = new HasTerminate() {
                        //TODO do something about this
                        @Override
                        public void terminate() {
                            System.out.println("terminating");
                        }

                        @Override
                        public boolean isTerminated() {
                            return false;
                        }
                    };
                    final Coprocessor coprocessor = new TableCoprocessor(
                            tableCoprocessorSettings, fieldIndexMap, taskMonitor, paramMap);

                    coprocessorMap.put(coprocessorId, coprocessor);
                }
            }
        }

        //TODO TableCoprocessor is doing a lot of work to pre-process and aggregate the datas

        for (Map<String, Object> hit : hits) {
            String[] dataArray = new String[fieldIndexMap.size()];

            //TODO should probably drive this off a new fieldIndexMap.getEntries() method or similar
            //then we only loop round fields we car about
            hit.forEach((fieldName, value) -> {
                int posInDataArray = fieldIndexMap.get(fieldName);
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
        final List<Integer> storeSize = Collections.singletonList(hits.size());
        ElasticStore store = new ElasticStore(storeSize, new StoreSize(storeSize));
        store.process(coprocessorSettingsMap);
        store.coprocessorMap(coprocessorMap);
        store.payloadMap(payloadMap);

        // defaultMaxResultsSizes could be obtained from the StatisticsStore but at this point that object is ephemeral.
        // It seems a little pointless to put it into the StatisticsStore only to get it out again so for now
        // we'll just get it straight from the config.

        final SearchResponseCreator searchResponseCreator = new SearchResponseCreator(store);

        return searchResponseCreator.create(searchRequest);
    }
}
