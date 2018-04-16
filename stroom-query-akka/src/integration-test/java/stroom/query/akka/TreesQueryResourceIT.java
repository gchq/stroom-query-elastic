package stroom.query.akka;

import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import org.eclipse.jetty.http.HttpStatus;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.datasource.api.v2.DataSource;
import stroom.datasource.api.v2.DataSourceField;
import stroom.query.akka.app.*;
import stroom.query.api.v2.*;
import stroom.query.audit.rest.AuditedDocRefResourceImpl;
import stroom.query.audit.rest.AuditedQueryResourceImpl;
import stroom.query.csv.CsvConfig;
import stroom.query.csv.CsvDataRow;
import stroom.query.csv.CsvDocRefEntity;
import stroom.query.csv.CsvFieldSupplier;
import stroom.query.testing.DropwizardAppWithClientsRule;
import stroom.query.testing.QueryResourceIT;
import stroom.query.testing.StroomAuthenticationRule;
import stroom.testdata.FlatFileTestDataRule;

import javax.ws.rs.core.Response;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static io.dropwizard.testing.ResourceHelpers.resourceFilePath;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static stroom.query.testing.FifoLogbackRule.containsAllOf;

public class TreesQueryResourceIT extends QueryResourceIT<CsvDocRefEntity, CsvConfig> {
    private static final Logger LOGGER = LoggerFactory.getLogger(TreesQueryResourceIT.class);

    @ClassRule
    public static final DropwizardAppWithClientsRule<CsvConfig> appRule =
            new DropwizardAppWithClientsRule<>(TreesApp.class, resourceFilePath(TestConstants.APP_CONFIG));

    @ClassRule
    public static final StroomAuthenticationRule authRule =
            new StroomAuthenticationRule(WireMockConfiguration.options().port(TestConstants.TEST_AUTH_PORT));

    @ClassRule
    public static final FlatFileTestDataRule testDataRule = FlatFileTestDataRule.withTempDirectory()
            .testDataGenerator(TreesTestData.build())
            .build();

    private static final CsvFieldSupplier csvFieldSupplier = new TreesFieldSupplier();

    public TreesQueryResourceIT() {
        super(CsvDocRefEntity.TYPE,
                appRule,
                authRule);
    }

    @Test
    public void testQuerySearch() {
        final DocRef docRef = createDocument(new CsvDocRefEntity.Builder()
                .dataDirectory(testDataRule.getFolder().getAbsolutePath())
                .build());

        auditLogRule.check().thereAreAtLeast(2)
                .containsOrdered(containsAllOf(AuditedDocRefResourceImpl.CREATE_DOC_REF, docRef.getUuid()))
                .containsOrdered(containsAllOf(AuditedDocRefResourceImpl.UPDATE_DOC_REF, docRef.getUuid()));

        final OffsetRange offset = new OffsetRange.Builder()
                .length(100L)
                .offset(0L)
                .build();
        final String testSpecies = "oak";
        final LocalDateTime testMaxDate = LocalDateTime.of(2017, 1, 1, 0, 0, 0);
        final ExpressionOperator expressionOperator = new ExpressionOperator.Builder(ExpressionOperator.Op.AND)
                .addTerm(TreeSighting.SPECIES, ExpressionTerm.Condition.CONTAINS, testSpecies)
                .addTerm(TreeSighting.TIME,
                        ExpressionTerm.Condition.LESS_THAN,
                        testMaxDate.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                )
                .build();

        final SearchRequest searchRequest = getValidSearchRequest(docRef, expressionOperator, offset);

        final Response response = queryClient.search(authRule.adminUser(), searchRequest);
        assertEquals(HttpStatus.OK_200, response.getStatus());

        final SearchResponse searchResponse = response.readEntity(SearchResponse.class);

        final Set<TreeSighting> resultsSet = getTreeSightingsFromResponse(searchResponse);

        // Check that the returned data matches the conditions
        resultsSet.stream().map(TreeSighting::getSpecies)
                .forEach(o -> assertEquals(testSpecies, o));
        resultsSet.stream().map(TreeSighting::getTime)
                .forEach(t -> assertTrue(testMaxDate.isAfter(t)));

        LOGGER.info("Results from Search {}", resultsSet.size());
        resultsSet.stream()
                .map(Object::toString)
                .forEach(LOGGER::info);

        auditLogRule.check().thereAreAtLeast(1)
                .containsOrdered(containsAllOf(AuditedQueryResourceImpl.QUERY_SEARCH, docRef.getUuid()));
    }

    public static Set<TreeSighting> getTreeSightingsFromResponse(final SearchResponse searchResponse) {
        final Set<TreeSighting> resultsSet = new HashSet<>();

        assertTrue("No results seen", searchResponse.getResults().size() > 0);
        for (final Result result : searchResponse.getResults()) {
            assertTrue(result instanceof FlatResult);

            final FlatResult flatResult = (FlatResult) result;
            flatResult.getValues().stream()
                    .map(o -> {
                        final CsvDataRow row = new CsvDataRow();
                        csvFieldSupplier.getFields()
                                .forEach(f -> row.withField(f, o.get(f.getPosition() + 3))); // skip over 3 std fields
                        return row;
                    })
                    .map(TreeSighting::new)
                    .forEach(resultsSet::add);
        }

        return resultsSet;
    }

    @Override
    protected void assertValidDataSource(final DataSource dataSource) {
        final Set<String> resultFieldNames = dataSource.getFields().stream()
                .map(DataSourceField::getName)
                .collect(Collectors.toSet());

        assertTrue(resultFieldNames.contains(TreeSighting.SPECIES));
        assertTrue(resultFieldNames.contains(TreeSighting.LOCATION));
        assertTrue(resultFieldNames.contains(TreeSighting.LUMBERJACK));
        assertTrue(resultFieldNames.contains(TreeSighting.TIME));
    }

    @Override
    protected CsvDocRefEntity getValidEntity(final DocRef docRef) {
        return new CsvDocRefEntity.Builder()
                .docRef(docRef)
                .dataDirectory(testDataRule.getFolder().getAbsolutePath())
                .build();
    }

    protected SearchRequest getValidSearchRequest(final DocRef docRef,
                                                  final ExpressionOperator expressionOperator,
                                                  final OffsetRange offsetRange) {
        return getTestSearchRequest(docRef, expressionOperator, offsetRange);
    }

    static SearchRequest getTestSearchRequest(final DocRef docRef,
                                                     final ExpressionOperator expressionOperator,
                                                     final OffsetRange offsetRange) {
        final String queryKey = UUID.randomUUID().toString();
        return new SearchRequest.Builder()
                .query(new Query.Builder()
                        .dataSource(docRef)
                        .expression(expressionOperator)
                        .build())
                .key(queryKey)
                .dateTimeLocale("en-gb")
                .incremental(true)
                .addResultRequests(new ResultRequest.Builder()
                        .fetch(ResultRequest.Fetch.ALL)
                        .resultStyle(ResultRequest.ResultStyle.FLAT)
                        .componentId("componentId")
                        .requestedRange(offsetRange)
                        .addMappings(new TableSettings.Builder()
                                .queryId(queryKey)
                                .extractValues(false)
                                .showDetail(false)
                                .addFields(new Field.Builder()
                                        .name(TreeSighting.SPECIES)
                                        .expression("${" + TreeSighting.SPECIES + "}")
                                        .build())
                                .addFields(new Field.Builder()
                                        .name(TreeSighting.LOCATION)
                                        .expression("${" + TreeSighting.LOCATION + "}")
                                        .build())
                                .addFields(new Field.Builder()
                                        .name(TreeSighting.LUMBERJACK)
                                        .expression("${" + TreeSighting.LUMBERJACK + "}")
                                        .build())
                                .addFields(new Field.Builder()
                                        .name(TreeSighting.TIME)
                                        .expression("${" + TreeSighting.TIME + "}")
                                        .build())
                                .addMaxResults(1000)
                                .build())
                        .build())
                .build();
    }
}
