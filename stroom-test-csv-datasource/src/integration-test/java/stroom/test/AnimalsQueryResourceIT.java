package stroom.test;

import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import org.eclipse.jetty.http.HttpStatus;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.datasource.api.v2.DataSource;
import stroom.datasource.api.v2.DataSourceField;
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
import java.util.stream.Collectors;

import static io.dropwizard.testing.ResourceHelpers.resourceFilePath;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static stroom.query.testing.FifoLogbackRule.containsAllOf;
import static stroom.test.AnimalTestData.getAnimalSightingsFromResponse;

public class AnimalsQueryResourceIT extends QueryResourceIT<CsvDocRefEntity, CsvConfig> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AnimalsQueryResourceIT.class);

    @ClassRule
    public static final DropwizardAppWithClientsRule<CsvConfig> appRule =
            new DropwizardAppWithClientsRule<>(AnimalApp.class, resourceFilePath(TestConstants.ANIMALS_APP_CONFIG));

    @ClassRule
    public static final StroomAuthenticationRule authRule =
            new StroomAuthenticationRule(WireMockConfiguration.options().port(TestConstants.TEST_AUTH_PORT));

    @ClassRule
    public static final FlatFileTestDataRule testDataRule = FlatFileTestDataRule.withTempDirectory()
            .testDataGenerator(AnimalTestData.build())
            .build();

    private static final CsvFieldSupplier csvFieldSupplier = new AnimalFieldSupplier();

    public AnimalsQueryResourceIT() {
        super(CsvDocRefEntity.TYPE,
                appRule,
                authRule);
    }

    @Test
    public void testStreamIdSearch() {
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
        final Long testMaxStreamId = 100L;
        final ExpressionOperator expressionOperator = new ExpressionOperator.Builder(ExpressionOperator.Op.AND)
                .addTerm(AnimalSighting.STREAM_ID, ExpressionTerm.Condition.LESS_THAN, Long.toString(testMaxStreamId))
                .build();

        final SearchRequest searchRequest = getValidSearchRequest(docRef, expressionOperator, offset);

        final Response response = queryClient.search(authRule.adminUser(), searchRequest);
        assertEquals(HttpStatus.OK_200, response.getStatus());

        final SearchResponse searchResponse = response.readEntity(SearchResponse.class);

        final Set<AnimalSighting> resultsSet = new HashSet<>();

        assertTrue("No results seen", searchResponse.getResults().size() > 0);
        for (final Result result : searchResponse.getResults()) {
            assertTrue(result instanceof FlatResult);

            final FlatResult flatResult = (FlatResult) result;
            flatResult.getValues().stream()
                    //.map(objects -> objects.get(1))
                    .map(o -> {
                        final CsvDataRow row = new CsvDataRow();
                        csvFieldSupplier.getFields()
                                .forEach(f -> row.withField(f, o.get(f.getPosition() + 3))); // skip over 3 std fields
                        return row;
                    })
                    .map(AnimalSighting::new)
                    .forEach(resultsSet::add);
        }

        // Check that the returned data matches the conditions
        resultsSet.stream().map(AnimalSighting::getStreamId)
                .forEach(t -> assertTrue(testMaxStreamId > t));

        LOGGER.info("Results from Search {}", resultsSet.size());
        resultsSet.stream()
                .map(Object::toString)
                .forEach(LOGGER::info);

        auditLogRule.check().thereAreAtLeast(1)
                .containsOrdered(containsAllOf(AuditedQueryResourceImpl.QUERY_SEARCH, docRef.getUuid()));
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
        final String testObserver = "alpha";
        final LocalDateTime testMaxDate = LocalDateTime.of(2017, 1, 1, 0, 0, 0);
        final ExpressionOperator expressionOperator = new ExpressionOperator.Builder(ExpressionOperator.Op.AND)
                .addTerm(AnimalSighting.OBSERVER, ExpressionTerm.Condition.CONTAINS, testObserver)
                .addTerm(AnimalSighting.TIME,
                        ExpressionTerm.Condition.LESS_THAN,
                        testMaxDate.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                )
                .build();

        final SearchRequest searchRequest = getValidSearchRequest(docRef, expressionOperator, offset);

        final Response response = queryClient.search(authRule.adminUser(), searchRequest);
        assertEquals(HttpStatus.OK_200, response.getStatus());

        final SearchResponse searchResponse = response.readEntity(SearchResponse.class);

        final Set<AnimalSighting> resultsSet = getAnimalSightingsFromResponse(searchResponse);

        // Check that the returned data matches the conditions
        resultsSet.stream().map(AnimalSighting::getObserver)
                .forEach(o -> assertEquals(testObserver, o));
        resultsSet.stream().map(AnimalSighting::getTime)
                .forEach(t -> assertTrue(testMaxDate.isAfter(t)));

        LOGGER.info("Results from Search {}", resultsSet.size());
        resultsSet.stream()
                .map(Object::toString)
                .forEach(LOGGER::info);

        auditLogRule.check().thereAreAtLeast(1)
                .containsOrdered(containsAllOf(AuditedQueryResourceImpl.QUERY_SEARCH, docRef.getUuid()));
    }

    @Override
    protected void assertValidDataSource(final DataSource dataSource) {
        final Set<String> resultFieldNames = dataSource.getFields().stream()
                .map(DataSourceField::getName)
                .collect(Collectors.toSet());

        assertTrue(resultFieldNames.contains(AnimalSighting.STREAM_ID));
        assertTrue(resultFieldNames.contains(AnimalSighting.SPECIES));
        assertTrue(resultFieldNames.contains(AnimalSighting.LOCATION));
        assertTrue(resultFieldNames.contains(AnimalSighting.OBSERVER));
        assertTrue(resultFieldNames.contains(AnimalSighting.TIME));
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
        return AnimalTestData.getTestSearchRequest(docRef, expressionOperator, offsetRange);
    }
}
