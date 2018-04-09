package stroom.autoindex.animals;

import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import org.eclipse.jetty.http.HttpStatus;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.autoindex.TestConstants;
import stroom.autoindex.animals.app.AnimalApp;
import stroom.autoindex.animals.app.AnimalConfig;
import stroom.autoindex.animals.app.AnimalDocRefEntity;
import stroom.autoindex.animals.app.AnimalSighting;
import stroom.datasource.api.v2.DataSource;
import stroom.datasource.api.v2.DataSourceField;
import stroom.query.api.v2.DocRef;
import stroom.query.api.v2.ExpressionOperator;
import stroom.query.api.v2.ExpressionTerm;
import stroom.query.api.v2.Field;
import stroom.query.api.v2.FlatResult;
import stroom.query.api.v2.OffsetRange;
import stroom.query.api.v2.Query;
import stroom.query.api.v2.Result;
import stroom.query.api.v2.ResultRequest;
import stroom.query.api.v2.SearchRequest;
import stroom.query.api.v2.SearchResponse;
import stroom.query.api.v2.TableSettings;
import stroom.query.audit.rest.AuditedDocRefResourceImpl;
import stroom.query.audit.rest.AuditedQueryResourceImpl;
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

public class AnimalsQueryResourceIT extends QueryResourceIT<AnimalDocRefEntity, AnimalConfig> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AnimalsQueryResourceIT.class);

    @ClassRule
    public static final DropwizardAppWithClientsRule<AnimalConfig> appRule =
            new DropwizardAppWithClientsRule<>(AnimalApp.class, resourceFilePath(TestConstants.ANIMALS_APP_CONFIG));

    @ClassRule
    public static final StroomAuthenticationRule authRule =
            new StroomAuthenticationRule(WireMockConfiguration.options().port(TestConstants.TEST_AUTH_PORT));

    @ClassRule
    public static final FlatFileTestDataRule testDataRule = FlatFileTestDataRule.withTempDirectory()
            .testDataGenerator(AnimalTestData.build())
            .build();

    public AnimalsQueryResourceIT() {
        super(AnimalDocRefEntity.class,
                AnimalDocRefEntity.TYPE,
                appRule,
                authRule);
    }

    @Test
    public void testStreamIdSearch() {
        final DocRef docRef = createDocument(new AnimalDocRefEntity.Builder()
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
                    .map(o -> new AnimalSighting.Builder()
                            .streamId(Long.valueOf(o.get(3).toString()))
                            .species(o.get(4).toString())
                            .location(o.get(5).toString())
                            .observer(o.get(6).toString())
                            .time(LocalDateTime.parse(o.get(7).toString(), DateTimeFormatter.ISO_LOCAL_DATE_TIME))
                            .build())
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
        final DocRef docRef = createDocument(new AnimalDocRefEntity.Builder()
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

    public static Set<AnimalSighting> getAnimalSightingsFromResponse(final SearchResponse searchResponse) {
        final Set<AnimalSighting> resultsSet = new HashSet<>();

        assertTrue("No results seen", searchResponse.getResults().size() > 0);
        for (final Result result : searchResponse.getResults()) {
            assertTrue(result instanceof FlatResult);

            final FlatResult flatResult = (FlatResult) result;
            flatResult.getValues().stream()
                    .map(o -> new AnimalSighting.Builder()
                            .streamId(Long.valueOf(o.get(3).toString()))
                            .species(o.get(4).toString())
                            .location(o.get(5).toString())
                            .observer(o.get(6).toString())
                            .time(LocalDateTime.parse(o.get(7).toString(), DateTimeFormatter.ISO_LOCAL_DATE_TIME))
                            .build())
                    .forEach(resultsSet::add);
        }

        return resultsSet;
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
    protected AnimalDocRefEntity getValidEntity(final DocRef docRef) {
        return new AnimalDocRefEntity.Builder()
                .docRef(docRef)
                .dataDirectory(testDataRule.getFolder().getAbsolutePath())
                .build();
    }

    protected SearchRequest getValidSearchRequest(final DocRef docRef,
                                                  final ExpressionOperator expressionOperator,
                                                  final OffsetRange offsetRange) {
        return AnimalsQueryResourceIT.getTestSearchRequest(docRef, expressionOperator, offsetRange);
    }

    public static SearchRequest getTestSearchRequest(final DocRef docRef,
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
                                        .name(AnimalSighting.STREAM_ID)
                                        .expression("${" + AnimalSighting.STREAM_ID + "}")
                                        .build())
                                .addFields(new Field.Builder()
                                        .name(AnimalSighting.SPECIES)
                                        .expression("${" + AnimalSighting.SPECIES + "}")
                                        .build())
                                .addFields(new Field.Builder()
                                        .name(AnimalSighting.LOCATION)
                                        .expression("${" + AnimalSighting.LOCATION + "}")
                                        .build())
                                .addFields(new Field.Builder()
                                        .name(AnimalSighting.OBSERVER)
                                        .expression("${" + AnimalSighting.OBSERVER + "}")
                                        .build())
                                .addFields(new Field.Builder()
                                        .name(AnimalSighting.TIME)
                                        .expression("${" + AnimalSighting.TIME + "}")
                                        .build())
                                .addMaxResults(1000)
                                .build())
                        .build())
                .build();
    }
}
