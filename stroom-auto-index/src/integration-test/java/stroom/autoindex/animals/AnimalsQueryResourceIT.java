package stroom.autoindex.animals;

import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import org.eclipse.jetty.http.HttpStatus;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.autoindex.animals.app.AnimalApp;
import stroom.autoindex.animals.app.AnimalConfig;
import stroom.autoindex.animals.app.AnimalDocRefEntity;
import stroom.autoindex.animals.app.AnimalSighting;
import stroom.datasource.api.v2.DataSource;
import stroom.datasource.api.v2.DataSourceField;
import stroom.elastic.test.FlatFileTestDataRule;
import stroom.query.api.v2.*;
import stroom.query.audit.authorisation.DocumentPermission;
import stroom.query.audit.rest.AuditedDocRefResourceImpl;
import stroom.query.testing.DropwizardAppWithClientsRule;
import stroom.query.testing.QueryResourceIT;
import stroom.query.testing.StroomAuthenticationRule;
import stroom.testdata.DataGenerator;
import stroom.testdata.FlatDataWriterBuilder;

import javax.ws.rs.core.Response;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static io.dropwizard.testing.ResourceHelpers.resourceFilePath;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static stroom.query.testing.FifoLogbackRule.containsAllOf;

public class AnimalsQueryResourceIT extends QueryResourceIT<AnimalDocRefEntity, AnimalConfig> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AnimalsQueryResourceIT.class);

    @ClassRule
    public static final DropwizardAppWithClientsRule<AnimalConfig> appRule =
            new DropwizardAppWithClientsRule<>(AnimalApp.class, resourceFilePath("animal/config.yml"));

    @ClassRule
    public static final StroomAuthenticationRule authRule =
            new StroomAuthenticationRule(WireMockConfiguration.options().port(10080), AnimalDocRefEntity.TYPE);

    @ClassRule
    public static final FlatFileTestDataRule testDataRule = FlatFileTestDataRule.withTempDirectory()
            .testDataGenerator(AnimalsQueryResourceIT::generateTestData)
            .build();

    private static void generateTestData(final Consumer<String> writer) {
        DataGenerator.buildDefinition()
                .addFieldDefinition(DataGenerator.randomValueField(AnimalSighting.SPECIES,
                        Arrays.asList("spider", "whale", "dog", "tiger", "monkey", "lion", "woodlouse", "honey-badger")))
                .addFieldDefinition(DataGenerator.randomValueField(AnimalSighting.LOCATION,
                        Arrays.asList("europe", "asia", "america", "antarctica", "africa", "australia")))
                .addFieldDefinition(DataGenerator.randomValueField(AnimalSighting.OBSERVER,
                        Arrays.asList("alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel")))
                .addFieldDefinition(DataGenerator.randomDateTimeField(AnimalSighting.TIME,
                        LocalDateTime.of(2016, 1, 1, 0, 0, 0),
                        LocalDateTime.of(2018, 1, 1, 0, 0, 0),
                        DateTimeFormatter.ISO_LOCAL_DATE_TIME))
                .setDataWriter(FlatDataWriterBuilder.defaultCsvFormat())
                .rowCount(1000)
                .consumedBy(writer)
                .generate();
    }

    public AnimalsQueryResourceIT() {
        super(AnimalDocRefEntity.class,
                AnimalDocRefEntity.TYPE,
                appRule,
                authRule);
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

        final Set<AnimalSighting> resultsSet = new HashSet<>();

        assertTrue("No results seen", searchResponse.getResults().size() > 0);
        for (final Result result : searchResponse.getResults()) {
            assertTrue(result instanceof FlatResult);

            final FlatResult flatResult = (FlatResult) result;
            flatResult.getValues().stream()
                    //.map(objects -> objects.get(1))
                    .map(o -> new AnimalSighting.Builder()
                            .species(o.get(3).toString())
                            .location(o.get(4).toString())
                            .observer(o.get(5).toString())
                            .time(LocalDateTime.parse(o.get(6).toString(), DateTimeFormatter.ISO_LOCAL_DATE_TIME))
                            .build())
                    .forEach(resultsSet::add);
        }

        // Check that the returned data matches the conditions
        resultsSet.stream().map(AnimalSighting::getObserver)
                .forEach(o -> assertEquals(testObserver, o));
        resultsSet.stream().map(AnimalSighting::getTime)
                .forEach(t -> assertTrue(testMaxDate.isAfter(t)));

        LOGGER.info("Results from Search {}", resultsSet.size());
        resultsSet.stream()
                .map(Object::toString)
                .forEach(LOGGER::info);
    }

    @Override
    protected void assertValidDataSource(final DataSource dataSource) {
        final Set<String> resultFieldNames = dataSource.getFields().stream()
                .map(DataSourceField::getName)
                .collect(Collectors.toSet());

        assertTrue(resultFieldNames.contains(AnimalSighting.SPECIES));
        assertTrue(resultFieldNames.contains(AnimalSighting.LOCATION));
        assertTrue(resultFieldNames.contains(AnimalSighting.OBSERVER));
        assertTrue(resultFieldNames.contains(AnimalSighting.TIME));
    }

    @Override
    protected AnimalDocRefEntity getValidEntity(final DocRef docRef) {
        return new AnimalDocRefEntity.Builder()
                .docRef(docRef)
                .dataDirectory(String.format("/tmp/%s", UUID.randomUUID().toString()))
                .build();
    }

    protected SearchRequest getValidSearchRequest(final DocRef docRef,
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

    /**
     * Utility function to randomly generate a new annotations index doc ref.
     * It assumes that the creation of documents works, the detail of that is tested in another suite of tests.
     * Once the document is created, the passed in doc ref entity is then used to flesh out the implementation
     * specific details.
     * @param docRefEntity The implementation specific entity, used to update the doc ref so it can be used.
     * @return The DocRef of the newly created annotations index.
     */
    protected DocRef createDocument(final AnimalDocRefEntity docRefEntity) {
        // Generate UUID's for the doc ref and it's parent folder
        final String parentFolderUuid = UUID.randomUUID().toString();
        final DocRef docRef = new DocRef.Builder()
                .uuid(UUID.randomUUID().toString())
                .type(AnimalDocRefEntity.TYPE)
                .name(UUID.randomUUID().toString())
                .build();

        // Ensure admin user can create the document in the folder
        authRule.giveFolderCreatePermission(authRule.adminUser(), parentFolderUuid);

        // Create a doc ref to hang the search from
        final Response createResponse = docRefClient.createDocument(
                authRule.adminUser(),
                docRef.getUuid(),
                docRef.getName(),
                parentFolderUuid);
        assertEquals(HttpStatus.OK_200, createResponse.getStatus());
        createResponse.close();

        // Give admin all the roles required to manipulate the document and it's underlying data
        authRule.giveDocumentPermission(authRule.adminUser(), docRef.getUuid(), DocumentPermission.READ);
        authRule.giveDocumentPermission(authRule.adminUser(), docRef.getUuid(), DocumentPermission.UPDATE);

        final AnimalDocRefEntity docRefEntityToUse = (docRefEntity != null) ? docRefEntity : getValidEntity(docRef);
        final Response updateIndexResponse =
                docRefClient.update(authRule.adminUser(),
                        docRef.getUuid(),
                        docRefEntityToUse);
        assertEquals(HttpStatus.OK_200, updateIndexResponse.getStatus());
        updateIndexResponse.close();

        return docRef;
    }
}
