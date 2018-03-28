package stroom.autoindex;

import org.eclipse.jetty.http.HttpStatus;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.autoindex.animals.AnimalsQueryResourceIT;
import stroom.autoindex.animals.app.AnimalSighting;
import stroom.autoindex.service.AutoIndexDocRefEntity;
import stroom.datasource.api.v2.DataSource;
import stroom.datasource.api.v2.DataSourceField;
import stroom.query.api.v2.*;
import stroom.query.audit.rest.AuditedQueryResourceImpl;

import javax.ws.rs.core.Response;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static stroom.autoindex.animals.AnimalsQueryResourceIT.getAnimalSightingsFromResponse;
import static stroom.query.testing.FifoLogbackRule.containsAllOf;

public class AutoIndexQueryResourceIT extends AbstractAutoIndexIntegrationTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(AutoIndexQueryResourceIT.class);

    @Test
    public void testGetDataSource() {
        final EntityWithDocRef<AutoIndexDocRefEntity> autoIndex = createAutoIndex();

        final Response response = autoIndexQueryClient.getDataSource(authRule.adminUser(), autoIndex.getDocRef());
        assertEquals(HttpStatus.OK_200, response.getStatus());
        final DataSource result = response.readEntity(DataSource.class);

        final Set<String> resultFieldNames = result.getFields().stream()
                .map(DataSourceField::getName)
                .collect(Collectors.toSet());

        assertTrue(resultFieldNames.contains(AnimalSighting.SPECIES));
        assertTrue(resultFieldNames.contains(AnimalSighting.LOCATION));
        assertTrue(resultFieldNames.contains(AnimalSighting.OBSERVER));
        assertTrue(resultFieldNames.contains(AnimalSighting.TIME));

        // Create the 3 doc refs, get data source, which will cause the raw data source to be queried
        auditLogRule.check()
                .thereAreAtLeast(2)
                .containsOrdered(containsAllOf(AuditedQueryResourceImpl.GET_DATA_SOURCE, autoIndex.getEntity().getRawDocRef().getUuid()))
                .containsOrdered(containsAllOf(AuditedQueryResourceImpl.GET_DATA_SOURCE, autoIndex.getDocRef().getUuid()));
    }

    @Test
    public void testSearch() {
        final EntityWithDocRef<AutoIndexDocRefEntity> autoIndex = createAutoIndex();

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

        final SearchRequest searchRequest = AnimalsQueryResourceIT
                .getTestSearchRequest(autoIndex.getDocRef(), expressionOperator, offset);

        final Response response = autoIndexQueryClient.search(authRule.adminUser(), searchRequest);
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

        // Check that the auto index and the raw data source were queried
        auditLogRule.check()
                .thereAreAtLeast(2)
                .containsOrdered(containsAllOf(AuditedQueryResourceImpl.QUERY_SEARCH, autoIndex.getEntity().getRawDocRef().getUuid()))
                .containsOrdered(containsAllOf(AuditedQueryResourceImpl.QUERY_SEARCH, autoIndex.getDocRef().getUuid()));
    }

}
