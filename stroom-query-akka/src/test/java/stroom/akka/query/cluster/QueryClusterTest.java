package stroom.akka.query.cluster;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import stroom.akka.query.cluster.QuerySearchBackendMain;
import stroom.akka.query.cluster.QuerySearchFrontendMain;
import stroom.akka.query.messages.QuerySearchMessages;
import stroom.datasource.api.v2.DataSource;
import stroom.datasource.api.v2.DataSourceField;
import stroom.query.api.v2.*;
import stroom.query.audit.service.QueryApiException;
import stroom.query.audit.service.QueryService;
import stroom.security.ServiceUser;

import java.io.IOException;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class QueryClusterTest {

    @Mock
    private QueryService queryService;

    @Test
    public void testSearchHandedToBackend() throws QueryApiException {

        // Given
        final ServiceUser user = new ServiceUser.Builder()
                .name("Test User Dude")
                .jwt(UUID.randomUUID().toString())
                .build();
        final DocRef testDocRef = new DocRef.Builder()
                .uuid(UUID.randomUUID().toString())
                .type("Test")
                .name("A test document")
                .build();
        final SearchRequest testRequest = new SearchRequest.Builder()
                .key(UUID.randomUUID().toString())
                .build();
        final DataSource testDataSource = new DataSource.Builder()
                .addFields(new DataSourceField.Builder()
                        .name("SomeID")
                        .type(DataSourceField.DataSourceFieldType.ID)
                        .build())
                .addFields(new DataSourceField.Builder()
                        .name("SomeName")
                        .type(DataSourceField.DataSourceFieldType.FIELD)
                        .build())
                .build();
        final SearchResponse testResponse = new SearchResponse.FlatResultBuilder()
                .addResults(new FlatResult.Builder()
                        .addField(new Field.Builder()
                                .name(UUID.randomUUID().toString())
                                .build())
                        .build())
                .build();
        when(queryService.getDataSource(user, testDocRef)).thenReturn(Optional.of(testDataSource));
        when(queryService.search(user, testRequest)).thenReturn(Optional.of(testResponse));

        // When
        // Run multiple backend nodes
        Stream.of(2551, 2552, 2553).forEach(port -> QuerySearchBackendMain.onPort(port)
                .withService(queryService)
                .asUser(user)
                .build()
                .run());

        final QuerySearchFrontendMain frontend = QuerySearchFrontendMain.create();
        final CompletableFuture<Boolean> clusterReady = frontend.run();

        try {
            clusterReady.get(10, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            fail(e.getLocalizedMessage());
        }

        final DataSource dataSourceResponse = frontend.getDataSource(user, testDocRef)
                .orElseThrow(() -> new AssertionError("No DataSource response given"));

        final SearchResponse searchResponse = frontend.search(user, testRequest)
                .orElseThrow(() -> new AssertionError("No search response given"));

        // Then
        verify(queryService).search(user, testRequest);
        verify(queryService).getDataSource(user, testDocRef);

        assertEquals(testDataSource, dataSourceResponse);
        assertEquals(testResponse, searchResponse);
    }
}
