package stroom.akka.query.cluster;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import stroom.akka.query.cluster.QuerySearchBackendMain;
import stroom.akka.query.cluster.QuerySearchFrontendMain;
import stroom.akka.query.messages.QuerySearchMessages;
import stroom.query.api.v2.Field;
import stroom.query.api.v2.FlatResult;
import stroom.query.api.v2.SearchRequest;
import stroom.query.api.v2.SearchResponse;
import stroom.query.audit.service.QueryApiException;
import stroom.query.audit.service.QueryService;
import stroom.security.ServiceUser;

import java.io.IOException;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

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
        final SearchRequest testRequest = new SearchRequest.Builder()
                .key(UUID.randomUUID().toString())
                .build();
        final SearchResponse testResponse = new SearchResponse.FlatResultBuilder()
                .addResults(new FlatResult.Builder()
                        .addField(new Field.Builder()
                                .name(UUID.randomUUID().toString())
                                .build())
                        .build())
                .build();
        when(queryService.search(user, testRequest)).thenReturn(Optional.of(testResponse));

        // When
        // Run multiple backend nodes
        Stream.of(2551, 2552, 2553).forEach(port -> QuerySearchBackendMain.onPort(port)
                .withService(queryService)
                .asUser(user)
                .build()
                .run());

        final QuerySearchFrontendMain frontend = QuerySearchFrontendMain.create();
        frontend.run();

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        final SearchResponse response = frontend.search(user, testRequest)
                .orElseThrow(() -> new AssertionError("No search response given"));

        // Then
        verify(queryService).search(user, testRequest);
    }
}
