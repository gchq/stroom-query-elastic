package stroom.akka.query;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.javadsl.TestKit;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.akka.query.actors.QueryDataSourceActor;
import stroom.akka.query.messages.QueryDataSourceMessages;
import stroom.query.api.v2.DocRef;
import stroom.query.audit.service.DocRefService;
import stroom.query.audit.service.QueryApiException;
import stroom.query.audit.service.QueryService;
import stroom.query.audit.service.QueryServiceSupplier;
import stroom.query.csv.CsvDocRefEntity;
import stroom.query.csv.CsvDocRefServiceImpl;
import stroom.query.csv.CsvFieldSupplier;
import stroom.query.csv.CsvQueryServiceImpl;
import stroom.security.ServiceUser;
import stroom.test.AnimalFieldSupplier;
import stroom.test.AnimalTestData;
import stroom.testdata.FlatFileTestDataRule;

import java.util.Optional;
import java.util.UUID;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class QueryDataSourceActorIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryDataSourceActorIT.class);

    private static ActorSystem system;

    private static DocRefService<CsvDocRefEntity> docRefService;
    private static QueryService queryService;
    private static QueryServiceSupplier queryServiceSupplier;

    @ClassRule
    public static final FlatFileTestDataRule testDataRule = FlatFileTestDataRule.withTempDirectory()
            .testDataGenerator(AnimalTestData.build())
            .build();

    @BeforeClass
    public static void beforeClass() {
        system = ActorSystem.create();

        final Injector injector = Guice.createInjector(new AbstractModule() {
            @Override
            protected void configure() {
                bind(QueryService.class).to(CsvQueryServiceImpl.class);
                bind(CsvFieldSupplier.class).to(AnimalFieldSupplier.class);
                bind(DocRefService.class).to(CsvDocRefServiceImpl.class);
            }
        });

        queryService = injector.getInstance(QueryService.class);
        docRefService = injector.getInstance(DocRefService.class);
        queryServiceSupplier = type -> Optional.of(type).filter(CsvDocRefEntity.TYPE::equals).map(t -> queryService);
    }

    @AfterClass
    public static void afterClass() {
        TestKit.shutdownActorSystem(system);
        system = null;
    }

    @Test
    public void testSingleRequest() throws QueryApiException {
        // Given
        final String docRefUuid = UUID.randomUUID().toString();
        final ServiceUser user = new ServiceUser.Builder()
                .name("Me")
                .jwt(UUID.randomUUID().toString())
                .build();
        final TestKit testProbe = new TestKit(system);
        final ActorRef searchActor = system.actorOf(QueryDataSourceActor.props(queryServiceSupplier));
        final CsvDocRefEntity docRefEntity = docRefService.createDocument(user, docRefUuid, "testName")
                .orElseThrow(() -> new AssertionError("Doc Ref Couldn't be created"));
        docRefEntity.setDataDirectory(testDataRule.getFolder().getAbsolutePath());
        docRefService.update(user, docRefUuid, docRefEntity);
        final DocRef docRef = new DocRef.Builder()
                .uuid(docRefEntity.getUuid())
                .name(docRefEntity.getName())
                .type(CsvDocRefEntity.TYPE)
                .build();

        // When
        searchActor.tell(QueryDataSourceMessages.dataSource(user, docRef), testProbe.getRef());

        // Then
        final QueryDataSourceMessages.JobComplete jobComplete = testProbe.expectMsgClass(QueryDataSourceMessages.JobComplete.class);
        LOGGER.info("Job Complete {}", jobComplete);
        assertNotNull(jobComplete.getResponse());
        assertNull(jobComplete.getError());

        // Now try with 'ask'

    }
}
