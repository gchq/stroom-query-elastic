package stroom.query.akka;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.javadsl.TestKit;
import com.google.inject.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.query.akka.app.TreeSighting;
import stroom.query.akka.app.TreesFieldSupplier;
import stroom.query.akka.app.TreesTestData;
import stroom.query.api.v2.*;
import stroom.query.audit.security.ServiceUser;
import stroom.query.audit.service.DocRefService;
import stroom.query.audit.service.QueryApiException;
import stroom.query.audit.service.QueryService;
import stroom.query.csv.CsvDocRefEntity;
import stroom.query.csv.CsvDocRefServiceImpl;
import stroom.query.csv.CsvFieldSupplier;
import stroom.query.csv.CsvQueryServiceImpl;
import stroom.testdata.FlatFileTestDataRule;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TreesQuerySearchActorIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(TreesQuerySearchActorIT.class);

    private static ActorSystem system;

    private static DocRefService<CsvDocRefEntity> docRefService;
    private static QueryService queryService;

    @ClassRule
    public static final FlatFileTestDataRule testDataRule = FlatFileTestDataRule.withTempDirectory()
            .testDataGenerator(TreesTestData.build())
            .build();

    @BeforeClass
    public static void beforeClass() {
        system = ActorSystem.create();

        final Injector injector = Guice.createInjector(new AbstractModule() {
            @Override
            protected void configure() {
                bind(QueryService.class).to(CsvQueryServiceImpl.class);
                bind(CsvFieldSupplier.class).to(TreesFieldSupplier.class);
                bind(DocRefService.class).to(CsvDocRefServiceImpl.class);
            }
        });

        queryService = injector.getInstance(QueryService.class);
        docRefService = (DocRefService<CsvDocRefEntity>) injector.getInstance(DocRefService.class);
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
        final ActorRef searchActor = system.actorOf(
                SearchBackendActor.props(true,
                        CsvDocRefEntity.TYPE,
                        user,
                        queryService,
                        testProbe.getRef()));
        final CsvDocRefEntity docRefEntity = docRefService.createDocument(user, docRefUuid, "testName")
                .orElseThrow(() -> new AssertionError("Doc Ref Couldn't be created"));
        docRefEntity.setDataDirectory(testDataRule.getFolder().getAbsolutePath());
        docRefService.update(user, docRefUuid, docRefEntity);
        final DocRef docRef = new DocRef.Builder()
                .uuid(docRefEntity.getUuid())
                .name(docRefEntity.getName())
                .type(CsvDocRefEntity.TYPE)
                .build();


        final OffsetRange offset = new OffsetRange.Builder()
                .length(100L)
                .offset(0L)
                .build();
        final String testSpecies = "redwood";
        final LocalDateTime testMaxDate = LocalDateTime.of(2017, 1, 1, 0, 0, 0);
        final ExpressionOperator expressionOperator = new ExpressionOperator.Builder(ExpressionOperator.Op.AND)
                .addTerm(TreeSighting.SPECIES, ExpressionTerm.Condition.CONTAINS, testSpecies)
                .addTerm(TreeSighting.TIME,
                        ExpressionTerm.Condition.LESS_THAN,
                        testMaxDate.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                )
                .build();
        final SearchRequest searchRequest = TreesQueryResourceIT.getTestSearchRequest(docRef, expressionOperator, offset);

        // When
        searchActor.tell(new SearchMessages.SearchJob(CsvDocRefEntity.TYPE, searchRequest), ActorRef.noSender());

        // Then
        final SearchMessages.SearchJobComplete jobComplete = testProbe.expectMsgClass(SearchMessages.SearchJobComplete.class);
        LOGGER.info("Job Complete {}", jobComplete);
        assertNotNull(jobComplete.getResponse());
        assertNull(jobComplete.getError());

    }
}
