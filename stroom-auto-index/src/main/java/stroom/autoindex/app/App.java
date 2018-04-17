package stroom.autoindex.app;

import akka.actor.*;
import com.codahale.metrics.health.HealthCheck;
import com.google.inject.*;
import com.google.inject.name.Names;
import com.google.inject.util.Modules;
import io.dropwizard.Application;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.elasticsearch.client.transport.TransportClient;
import stroom.autoindex.AutoIndexConstants;
import stroom.autoindex.QueryClientCache;
import stroom.autoindex.akka.ManagedActorSystem;
import stroom.autoindex.indexing.*;
import stroom.autoindex.service.AutoIndexDocRefEntity;
import stroom.autoindex.service.AutoIndexDocRefServiceImpl;
import stroom.autoindex.service.AutoIndexQueryServiceImpl;
import stroom.query.audit.client.DocRefResourceHttpClient;
import stroom.query.audit.client.QueryResourceHttpClient;
import stroom.query.audit.rest.DocRefResource;
import stroom.query.audit.rest.QueryResource;
import stroom.query.audit.security.ServiceUser;
import stroom.query.elastic.transportClient.TransportClientBundle;
import stroom.query.jooq.AuditedJooqDocRefBundle;
import stroom.tracking.TimelineTrackerDao;
import stroom.tracking.TimelineTrackerDaoJooqImpl;
import stroom.tracking.TimelineTrackerService;
import stroom.tracking.TimelineTrackerServiceImpl;

import javax.inject.Named;
import java.util.Timer;

import static stroom.autoindex.AutoIndexConstants.INDEX_JOB_POST_HANDLER;
import static stroom.autoindex.AutoIndexConstants.TASK_HANDLER_NAME;

public class App extends Application<Config> {

    private ManagedActorSystem actorSystem;
    private Injector injector;

    private AuditedJooqDocRefBundle<Config,
            AutoIndexDocRefServiceImpl,
            AutoIndexDocRefEntity,
            AutoIndexQueryServiceImpl> auditedQueryBundle;

    private TransportClientBundle<Config> transportClientBundle = new TransportClientBundle<>();

    @Override
    public void run(final Config configuration,
                    final Environment environment) throws Exception {
        environment.healthChecks().register("Something", new HealthCheck() {
            @Override
            protected Result check() {
                return Result.healthy("Keeps Dropwizard Happy");
            }
        });

        environment.lifecycle().manage(actorSystem);

        if (configuration.getIndexingConfig().getEnabled()) {
            final Timer timer = new Timer();
            final IndexingTimerTask indexingTimerTask = injector.getInstance(IndexingTimerTask.class);
            timer.schedule(indexingTimerTask,
                    configuration.getIndexingConfig().getSecondsBetweenChecks() * 1000);
        }
    }

    private Module getGuiceModule(final Config configuration) {
        return Modules.combine(new AbstractModule() {
            @Override
            protected void configure() {
                bind(TimelineTrackerDao.class).to(TimelineTrackerDaoJooqImpl.class);
                bind(TimelineTrackerService.class).to(TimelineTrackerServiceImpl.class);
                bind(IndexJobDao.class).to(IndexJobDaoImpl.class);
                bind(new TypeLiteral<QueryClientCache<QueryResource>>(){})
                        .toInstance(new QueryClientCache<>(configuration, QueryResourceHttpClient::new));
                bind(new TypeLiteral<QueryClientCache<DocRefResource>>(){})
                        .toInstance(new QueryClientCache<>(configuration, DocRefResourceHttpClient::new));
                bind(IndexingConfig.class).toInstance(configuration.getIndexingConfig());
                bind(IndexWriter.class).to(IndexWriterImpl.class);
                bind(Config.class).toInstance(configuration);
                bind(ServiceUser.class).annotatedWith(Names.named(AutoIndexConstants.STROOM_SERVICE_USER))
                        .toInstance(new ServiceUser.Builder()
                                .name(configuration.getServiceUser().getName())
                                .jwt(configuration.getServiceUser().getJwt())
                                .build());
                bind(TransportClient.class).toInstance(transportClientBundle.getTransportClient());
                bind(ActorSystem.class).toInstance(actorSystem.getActorSystem());
            }

            @Provides
            @Named(INDEX_JOB_POST_HANDLER)
            public ActorRef indexJobPostHandler() {
                return actorSystem.getActorSystem().actorOf(Props.create(AbstractActor.class, () -> new AbstractActor() {
                    @Override
                    public Receive createReceive() {
                        return receiveBuilder()
                                .match(IndexJob.class, indexJob -> {
                                    // do nothing...for now
                                })
                                .build();
                    }
                }));
            }

            @Provides
            @Named(TASK_HANDLER_NAME)
            public ActorRef indexJobConsumerActorRef(final IndexJobConsumer jobHandler,
                                                     @Named(INDEX_JOB_POST_HANDLER)
                                                     final ActorRef postHandler) {
                return actorSystem.getActorSystem().actorOf(IndexJobActor.props(jobHandler, postHandler));
            }
        },
                auditedQueryBundle.getGuiceModule(configuration));
    }

    @Override
    public void initialize(final Bootstrap<Config> bootstrap) {
        super.initialize(bootstrap);

        actorSystem = new ManagedActorSystem();

        auditedQueryBundle =
                new AuditedJooqDocRefBundle<>(
                        (c) -> {
                            this.injector = Guice.createInjector(getGuiceModule(c));
                            return injector;
                        },
                        AutoIndexDocRefServiceImpl.class,
                        AutoIndexDocRefEntity.class,
                        AutoIndexQueryServiceImpl.class);

        // This allows us to use templating in the YAML configuration.
        bootstrap.setConfigurationSourceProvider(new SubstitutingSourceProvider(
                bootstrap.getConfigurationSourceProvider(),
                new EnvironmentVariableSubstitutor(false)));

        bootstrap.addBundle(this.transportClientBundle);
        bootstrap.addBundle(this.auditedQueryBundle);

    }
}
