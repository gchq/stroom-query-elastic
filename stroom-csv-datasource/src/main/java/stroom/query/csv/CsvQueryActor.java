package stroom.query.csv;

import akka.actor.AbstractActor;
import akka.actor.Actor;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.Creator;
import stroom.query.api.v2.SearchRequest;
import stroom.query.audit.service.DocRefService;
import stroom.security.ServiceUser;

import java.util.Optional;

public class CsvQueryActor extends AbstractActor {
    private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    public static Props props(final ServiceUser user,
                              final DocRefService docRefService,
                              final CsvFieldSupplier fieldSupplier) {
        return Props.create(new Creator<Actor>() {
            @Override
            public Actor create() throws Exception {
                return new CsvQueryActor(user, docRefService, fieldSupplier);
            }
        });
    }

    private final ServiceUser user;
    private final DocRefService docRefService;
    private final CsvFieldSupplier fieldSupplier;

    public CsvQueryActor(final ServiceUser user,
                         final DocRefService docRefService,
                         final CsvFieldSupplier fieldSupplier) {
        this.user = user;
        this.docRefService = docRefService;
        this.fieldSupplier = fieldSupplier;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(SearchRequest.class, (request) -> {
                    final String dataSourceUuid = request.getQuery().getDataSource().getUuid();

                    final Optional<CsvDocRefEntity> docRefEntity = docRefService.get(user, dataSourceUuid);

                    if (!docRefEntity.isPresent()) {
                        return;
                    }

                    log.info("Searching for Animal Sightings in Doc Ref {}", docRefEntity);
                })
                .build();
    }
}
