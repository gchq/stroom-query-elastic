package stroom.autoindex.search;

import akka.actor.AbstractActor;
import akka.actor.Actor;
import akka.actor.Props;
import akka.japi.Creator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.autoindex.service.AutoIndexDocRefEntity;
import stroom.query.api.v2.DocRef;
import stroom.query.api.v2.SearchRequest;
import stroom.query.audit.client.NotFoundException;
import stroom.query.audit.security.ServiceUser;
import stroom.query.audit.service.DocRefService;
import stroom.query.audit.service.QueryApiException;
import stroom.tracking.TimelineTracker;
import stroom.tracking.TimelineTrackerService;

import java.util.concurrent.CompletableFuture;

import static akka.pattern.PatternsCS.pipe;

public class SearchRequestSplitterActor extends AbstractActor {

    private static final Logger LOGGER = LoggerFactory.getLogger(SearchRequestSplitterActor.class);

    public static final Props props(final DocRefService docRefService,
                                    final TimelineTrackerService trackerService) {
        return Props.create(new Creator<Actor>() {
            @Override
            public Actor create() throws Exception {
                return new SearchRequestSplitterActor(docRefService, trackerService);
            }
        });
    }

    private final DocRefService<AutoIndexDocRefEntity> docRefService;

    private final TimelineTrackerService trackerService;

    public SearchRequestSplitterActor(final DocRefService docRefService,
                                      final TimelineTrackerService trackerService) {
        this.docRefService = docRefService;
        this.trackerService = trackerService;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(SearchMessages.SearchJob.class, searchJob -> {
                    final SearchRequest searchRequest = searchJob.getRequest();
                    final ServiceUser user = searchJob.getUser();

                    final CompletableFuture<SearchMessages.SplitSearchJobComplete> result =
                            CompletableFuture.supplyAsync(() -> {
                                try {
                                    // Retrieve the full Auto Index Doc Ref for the request
                                    final String docRefUuid = searchRequest.getQuery().getDataSource().getUuid();
                                    final AutoIndexDocRefEntity docRefEntity =
                                            docRefService.get(user, docRefUuid).orElseThrow(NotFoundException::new);

                                    // Retrieve the tracker for this doc ref
                                    final TimelineTracker tracker = trackerService.get(docRefUuid);

                                    final SplitSearchRequest splitSearchRequest = SearchRequestSplitter.withSearchRequest(searchRequest)
                                            .autoIndex(docRefEntity)
                                            .tracker(tracker)
                                            .split();

                                    return SearchMessages.splitComplete(user,
                                            searchRequest,
                                            new DocRef.Builder()
                                                    .uuid(docRefUuid)
                                                    .type(docRefEntity.getType())
                                                    .name(docRefEntity.getName())
                                                    .build(),
                                            splitSearchRequest);
                                } catch (QueryApiException e) {
                                    LOGGER.error("Failed to run search", e);
                                    throw new RuntimeException(e);
                                } catch (RuntimeException e) {
                                    LOGGER.error("Failed to run search", e);
                                    throw e;
                                }
                            })
                                    .exceptionally(e -> SearchMessages.splitFailed(user, searchRequest, e));

                    pipe(result, getContext().dispatcher()).to(getSender());
                })
                .build();
    }
}
