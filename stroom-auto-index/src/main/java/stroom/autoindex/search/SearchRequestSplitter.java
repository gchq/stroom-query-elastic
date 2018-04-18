package stroom.autoindex.search;

import stroom.autoindex.service.AutoIndexDocRefEntity;
import stroom.query.api.v2.*;
import stroom.tracking.TimelineTracker;
import stroom.tracking.TrackerInverter;

/**
 * Given a search request and a tracker, generates two split search requests to give
 * to the raw and indexed data sources. The generated requests will contain
 * the same expression terms as the input, wrapped with an AND that adds the constraints
 * defined in the tracker windows.
 */
public class SearchRequestSplitter {
    private final SearchRequest searchRequest;
    private AutoIndexDocRefEntity autoIndexDocRefEntity;
    private TimelineTracker tracker;

    public static SearchRequestSplitter withSearchRequest(final SearchRequest searchRequest) {
        return new SearchRequestSplitter(searchRequest);
    }

    private SearchRequestSplitter(final SearchRequest searchRequest) {
        this.searchRequest = searchRequest;
    }

    public SearchRequestSplitter autoIndex(final AutoIndexDocRefEntity value) {
        this.autoIndexDocRefEntity = value;
        return this;
    }

    public SearchRequestSplitter tracker(final TimelineTracker value) {
        this.tracker = value;
        return this;
    }

    public SplitSearchRequest split() {
        final SplitSearchRequest.Builder splitSearchRequestBuilder = SplitSearchRequest.start();

        split(autoIndexDocRefEntity.getIndexDocRef(),
                tracker,
                splitSearchRequestBuilder);

        // Only query the raw doc ref if we can generate an inverted timeline
        try {
            final TimelineTracker invertedTracking = TrackerInverter.withTracker(tracker).invert();
            split(autoIndexDocRefEntity.getRawDocRef(),
                    invertedTracking,
                    splitSearchRequestBuilder);
        } catch (final RuntimeException e) {
            // do nothing
        }

        return splitSearchRequestBuilder.build();
    }

    private void split(final DocRef docRef,
                       final TimelineTracker tracker,
                       final SplitSearchRequest.Builder splitSearchRequestBuilder) {
        // Get access to the input query
        final Query inputQuery = searchRequest.getQuery();

        tracker.getWindows().forEach(trackerWindow -> {
            final String windowValue = String.format("%d,%d",
                    trackerWindow.getFrom(),
                    trackerWindow.getTo());

            // Create a query for the raw data source
            final ExpressionOperator timelineBoundOperator = new ExpressionOperator.Builder()
                    .addOperator(inputQuery.getExpression())
                    .addTerm(autoIndexDocRefEntity.getTimeFieldName(), ExpressionTerm.Condition.BETWEEN, windowValue)
                    .build();

            // Build the new query
            final Query.Builder rawQueryBuilder = new Query.Builder()
                    .expression(timelineBoundOperator);
            inputQuery.getParams().forEach(rawQueryBuilder::addParams);
            rawQueryBuilder.dataSource(docRef);

            // Build the new search request
            final SearchRequest.Builder partSearchRequestBuilder = new SearchRequest.Builder()
                    .dateTimeLocale(searchRequest.getDateTimeLocale())
                    .incremental(searchRequest.incremental())
                    .key(searchRequest.getKey())
                    .timeout(searchRequest.getTimeout())
                    .query(rawQueryBuilder.build());

            // The same results will be requested from all searches
            searchRequest.getResultRequests().forEach(partSearchRequestBuilder::addResultRequests);

            // The search request is ready to add to the split request
            splitSearchRequestBuilder.withRequest(docRef, trackerWindow, partSearchRequestBuilder.build());
        });
    }
}
