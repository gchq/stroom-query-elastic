package stroom.autoindex.search;

import org.junit.Test;
import stroom.autoindex.service.AutoIndexDocRefEntity;
import stroom.query.api.v2.*;
import stroom.tracking.TimelineTracker;
import stroom.tracking.TrackerWindow;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class SearchRequestSplitterTest {
    static final String NAME_FIELD_NAME = "name";
    static final String TIMELINE_FIELD_NAME = "streamId";
    static final String TIMESTAMP_FIELD_NAME = "timestamp";

    @Test
    public void testSimpleSplit() {
        // Given
        final Long timelineWindow = 100L; // size of the window when future jobs are created
        final Long windowsInTest = 10L; // number of windows we want for a whole timeline
        final Long timelineStart = 1000L; // The start value
        final Long filledInWindows = 3L; // The number of windows we will have filled in already (from the end)
        final Long timelineEnd = timelineStart + (windowsInTest * timelineWindow); // Calculated end value
        final Long filledInStart = timelineEnd - (filledInWindows * timelineWindow); // Start value of filled in window

        final DocRef autoIndexDocRef = new DocRef.Builder()
                .type(AutoIndexDocRefEntity.TYPE)
                .uuid(UUID.randomUUID().toString())
                .build();
        final AutoIndexDocRefEntity autoIndexDocRefEntity =
                new AutoIndexDocRefEntity.Builder()
                        .docRef(autoIndexDocRef)
                        .indexDocRef(new DocRef.Builder()
                                .type(UUID.randomUUID().toString())
                                .uuid(UUID.randomUUID().toString())
                                .build())
                        .rawDocRef(new DocRef.Builder()
                                .type(UUID.randomUUID().toString())
                                .uuid(UUID.randomUUID().toString())
                                .build())
                        .timeFieldName(TIMELINE_FIELD_NAME)
                        .indexWindow(100L)
                        .build();
        final TimelineTracker timelineTracker = TimelineTracker.forDocRef(autoIndexDocRefEntity.getUuid())
                .withBounds(TrackerWindow.from(timelineStart).to(timelineEnd))
                .withWindow(TrackerWindow.from(filledInStart).to(timelineEnd));
        final SearchRequest searchRequest = getTestSearchRequest(autoIndexDocRef);

        // When
        final SplitSearchRequest splitSearchRequest =
                SearchRequestSplitter.withSearchRequest(searchRequest)
                        .autoIndex(autoIndexDocRefEntity)
                        .tracker(timelineTracker)
                        .split();

        // Then
        assertSplitPart(searchRequest,
                splitSearchRequest,
                TrackerWindow.from(timelineStart).to(filledInStart),
                autoIndexDocRefEntity.getRawDocRef());
        assertSplitPart(searchRequest,
                splitSearchRequest,
                TrackerWindow.from(filledInStart).to(timelineEnd),
                autoIndexDocRefEntity.getIndexDocRef());
    }

    static void assertSplitPart(final SearchRequest searchRequest,
                                final SplitSearchRequest splitSearchRequest,
                                final TrackerWindow trackerWindow,
                                final DocRef docRef) {
        final Map<TrackerWindow, SearchRequest> rawSearchRequests = Objects.requireNonNull(
                splitSearchRequest.getRequests().get(docRef));

        final SearchRequest rawSearchRequest = Objects.requireNonNull(rawSearchRequests.get(trackerWindow));
        assertCommonSearchRequestParts(searchRequest, rawSearchRequest);
        assertEquals(docRef, rawSearchRequest.getQuery().getDataSource());

        final ExpressionOperator rawExpression = rawSearchRequest.getQuery().getExpression();
        final ExpressionTerm timelineExpression = extractTimelineExpressionTerm(rawExpression, trackerWindow);
        final ExpressionItem usersExpression = extractOtherExpressionTerm(rawExpression, timelineExpression);

        assertEquals(searchRequest.getQuery().getExpression(), usersExpression);
    }

    static ExpressionTerm extractTimelineExpressionTerm(final ExpressionOperator operator,
                                                        final TrackerWindow trackerWindow) {
        assertEquals(ExpressionOperator.Op.AND, operator.getOp());
        return operator.getChildren().stream()
                .filter(eItem -> eItem instanceof ExpressionTerm)
                .map(eItem -> (ExpressionTerm) eItem)
                .filter(eTerm -> TIMELINE_FIELD_NAME.equals(eTerm.getField()))
                .filter(eTerm -> ExpressionTerm.Condition.BETWEEN.equals(eTerm.getCondition()))
                .filter(eTerm -> eTerm.getValue().contains(trackerWindow.getFrom().toString()) &&
                        eTerm.getValue().contains(trackerWindow.getTo().toString()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Could not find timeline expression in query"));
    }

    static ExpressionItem extractOtherExpressionTerm(final ExpressionOperator operator,
                                                     final ExpressionTerm timelineTerm) {
        assertEquals(2, operator.getChildren().size());
        return operator.getChildren().stream()
                .filter(eItem -> !eItem.equals(timelineTerm))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Could not find the other expression item in query"));
    }

    static void assertCommonSearchRequestParts(final SearchRequest searchRequest,
                                               final SearchRequest other) {
        assertEquals(searchRequest.getKey(), other.getKey());
        assertEquals(searchRequest.getDateTimeLocale(), other.getDateTimeLocale());
        assertEquals(searchRequest.getTimeout(), other.getTimeout());
        assertEquals(searchRequest.getResultRequests(), other.getResultRequests());
        assertEquals(searchRequest.getIncremental(), other.getIncremental());

        assertEquals(searchRequest.getQuery().getParams(), other.getQuery().getParams());
    }

    static SearchRequest getTestSearchRequest(final DocRef docRef) {
        final OffsetRange offset = new OffsetRange.Builder()
                .length(100L)
                .offset(0L)
                .build();
        final String testName = "alpha";
        final LocalDateTime testMaxDate = LocalDateTime.of(2017, 1, 1, 0, 0, 0);
        final ExpressionOperator expressionOperator = new ExpressionOperator.Builder(ExpressionOperator.Op.AND)
                .addTerm(NAME_FIELD_NAME, ExpressionTerm.Condition.CONTAINS, testName)
                .addTerm(TIMESTAMP_FIELD_NAME,
                        ExpressionTerm.Condition.LESS_THAN,
                        testMaxDate.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                )
                .build();

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
                        .requestedRange(offset)
                        .addMappings(new TableSettings.Builder()
                                .queryId(queryKey)
                                .extractValues(false)
                                .showDetail(false)
                                .addFields(new Field.Builder()
                                        .name(NAME_FIELD_NAME)
                                        .expression("${" + NAME_FIELD_NAME + "}")
                                        .build())
                                .addFields(new Field.Builder()
                                        .name(TIMELINE_FIELD_NAME)
                                        .expression("${" + TIMELINE_FIELD_NAME + "}")
                                        .build())
                                .addFields(new Field.Builder()
                                        .name(TIMESTAMP_FIELD_NAME)
                                        .expression("${" + TIMESTAMP_FIELD_NAME + "}")
                                        .build())
                                .addMaxResults(1000)
                                .build())
                        .build())
                .build();
    }
}
