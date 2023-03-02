package software.amazon.rds.common.handler;

import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DescribeEventsRequest;
import software.amazon.awssdk.services.rds.model.DescribeEventsResponse;
import software.amazon.awssdk.services.rds.model.Event;
import software.amazon.awssdk.services.rds.model.SourceType;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.rds.common.error.ErrorCode;
import software.amazon.rds.common.error.ErrorRuleSet;
import software.amazon.rds.common.error.ErrorStatus;

public class Events {

    protected static final String EVENT_CATEGORY_NOTIFICATION = "notification";

    protected static final ErrorRuleSet DESCRIBE_EVENTS_ERROR_RULE_SET = ErrorRuleSet
            .extend(Commons.DEFAULT_ERROR_RULE_SET)
            .withErrorCodes(ErrorStatus.ignore(OperationStatus.IN_PROGRESS),
                    ErrorCode.AccessDenied,
                    ErrorCode.AccessDeniedException,
                    ErrorCode.NotAuthorized,
                    ErrorCode.UnauthorizedOperation)
            .build();

    public static boolean isEventMessageContains(
            final Event event,
            final String fragment
    ) {
        if (event != null) {
            final String msg = event.message();
            if (msg != null) {
                return msg.toLowerCase(Locale.getDefault())
                        .contains(fragment.toLowerCase(Locale.getDefault()));
            }
        }
        return false;
    }

    public static <M, C> List<Event> fetchEvents(
            final ProxyClient<RdsClient> rdsProxyClient,
            final String resourceIdentifier,
            final SourceType sourceType,
            final String eventType,
            final Instant fetchSince
    ) {
        final DescribeEventsResponse response = rdsProxyClient.injectCredentialsAndInvokeV2(
                describeEventsRequest(
                        sourceType,
                        resourceIdentifier,
                        Collections.singletonList(eventType),
                        fetchSince,
                        Instant.now()
                ),
                rdsProxyClient.client()::describeEvents
        );
        return response.events();
    }

    public static <M, C> ProgressEvent<M, C> checkFailedEvents(
            final ProxyClient<RdsClient> rdsClient,
            final Logger logger,
            final ProgressEvent<M, C> progress,
            final Instant fetchSince,
            final String resourceIdentifier,
            final SourceType sourceType,
            final Predicate<Event> isFailureEvent
    ) {
        try {
            final List<Event> failures = fetchEvents(rdsClient, resourceIdentifier, sourceType, EVENT_CATEGORY_NOTIFICATION, fetchSince)
                    .stream()
                    .filter(isFailureEvent)
                    .collect(Collectors.toList());
            if (!com.amazonaws.util.CollectionUtils.isNullOrEmpty(failures)) {
                return ProgressEvent.failed(
                        progress.getResourceModel(),
                        progress.getCallbackContext(),
                        HandlerErrorCode.GeneralServiceException,
                        failures.get(0).message()
                );
            }
        } catch (Exception e) {
            logger.log(String.format("Failed to fetch events: %s", e.getMessage()));
            return Commons.handleException(progress, e, DESCRIBE_EVENTS_ERROR_RULE_SET);
        }
        return progress;
    }

    private static DescribeEventsRequest describeEventsRequest(
            final SourceType sourceType,
            final String sourceIdentifier,
            final Collection<String> eventCategories,
            final Instant startTime,
            final Instant endTime
    ) {
        return DescribeEventsRequest.builder()
                .eventCategories(eventCategories.toArray(new String[0]))
                .sourceIdentifier(sourceIdentifier)
                .sourceType(sourceType)
                .startTime(startTime)
                .endTime(endTime)
                .build();
    }
}
