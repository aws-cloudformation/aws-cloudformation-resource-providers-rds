package software.amazon.rds.common.handler;

import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.amazonaws.util.CollectionUtils;
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
import software.amazon.rds.common.logging.RequestLogger;

public class Events {

    protected static final String EVENT_CATEGORY_NOTIFICATION = "notification";
    protected static final String EVENT_CATEGORY_MAINTENANCE = "maintenance";

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

    public static List<Event> fetchEvents(
            final ProxyClient<RdsClient> rdsProxyClient,
            final String sourceIdentifier,
            final SourceType sourceType,
            final String[] eventCategories,
            final Instant startTime
    ) {
        final DescribeEventsResponse response = rdsProxyClient.injectCredentialsAndInvokeV2(
                DescribeEventsRequest.builder()
                        .sourceType(sourceType)
                        .sourceIdentifier(sourceIdentifier)
                        .eventCategories(eventCategories)
                        .startTime(startTime)
                        .endTime(Instant.now())
                        .build(),
                rdsProxyClient.client()::describeEvents
        );
        return response.events();
    }

    public static <M, C> ProgressEvent<M, C> checkFailedEvents(
            final ProxyClient<RdsClient> rdsProxyClient,
            final String sourceIdentifier,
            final SourceType sourceType,
            final Instant startTime,
            final ProgressEvent<M, C> progress,
            final Predicate<Event> isFailureEvent,
            final RequestLogger logger
    ) {
        try {
            final List<Event> failures = fetchEvents(
                    rdsProxyClient,
                    sourceIdentifier,
                    sourceType,
                    new String[]{EVENT_CATEGORY_NOTIFICATION, EVENT_CATEGORY_MAINTENANCE},
                    startTime
            )
                    .stream()
                    .filter(isFailureEvent)
                    .collect(Collectors.toList());
            if (!CollectionUtils.isNullOrEmpty(failures)) {
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
}
