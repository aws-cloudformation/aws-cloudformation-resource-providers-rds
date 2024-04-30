package software.amazon.rds.common.handler;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.function.Function;

import lombok.NonNull;
import org.apache.commons.lang3.exception.ExceptionUtils;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.services.rds.model.KmsKeyNotAccessibleException;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.resource.ResourceTypeSchema;
import software.amazon.rds.common.error.ErrorRuleSet;
import software.amazon.rds.common.error.ErrorStatus;
import software.amazon.rds.common.error.ErrorCode;
import software.amazon.rds.common.error.IgnoreErrorStatus;
import software.amazon.rds.common.error.RetryErrorStatus;
import software.amazon.rds.common.error.UnexpectedErrorStatus;
import software.amazon.rds.common.error.HandlerErrorStatus;
import software.amazon.rds.common.logging.RequestLogger;
import software.amazon.rds.common.util.DriftDetector;
import software.amazon.rds.common.util.DriftDetectorReport;
import software.amazon.rds.common.util.Mutation;
import javax.annotation.Nullable;


public final class Commons {


    public static final ErrorRuleSet DEFAULT_ERROR_RULE_SET = ErrorRuleSet.extend(ErrorRuleSet.EMPTY_RULE_SET)
            .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.ServiceInternalError),
                    ErrorCode.ClientUnavailable,
                    ErrorCode.InternalFailure)
            .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.AccessDenied),
                    ErrorCode.AccessDenied,
                    ErrorCode.AccessDeniedException,
                    ErrorCode.NotAuthorized,
                    ErrorCode.UnauthorizedOperation)
            .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.Throttling),
                    ErrorCode.ThrottlingException,
                    ErrorCode.Throttling)
            .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.InvalidRequest),
                    ErrorCode.InvalidParameterCombination,
                    ErrorCode.InvalidParameterValue,
                    ErrorCode.MissingParameter)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.AccessDenied),
                    KmsKeyNotAccessibleException.class)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.ServiceInternalError),
                    SdkServiceException.class,
                    SdkClientException.class)
            .build();

    private Commons() {
    }

    public static final int STATUS_MESSAGE_MAXIMUM_LENGTH = 100;
    /* The metric name limit is 255 as per CloudWatch documentation.
    Setting this as 100 will ensure it will capture as much of the error message without making
    the metric name too large.
    */

    public static <M, C> ProgressEvent<M, C> handleException(
            final ProgressEvent<M, C> progress,
            final Exception exception,
            final ErrorRuleSet errorRuleSet,
            final RequestLogger requestLogger
    ) {
        final M model = progress.getResourceModel();
        final C context = progress.getCallbackContext();
        final ErrorStatus errorStatus = errorRuleSet.handle(exception);
        final String exceptionMessage = getRootCauseExceptionMessage(exception);
        final String exceptionClass = exception.getClass().getCanonicalName();
        final StringBuilder messageBuilder = new StringBuilder();

        if (errorStatus instanceof IgnoreErrorStatus) {
            switch (((IgnoreErrorStatus) errorStatus).getStatus()) {
                case IN_PROGRESS:
                    return ProgressEvent.progress(model, context);
                default:
                    return ProgressEvent.success(model, context);
            }
        } else if (errorStatus instanceof RetryErrorStatus) {
            RetryErrorStatus retryErrorStatus = (RetryErrorStatus) errorStatus;
            return ProgressEvent.defaultInProgressHandler(context, retryErrorStatus.getCallbackDelay(), model);
        } else if (errorStatus instanceof HandlerErrorStatus) {
            final HandlerErrorStatus handlerErrorStatus = (HandlerErrorStatus) errorStatus;
            // We need to set model and context to null in case of AlreadyExists errors
            // If we not set model and context to null, CFN will attempt to perform delete of the resource it has attempted to create,
            // even if resource belongs to a different stack altogether, or was created out of bounds
            if (handlerErrorStatus.getHandlerErrorCode() == HandlerErrorCode.AlreadyExists) {
                return ProgressEvent.failed(null, null, handlerErrorStatus.getHandlerErrorCode(), exception.getMessage());
            }
            return ProgressEvent.failed(model, context, handlerErrorStatus.getHandlerErrorCode(), exception.getMessage());
        } if (errorStatus instanceof UnexpectedErrorStatus && requestLogger != null) {
            messageBuilder.append(exceptionClass).append("\n")
                    .append(exceptionMessage).append("\n")
                    .append(ExceptionUtils.getStackTrace(exception));
            requestLogger.log("UnexpectedErrorStatus", messageBuilder.toString());
        }
        return ProgressEvent.failed(model, context, HandlerErrorCode.InternalFailure, exception.getMessage());
    }

    public static <M, C> ProgressEvent<M, C> execOnce(
            final ProgressEvent<M, C> progress,
            final ProgressEventLambda<M, C> func,
            final Function<C, Boolean> conditionGetter,
            final VoidBiFunction<C, Boolean> conditionSetter
    ) {
        if (!conditionGetter.apply(progress.getCallbackContext())) {
            return func.enact().then(p -> {
                conditionSetter.apply(p.getCallbackContext(), true);
                return p;
            });
        }
        return progress;
    }

    public static Instant parseTimestamp(@NonNull String timestamp) {
        return ZonedDateTime.parse(timestamp).toInstant();
    }

    public static <M, C> ProgressEvent<M, C> reportResourceDrift(
            final M inputModel,
            final ProgressEvent<M, C> progress,
            final ResourceTypeSchema schema,
            final RequestLogger logger
    ) {
        try {
            final DriftDetector driftDetector = new DriftDetector(schema);
            final Map<String, Mutation> mutations = driftDetector.detectDrift(inputModel, progress.getResourceModel());
            if (!mutations.isEmpty()) {
                logger.log("Resource drift detected", new DriftDetectorReport(mutations));
            }
        } catch (Exception e) {
            logger.log("Drift detector internal error", e);
        }
        return progress;
    }

    @Nullable
    public static String getRootCauseExceptionMessage(final Throwable t)
    {
        if(t == null){
            return null;
        }
        String statusMessage = t.getMessage();
        if(statusMessage != null) {
            statusMessage = statusMessage.substring(0, Math.min(statusMessage.length(), STATUS_MESSAGE_MAXIMUM_LENGTH));
        }
        return statusMessage;
    }
}
