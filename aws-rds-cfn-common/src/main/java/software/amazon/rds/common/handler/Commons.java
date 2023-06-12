package software.amazon.rds.common.handler;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.function.Function;

import lombok.NonNull;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.services.rds.model.KmsKeyNotAccessibleException;
import software.amazon.awssdk.utils.StringUtils;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.rds.common.error.ErrorCode;
import software.amazon.rds.common.error.ErrorRuleSet;
import software.amazon.rds.common.error.ErrorStatus;
import software.amazon.rds.common.error.HandlerErrorStatus;
import software.amazon.rds.common.error.IgnoreErrorStatus;

public final class Commons {
    public static final String NOT_AUTHORIZED_TO_PERFORM_RDS_ADD_TAGS_TO_RESOURCE = "is not authorized to perform: rds:AddTagsToResource";

    protected static final Function<Exception, ErrorStatus> isUnauthorizedTaggingOperation = exception -> {
        if (isUnauthorizedTaggingExceptionMessage(exception.getMessage())) {
            return ErrorStatus.failWith(HandlerErrorCode.UnauthorizedTaggingOperation);
        }
        return ErrorStatus.failWith(HandlerErrorCode.AccessDenied);
    };

    private static boolean isUnauthorizedTaggingExceptionMessage(final String message) {
        if (StringUtils.isBlank(message)) {
            return false;
        }
        return message.contains(NOT_AUTHORIZED_TO_PERFORM_RDS_ADD_TAGS_TO_RESOURCE);
    }

    public static final ErrorRuleSet DEFAULT_ERROR_RULE_SET = ErrorRuleSet.extend(ErrorRuleSet.EMPTY_RULE_SET)
            .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.ServiceInternalError),
                    ErrorCode.ClientUnavailable,
                    ErrorCode.InternalFailure)
            .withErrorCodes(ErrorStatus.conditional(isUnauthorizedTaggingOperation),
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

    public static <M, C> ProgressEvent<M, C> handleException(
            final ProgressEvent<M, C> progress,
            final Exception exception,
            final ErrorRuleSet errorRuleSet
    ) {
        final M model = progress.getResourceModel();
        final C context = progress.getCallbackContext();

        final ErrorStatus errorStatus = errorRuleSet.handle(exception);

        if (errorStatus instanceof IgnoreErrorStatus) {
            switch (((IgnoreErrorStatus) errorStatus).getStatus()) {
                case IN_PROGRESS:
                    return ProgressEvent.progress(model, context);
                default:
                    return ProgressEvent.success(model, context);
            }
        } else if (errorStatus instanceof HandlerErrorStatus) {
            final HandlerErrorStatus handlerErrorStatus = (HandlerErrorStatus) errorStatus;
            // We need to set model and context to null in case of AlreadyExists errors
            // If we not set model and context to null, CFN will attempt to perform delete of the resource it has attempted to create,
            // even if resource belongs to a different stack altogether, or was created out of bounds
            if (handlerErrorStatus.getHandlerErrorCode() == HandlerErrorCode.AlreadyExists) {
                return ProgressEvent.failed(null, null, handlerErrorStatus.getHandlerErrorCode(), exception.getMessage());
            }
            return ProgressEvent.failed(model, context, handlerErrorStatus.getHandlerErrorCode(), exception.getMessage());
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
}
