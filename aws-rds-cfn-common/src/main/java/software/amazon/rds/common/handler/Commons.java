package software.amazon.rds.common.handler;

import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.rds.common.error.ErrorCode;
import software.amazon.rds.common.error.ErrorRuleSet;
import software.amazon.rds.common.error.ErrorStatus;
import software.amazon.rds.common.error.HandlerErrorStatus;
import software.amazon.rds.common.error.IgnoreErrorStatus;

public final class Commons {

    private Commons() {
    }

    public static final ErrorRuleSet DEFAULT_ERROR_RULE_SET = ErrorRuleSet.builder()
            .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.ServiceInternalError),
                    ErrorCode.ClientUnavailable)
            .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.AccessDenied),
                    ErrorCode.AccessDeniedException,
                    ErrorCode.NotAuthorized)
            .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.Throttling),
                    ErrorCode.ThrottlingException)
            .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.InvalidRequest),
                    ErrorCode.InvalidParameterCombination,
                    ErrorCode.InvalidParameterValue,
                    ErrorCode.MissingParameter)
            .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.ServiceInternalError),
                    SdkClientException.class)
            .build();

    public static <M, C> ProgressEvent<M, C> handleException(
            final ProgressEvent<M, C> progress,
            final Exception exception,
            final ErrorRuleSet errorRuleSet
    ) {
        final M model = progress.getResourceModel();
        final C context = progress.getCallbackContext();

        final ErrorStatus errorStatus = errorRuleSet.handle(exception);

        if (errorStatus instanceof IgnoreErrorStatus) {
            return ProgressEvent.progress(model, context);
        } else if (errorStatus instanceof HandlerErrorStatus) {
            final HandlerErrorStatus handlerErrorStatus = (HandlerErrorStatus) errorStatus;
            return ProgressEvent.failed(model, context, handlerErrorStatus.getHandlerErrorCode(), exception.getMessage());
        }

        return ProgressEvent.failed(model, context, HandlerErrorCode.InternalFailure, exception.getMessage());
    }
}
