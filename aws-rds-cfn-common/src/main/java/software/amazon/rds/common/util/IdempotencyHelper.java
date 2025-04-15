package software.amazon.rds.common.util;

import java.util.function.Function;
import java.util.function.UnaryOperator;

import com.google.common.annotations.VisibleForTesting;
import software.amazon.cloudformation.exceptions.CfnAlreadyExistsException;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.rds.common.logging.RequestLogger;

/**
 * Utility class containing functions to handle non-idempotent APIs.
 */
public final class IdempotencyHelper {

    private static boolean bypass = false;

    @VisibleForTesting
    public static void setBypass(boolean bypass) {
        IdempotencyHelper.bypass = bypass;
    }

    /**
     * Wraps a non-idempotent create operation to prevent spurious "already exists" errors.
     *
     * @param checkExistenceFunction a function that checks if the resource exists and returns the resource if it does,
     *                               or if it does not, then null or a CfnNotFoundException.
     * @param createFunction         the non-idempotent create operation
     * @param resourceTypeName       the resource type name (used to form the error message)
     * @param resourceIdentifier     the resource identifier (used to form the error message)
     */
    public static <ResourceT, CallbackT extends PreExistenceContext> ProgressEvent<ResourceT, CallbackT> safeCreate(
        final Function<ResourceT, ?> checkExistenceFunction,
        final UnaryOperator<ProgressEvent<ResourceT, CallbackT>> createFunction,
        final String resourceTypeName,
        final String resourceIdentifier,
        final ProgressEvent<ResourceT, CallbackT> progress,
        final RequestLogger requestLogger
    ) {
        // The approach recommended by CloudFormation is as follows:
        // - First, check whether the requested resource already exists. If it does, then fail immediately. Otherwise,
        //   return IN_PROGRESS with a non-zero callback delay. This forces the handler to return back to CFN, which
        //   will persist the status so that the pre-existence check is not repeated in case the next step fails.
        // - Then, perform the create operation. If it returns an AlreadyExists error, then ignore it.

        if (bypass) {
            return createFunction.apply(progress);
        }

        final var preExistenceCheckDone = progress.getCallbackContext().getPreExistenceCheckDone();
        if (preExistenceCheckDone == null || !preExistenceCheckDone) {
            try {
                final var existingResource = checkExistenceFunction.apply(progress.getResourceModel());
                if (existingResource != null) {
                    requestLogger.log("Resource already exists");
                    throw new CfnAlreadyExistsException(resourceTypeName, resourceIdentifier);
                }
            } catch (final CfnNotFoundException ignored) {
                requestLogger.log("CfnNotFoundException thrown during pre-existence check (all good)");
            }

            progress.getCallbackContext().setPreExistenceCheckDone(true);

            // !!!: The callbackDelaySeconds of 1 is important here. (See canContinueProgress in ProgressEvent)
            return ProgressEvent.defaultInProgressHandler(progress.getCallbackContext(), 1,
                progress.getResourceModel());
        } else {
            final var result = createFunction.apply(progress);
            if (result.isFailed() && result.getErrorCode() == HandlerErrorCode.AlreadyExists) {
                requestLogger.log("Ignoring AlreadyExists error from create operation");
                return ProgressEvent.defaultInProgressHandler(progress.getCallbackContext(), 0,
                    progress.getResourceModel());
            } else {
                return result;
            }
        }
    }

    public interface PreExistenceContext {
        Boolean getPreExistenceCheckDone();

        void setPreExistenceCheckDone(Boolean preExistenceCheckDone);
    }

}
