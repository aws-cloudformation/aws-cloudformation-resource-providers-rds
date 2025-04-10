package software.amazon.rds.common.validation;

import com.google.common.collect.ImmutableMap;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.rds.common.error.ErrorStatus;
import software.amazon.rds.common.error.HandlerErrorStatus;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.logging.RequestLogger;

import java.util.function.Supplier;

public class ValidationUtils {
    private final static String MISSING_PERMISSION = "MissingPermission";
    public static <T> T fetchResourceForValidation(Supplier<T> supplier, String requiredPermission) throws ValidationAccessException {
        try {
            return supplier.get();
        } catch (Exception ex) {
            final ErrorStatus error = Commons.ACCESS_DENIED_RULE_SET.handle(ex);
            if (error instanceof HandlerErrorStatus &&
                    ((HandlerErrorStatus)error).getHandlerErrorCode() == HandlerErrorCode.AccessDenied ){
                throw new ValidationAccessException(requiredPermission);
            }
            throw ex;
        }
    }

    public static void emitMetric(final RequestLogger logger, final String validationMetric, ValidationAccessException ex) {
        logger.log(validationMetric, ImmutableMap.of(MISSING_PERMISSION, ex.getMessage()));
    }
}
