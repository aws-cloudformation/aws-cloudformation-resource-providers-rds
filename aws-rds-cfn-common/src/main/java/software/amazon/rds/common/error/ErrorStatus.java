package software.amazon.rds.common.error;

import java.util.function.Function;

import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.OperationStatus;

public interface ErrorStatus {

    static ErrorStatus failWith(HandlerErrorCode errorCode) {
        return new HandlerErrorStatus(errorCode);
    }

    static ErrorStatus ignore() {
        return ignore(OperationStatus.SUCCESS);
    }

    static ErrorStatus ignore(final OperationStatus status) {
        return new IgnoreErrorStatus(status);
    }

    static ErrorStatus conditional(Function<Exception, ErrorStatus> condition) {
        return new ConditionalErrorStatus(condition);
    }

    default ErrorStatus interpret(final Exception exception) {
        return this;
    }
}
