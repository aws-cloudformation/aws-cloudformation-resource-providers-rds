package software.amazon.rds.common.error;

import lombok.Getter;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.OperationStatus;

public class RetryErrorStatus implements ErrorStatus {
    @Getter
    private final OperationStatus status;

    @Getter
    private final int callbackDelay;

    @Getter
    HandlerErrorCode handlerErrorCode;

    public RetryErrorStatus(final OperationStatus status, int callbackDelay, final HandlerErrorCode handlerErrorCode) {
        this.status = status;
        this.callbackDelay = callbackDelay;
        this.handlerErrorCode = handlerErrorCode;
    }
}
