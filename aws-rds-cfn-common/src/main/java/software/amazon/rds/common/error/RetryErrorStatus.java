package software.amazon.rds.common.error;

import lombok.Getter;
import software.amazon.cloudformation.proxy.OperationStatus;

public class RetryErrorStatus implements ErrorStatus {
    @Getter
    private final OperationStatus status;

    @Getter
    private final int callbackDelay;

    public RetryErrorStatus(final OperationStatus status, int callbackDelay) {
        this.status = status;
        this.callbackDelay = callbackDelay;
    }
}
