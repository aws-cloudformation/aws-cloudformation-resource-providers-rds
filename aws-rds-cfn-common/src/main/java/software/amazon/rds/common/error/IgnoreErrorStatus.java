package software.amazon.rds.common.error;

import lombok.Getter;
import software.amazon.cloudformation.proxy.OperationStatus;

public class IgnoreErrorStatus implements ErrorStatus {
    @Getter
    private final OperationStatus status;

    public IgnoreErrorStatus(final OperationStatus status) {
        this.status = status;
    }
}
