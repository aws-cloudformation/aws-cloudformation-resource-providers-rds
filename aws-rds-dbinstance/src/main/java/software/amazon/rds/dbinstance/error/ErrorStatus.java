package software.amazon.rds.dbinstance.error;

import software.amazon.cloudformation.proxy.HandlerErrorCode;

public interface ErrorStatus {

    static ErrorStatus failWith(HandlerErrorCode errorCode) {
        return new HandlerErrorStatus(errorCode);
    }

    static ErrorStatus ignore() {
        return new IgnoreErrorStatus();
    }
}
