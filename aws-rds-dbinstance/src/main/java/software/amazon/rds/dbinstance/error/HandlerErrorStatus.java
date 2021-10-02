package software.amazon.rds.dbinstance.error;

import lombok.AllArgsConstructor;
import lombok.Getter;
import software.amazon.cloudformation.proxy.HandlerErrorCode;

@AllArgsConstructor
public class HandlerErrorStatus implements ErrorStatus {
    @Getter
    HandlerErrorCode handlerErrorCode;
}
