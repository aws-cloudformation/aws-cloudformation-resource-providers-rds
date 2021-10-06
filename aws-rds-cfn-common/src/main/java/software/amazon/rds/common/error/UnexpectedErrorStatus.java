package software.amazon.rds.common.error;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public class UnexpectedErrorStatus implements ErrorStatus {
    @Getter
    Exception exception;
}
