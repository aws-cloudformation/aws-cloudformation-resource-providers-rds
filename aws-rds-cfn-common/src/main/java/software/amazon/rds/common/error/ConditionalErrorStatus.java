package software.amazon.rds.common.error;

import java.util.function.Function;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public class ConditionalErrorStatus implements ErrorStatus {
    @Getter
    Function<Exception, ErrorStatus> condition;

    @Override
    public ErrorStatus interpret(final Exception exception) {
        return condition.apply(exception);
    }
}
