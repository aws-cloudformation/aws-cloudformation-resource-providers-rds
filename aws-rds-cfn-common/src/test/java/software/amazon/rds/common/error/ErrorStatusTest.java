package software.amazon.rds.common.error;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.OperationStatus;

class ErrorStatusTest {

    @Test
    void failWith() {
        final HandlerErrorCode errorCode = HandlerErrorCode.AccessDenied;
        final ErrorStatus errorStatus = ErrorStatus.failWith(errorCode);
        assertThat(errorStatus).isInstanceOf(HandlerErrorStatus.class);
        assertThat(((HandlerErrorStatus)errorStatus).handlerErrorCode).isEqualTo(errorCode);
    }

    @Test
    void ignore() {
        final ErrorStatus errorStatus = ErrorStatus.ignore();
        assertThat(errorStatus).isInstanceOf(IgnoreErrorStatus.class);
    }

    @Test
    void retry() {
        final ErrorStatus errorStatus = ErrorStatus.retry(3);
        assertThat(errorStatus).isInstanceOf(RetryErrorStatus.class);
        assertThat(((RetryErrorStatus)errorStatus).getCallbackDelay()).isEqualTo(3);
        assertThat(((RetryErrorStatus)errorStatus).getStatus()).isEqualTo(OperationStatus.IN_PROGRESS);
    }


    @Test
    void conditional() {
        final ErrorStatus errorStatus = ErrorStatus.conditional(e -> {
            if (e instanceof AwsServiceException) {
                return ErrorStatus.ignore();
            }
            return ErrorStatus.failWith(HandlerErrorCode.Unknown);
        });
        assertThat(errorStatus.interpret(AwsServiceException.builder().build())).isInstanceOf(IgnoreErrorStatus.class);
        assertThat(errorStatus.interpret(new RuntimeException())).isInstanceOf(HandlerErrorStatus.class);
    }
}
