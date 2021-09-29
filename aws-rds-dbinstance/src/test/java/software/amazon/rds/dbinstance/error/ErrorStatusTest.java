package software.amazon.rds.dbinstance.error;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import software.amazon.cloudformation.proxy.HandlerErrorCode;

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
}
