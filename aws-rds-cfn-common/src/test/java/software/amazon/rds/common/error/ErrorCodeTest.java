package software.amazon.rds.common.error;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Random;

import org.junit.jupiter.api.Test;

import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;

class ErrorCodeTest {

    @Test
    void fromString_ExistingErrorCode() {
        for (final ErrorCode errorCode : ErrorCode.values()) {
            final String errorCodeStr = errorCode.toString();
            final ErrorCode fromString = ErrorCode.fromString(errorCodeStr);
            assertThat(errorCode).isEqualTo(fromString);
        }
    }

    @Test
    void fromString_NonExistingErrorCode() {
        final ErrorCode nonExisting = ErrorCode.fromString("test-non-existing");
        assertThat(nonExisting).isNull();
    }

    @Test
    void fromString_NullString() {
        final ErrorCode nullCode = ErrorCode.fromString(null);
        assertThat(nullCode).isNull();
    }

    @Test
    void testEquals() {
        for (final ErrorCode errorCode : ErrorCode.values()) {
            final String errorCodeStr = errorCode.toString();
            assertThat(errorCodeStr).isNotNull();
            assertThat(errorCode.equals(errorCodeStr)).isTrue();
        }
    }

    @Test
    void testEquals_notEqual() {
        final ErrorCode errorCode = ErrorCode.values()[new Random().nextInt(ErrorCode.values().length)];
        assertThat(errorCode.equals("test-non-existing")).isFalse();
    }

    @Test
    void fromException() {
        final AwsServiceException exception = AwsServiceException.builder()
                .awsErrorDetails(AwsErrorDetails.builder()
                        .errorCode(ErrorCode.InternalFailure.toString())
                        .build())
                .build();
        final ErrorCode errorCode = ErrorCode.fromException(exception);
        assertThat(errorCode).isEqualTo(ErrorCode.InternalFailure);
    }

    @Test
    void fromException_EmptyDetails() {
        final AwsServiceException exception = AwsServiceException.builder().build();
        final ErrorCode errorCode = ErrorCode.fromException(exception);
        assertThat(errorCode).isNull();
    }

    @Test
    void fromException_UnknownErrorCode() {
        final AwsServiceException exception = AwsServiceException.builder()
                .awsErrorDetails(AwsErrorDetails.builder()
                        .errorCode("unknown-code")
                        .build())
                .build();
        final ErrorCode errorCode = ErrorCode.fromException(exception);
        assertThat(errorCode).isNull();
    }
}
