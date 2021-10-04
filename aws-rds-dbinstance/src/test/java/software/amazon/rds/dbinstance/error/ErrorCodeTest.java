package software.amazon.rds.dbinstance.error;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Random;

import org.junit.jupiter.api.Test;

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
}
