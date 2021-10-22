package software.amazon.rds.common.error;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;

import org.junit.jupiter.api.Test;

import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.cloudformation.proxy.HandlerErrorCode;

class ErrorRuleSetTest {

    @Test
    void builder_withErrorClasses() {
        final ErrorRuleSet ruleSet = ErrorRuleSet.builder()
                .withErrorClasses(Collections.singletonList(RuntimeException.class), ErrorStatus.ignore())
                .build();
        assertThat(ruleSet.handle(new RuntimeException())).isInstanceOf(IgnoreErrorStatus.class);
    }

    @Test
    void builder_withErrorCodes() {
        final ErrorCode errorCode = ErrorCode.AccessDeniedException;
        final ErrorRuleSet ruleSet = ErrorRuleSet.builder()
                .withErrorCodes(Collections.singletonList(errorCode), ErrorStatus.ignore())
                .build();
        final AwsServiceException exception = AwsServiceException.builder()
                .awsErrorDetails(AwsErrorDetails.builder()
                        .errorCode(errorCode.toString())
                        .build()
                ).build();
        assertThat(ruleSet.handle(exception)).isInstanceOf(IgnoreErrorStatus.class);
    }

    @Test
    void builder_unexpectedException() {
        final ErrorCode errorCode = ErrorCode.AccessDeniedException;
        final ErrorRuleSet ruleSet = ErrorRuleSet.builder().build();
        final AwsServiceException exception = AwsServiceException.builder()
                .awsErrorDetails(AwsErrorDetails.builder()
                        .errorCode(errorCode.toString())
                        .build()
                ).build();
        assertThat(ruleSet.handle(exception)).isInstanceOf(UnexpectedErrorStatus.class);
    }

    @Test
    void or_nonOverlappingRules() {
        final ErrorCode errorCode = ErrorCode.AccessDeniedException;
        final ErrorRuleSet errorRuleSet1 = ErrorRuleSet.builder()
                .withErrorClasses(Collections.singletonList(RuntimeException.class), ErrorStatus.ignore())
                .build();
        final ErrorRuleSet errorRuleSet2 = ErrorRuleSet.builder()
                .withErrorCodes(Collections.singletonList(errorCode), ErrorStatus.ignore())
                .build();
        final ErrorRuleSet errorRuleSetOr = errorRuleSet1.orElse(errorRuleSet2);

        final RuntimeException exception1 = new RuntimeException();
        final AwsServiceException exception2 = AwsServiceException.builder()
                .awsErrorDetails(AwsErrorDetails.builder()
                        .errorCode(errorCode.toString())
                        .build()
                ).build();
        final Exception exception3 = new Exception();

        assertThat(errorRuleSetOr.handle(exception1)).isInstanceOf(IgnoreErrorStatus.class);
        assertThat(errorRuleSetOr.handle(exception2)).isInstanceOf(IgnoreErrorStatus.class);
        assertThat(errorRuleSetOr.handle(exception3)).isInstanceOf(UnexpectedErrorStatus.class);
    }

    @Test
    void or_overlappingRules_Classes() {
        final ErrorRuleSet errorRuleSet1 = ErrorRuleSet.builder()
                .withErrorClasses(Collections.singletonList(RuntimeException.class), ErrorStatus.ignore())
                .build();
        final ErrorRuleSet errorRuleSet2 = ErrorRuleSet.builder()
                .withErrorClasses(Collections.singletonList(RuntimeException.class), ErrorStatus.failWith(HandlerErrorCode.AccessDenied))
                .build();

        final RuntimeException exception = new RuntimeException();

        final ErrorRuleSet errorRuleSetOrDirect = errorRuleSet1.orElse(errorRuleSet2);
        final ErrorRuleSet errorRuleSetOrReverse = errorRuleSet2.orElse(errorRuleSet1);

        assertThat(errorRuleSetOrDirect.handle(exception)).isInstanceOf(IgnoreErrorStatus.class);
        assertThat(errorRuleSetOrReverse.handle(exception)).isInstanceOf(HandlerErrorStatus.class);
    }

    @Test
    void or_overlappingRules_Codes() {
        final ErrorCode errorCode = ErrorCode.AccessDeniedException;
        final ErrorRuleSet errorRuleSet1 = ErrorRuleSet.builder()
                .withErrorCodes(Collections.singletonList(errorCode), ErrorStatus.ignore())
                .build();
        final ErrorRuleSet errorRuleSet2 = ErrorRuleSet.builder()
                .withErrorCodes(Collections.singletonList(errorCode), ErrorStatus.failWith(HandlerErrorCode.AccessDenied))
                .build();
        final ErrorRuleSet errorRuleSetOrDirect = errorRuleSet1.orElse(errorRuleSet2);
        final ErrorRuleSet errorRuleSetOrReverse = errorRuleSet2.orElse(errorRuleSet1);

        final AwsServiceException exception = AwsServiceException.builder()
                .awsErrorDetails(AwsErrorDetails.builder()
                        .errorCode(errorCode.toString())
                        .build()
                ).build();

        assertThat(errorRuleSetOrDirect.handle(exception)).isInstanceOf(IgnoreErrorStatus.class);
        assertThat(errorRuleSetOrReverse.handle(exception)).isInstanceOf(HandlerErrorStatus.class);
    }
}
