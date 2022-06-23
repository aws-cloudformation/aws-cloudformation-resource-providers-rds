package software.amazon.rds.common.error;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.OperationStatus;

class ErrorRuleSetTest {

    @Test
    void builder_withErrorClasses() {
        final ErrorRuleSet ruleSet = ErrorRuleSet.extend(ErrorRuleSet.EMPTY_RULE_SET)
                .withErrorClasses(ErrorStatus.ignore(), RuntimeException.class)
                .build();
        assertThat(ruleSet.handle(new RuntimeException())).isInstanceOf(IgnoreErrorStatus.class);
    }

    @Test
    void builder_withErrorCodes() {
        final ErrorCode errorCode = ErrorCode.AccessDeniedException;
        final ErrorRuleSet ruleSet = ErrorRuleSet.extend(ErrorRuleSet.EMPTY_RULE_SET)
                .withErrorCodes(ErrorStatus.ignore(), errorCode)
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
        final ErrorRuleSet ruleSet = ErrorRuleSet.extend(ErrorRuleSet.EMPTY_RULE_SET).build();
        final AwsServiceException exception = AwsServiceException.builder()
                .awsErrorDetails(AwsErrorDetails.builder()
                        .errorCode(errorCode.toString())
                        .build()
                ).build();
        assertThat(ruleSet.handle(exception)).isInstanceOf(UnexpectedErrorStatus.class);
    }

    private AwsServiceException newAwsServiceException(final ErrorCode errorCode) {
        return AwsServiceException.builder()
                .awsErrorDetails(AwsErrorDetails.builder()
                        .errorCode(errorCode.toString())
                        .build())
                .build();
    }

    @Test
    void testExtendWithErrorRuleSetWithEmptyBase() {
        final ErrorRuleSet base = ErrorRuleSet.extend(ErrorRuleSet.EMPTY_RULE_SET)
                .withErrorCodes(ErrorStatus.ignore(OperationStatus.PENDING), ErrorCode.InternalFailure)
                .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.InternalFailure), RuntimeException.class)
                .build();
        final ErrorRuleSet extension = ErrorRuleSet.extend(ErrorRuleSet.EMPTY_RULE_SET)
                // extension definition has a higher precedence in the propagation chain
                .withErrorClasses(ErrorStatus.ignore(OperationStatus.PENDING), RuntimeException.class)
                .build();
        final ErrorRuleSet extended = base.extendWith(extension);

        final Map<Exception, ErrorStatus> expected = ImmutableMap.of(
                new RuntimeException(), new IgnoreErrorStatus(OperationStatus.PENDING),
                newAwsServiceException(ErrorCode.InternalFailure), new IgnoreErrorStatus(OperationStatus.PENDING),
                new Exception(), new UnexpectedErrorStatus(new Exception())
        );

        for (final Map.Entry<Exception, ErrorStatus> entry : expected.entrySet()) {
            final ErrorStatus observedStatus = extended.handle(entry.getKey());
            assertEquivalentErrorStatuses(entry.getValue(), observedStatus);
        }
    }

    @Test
    void testExtendWithErrorRuleSetWithNonEmptyBase() {
        final ErrorRuleSet base0 = ErrorRuleSet.extend(ErrorRuleSet.EMPTY_RULE_SET)
                .withErrorCodes(ErrorStatus.ignore(), ErrorCode.Throttling)
                .build();
        final ErrorRuleSet base1 = ErrorRuleSet.extend(ErrorRuleSet.EMPTY_RULE_SET)
                .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.Throttling), ErrorCode.Throttling)
                .build();
        final ErrorRuleSet extension = ErrorRuleSet.extend(ErrorRuleSet.EMPTY_RULE_SET)
                .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.ResourceConflict), ErrorCode.DBInstanceAlreadyExists)
                .build();
        final ErrorRuleSet extended1 = base1.extendWith(extension);
        final ErrorRuleSet extended0 = base0.extendWith(extended1);

        final Map<Exception, ErrorStatus> expected = ImmutableMap.of(
                newAwsServiceException(ErrorCode.DBInstanceAlreadyExists), new HandlerErrorStatus(HandlerErrorCode.ResourceConflict),
                newAwsServiceException(ErrorCode.Throttling), new HandlerErrorStatus(HandlerErrorCode.Throttling),
                newAwsServiceException(ErrorCode.AccessDenied), new UnexpectedErrorStatus(new Exception())
        );

        for (final Map.Entry<Exception, ErrorStatus> entry : expected.entrySet()) {
            final ErrorStatus observedStatus = extended0.handle(entry.getKey());
            assertEquivalentErrorStatuses(entry.getValue(), observedStatus);
        }
    }

    private void assertEquivalentErrorStatuses(final ErrorStatus expected, final ErrorStatus observed) {
        assertThat(observed).hasSameClassAs(expected);
        if (expected instanceof UnexpectedErrorStatus) {
            assertThat(observed).isInstanceOf(UnexpectedErrorStatus.class);
        } else if (expected instanceof IgnoreErrorStatus) {
            assertThat(((IgnoreErrorStatus) expected).getStatus()).isEqualTo(((IgnoreErrorStatus) observed).getStatus());
        } else if (expected instanceof HandlerErrorStatus) {
            assertThat(((HandlerErrorStatus) expected).getHandlerErrorCode()).isEqualTo(((HandlerErrorStatus) observed).getHandlerErrorCode());
        }
    }
}
