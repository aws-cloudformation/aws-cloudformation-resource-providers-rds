package software.amazon.rds.common.handler;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.rds.common.error.ErrorCode;
import software.amazon.rds.common.error.ErrorRuleSet;
import software.amazon.rds.common.error.ErrorStatus;
import software.amazon.rds.common.error.HandlerErrorStatus;

public class CommonsTest {

    @Test
    public void handle_ClientUnavailable() {
        final ErrorStatus status = Commons.DEFAULT_ERROR_RULE_SET.handle(newAwsServiceException(ErrorCode.ClientUnavailable));
        assertThat(status).isInstanceOf(HandlerErrorStatus.class);
        assertThat(((HandlerErrorStatus) status).getHandlerErrorCode()).isEqualTo(HandlerErrorCode.ServiceInternalError);
    }

    @Test
    public void handle_AccessDeniedException() {
        final ErrorStatus status = Commons.DEFAULT_ERROR_RULE_SET.handle(newAwsServiceException(ErrorCode.AccessDeniedException));
        assertThat(status).isInstanceOf(HandlerErrorStatus.class);
        assertThat(((HandlerErrorStatus) status).getHandlerErrorCode()).isEqualTo(HandlerErrorCode.AccessDenied);
    }

    @Test
    public void handle_NotAuthorized() {
        final ErrorStatus status = Commons.DEFAULT_ERROR_RULE_SET.handle(newAwsServiceException(ErrorCode.NotAuthorized));
        assertThat(status).isInstanceOf(HandlerErrorStatus.class);
        assertThat(((HandlerErrorStatus) status).getHandlerErrorCode()).isEqualTo(HandlerErrorCode.AccessDenied);
    }

    @Test
    public void handle_ThrottlingException() {
        final ErrorStatus status = Commons.DEFAULT_ERROR_RULE_SET.handle(newAwsServiceException(ErrorCode.ThrottlingException));
        assertThat(status).isInstanceOf(HandlerErrorStatus.class);
        assertThat(((HandlerErrorStatus) status).getHandlerErrorCode()).isEqualTo(HandlerErrorCode.Throttling);
    }

    @Test
    public void handle_InvalidParameterCombination() {
        final ErrorStatus status = Commons.DEFAULT_ERROR_RULE_SET.handle(newAwsServiceException(ErrorCode.InvalidParameterCombination));
        assertThat(status).isInstanceOf(HandlerErrorStatus.class);
        assertThat(((HandlerErrorStatus) status).getHandlerErrorCode()).isEqualTo(HandlerErrorCode.InvalidRequest);
    }

    @Test
    public void handle_InvalidParameterValue() {
        final ErrorStatus status = Commons.DEFAULT_ERROR_RULE_SET.handle(newAwsServiceException(ErrorCode.InvalidParameterValue));
        assertThat(status).isInstanceOf(HandlerErrorStatus.class);
        assertThat(((HandlerErrorStatus) status).getHandlerErrorCode()).isEqualTo(HandlerErrorCode.InvalidRequest);
    }

    @Test
    public void handle_MissingParameter() {
        final ErrorStatus status = Commons.DEFAULT_ERROR_RULE_SET.handle(newAwsServiceException(ErrorCode.MissingParameter));
        assertThat(status).isInstanceOf(HandlerErrorStatus.class);
        assertThat(((HandlerErrorStatus) status).getHandlerErrorCode()).isEqualTo(HandlerErrorCode.InvalidRequest);
    }

    @Test
    public void handle_SdkClientException() {
        final ErrorStatus status = Commons.DEFAULT_ERROR_RULE_SET.handle(SdkClientException.builder().build());
        assertThat(status).isInstanceOf(HandlerErrorStatus.class);
        assertThat(((HandlerErrorStatus) status).getHandlerErrorCode()).isEqualTo(HandlerErrorCode.ServiceInternalError);
    }

    @Test
    public void handleException_Ignore() {
        final ProgressEvent<Void, Void> event = new ProgressEvent<>();
        final Exception exception = new RuntimeException("test exception");
        final ErrorRuleSet ruleSet = ErrorRuleSet.extend(ErrorRuleSet.EMPTY_RULE_SET)
                .withErrorClasses(ErrorStatus.ignore(), RuntimeException.class)
                .build();
        final ProgressEvent<Void, Void> resultEvent = Commons.handleException(event, exception, ruleSet);
        assertThat(resultEvent).isNotNull();
        assertThat(resultEvent.isSuccess()).isTrue();
    }

    @Test
    public void handleException_HandlerError() {
        final ProgressEvent<Void, Void> event = new ProgressEvent<>();
        final Exception exception = new RuntimeException("test exception");
        final ErrorRuleSet ruleSet = ErrorRuleSet.extend(ErrorRuleSet.EMPTY_RULE_SET)
                .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.InvalidRequest), RuntimeException.class)
                .build();
        final ProgressEvent<Void, Void> resultEvent = Commons.handleException(event, exception, ruleSet);
        assertThat(resultEvent).isNotNull();
        assertThat(resultEvent.isFailed()).isTrue();
        assertThat(resultEvent.getErrorCode()).isEqualTo(HandlerErrorCode.InvalidRequest);
    }

    @Test
    public void handleException_UnknownError() {
        final ProgressEvent<Void, Void> event = new ProgressEvent<>();
        final Exception exception = new RuntimeException("test exception");
        final ErrorRuleSet ruleSet = ErrorRuleSet.extend(ErrorRuleSet.EMPTY_RULE_SET).build();
        final ProgressEvent<Void, Void> resultEvent = Commons.handleException(event, exception, ruleSet);
        assertThat(resultEvent).isNotNull();
        assertThat(resultEvent.isFailed()).isTrue();
        assertThat(resultEvent.getErrorCode()).isEqualTo(HandlerErrorCode.InternalFailure);
    }

    @Test
    public void execOnce_invoke() {
        AtomicReference<Boolean> flag = new AtomicReference<>(false);
        AtomicReference<Boolean> invokedOnce = new AtomicReference<>(false);
        ProgressEvent<Void, Void> progress = ProgressEvent.progress(null, null);
        Commons.execOnce(
                progress,
                new ProgressEventLambda<Void, Void>() {
                    @Override
                    public ProgressEvent<Void, Void> enact() {
                        invokedOnce.set(true);
                        return progress;
                    }
                },
                c -> flag.get(),
                (c, v) -> flag.set(v));
        assertThat(flag.get()).isTrue();
        assertThat(invokedOnce.get()).isTrue();
    }

    @Test
    public void execOnce_skip() {
        AtomicReference<Boolean> invokedOnce = new AtomicReference<>(false);
        ProgressEvent<Void, Void> progress = ProgressEvent.progress(null, null);
        Commons.execOnce(
                progress,
                new ProgressEventLambda<Void, Void>() {
                    @Override
                    public ProgressEvent<Void, Void> enact() {
                        invokedOnce.set(true);
                        return progress;
                    }
                },
                c -> true,
                (c, v) -> {
                });
        assertThat(invokedOnce.get()).isFalse();
    }

    private AwsServiceException newAwsServiceException(final ErrorCode errorCode) {
        return AwsServiceException.builder()
                .awsErrorDetails(AwsErrorDetails.builder()
                        .errorCode(errorCode.toString())
                        .build()).build();
    }

    static class TaggingCallbackContext implements TaggingContext.Provider {

        private final TaggingContext taggingContext;

        public TaggingCallbackContext() {
            this.taggingContext = new TaggingContext();
        }

        @Override
        public TaggingContext getTaggingContext() {
            return this.taggingContext;
        }
    }
}
