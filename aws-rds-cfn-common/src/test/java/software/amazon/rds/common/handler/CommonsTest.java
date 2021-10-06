package software.amazon.rds.common.handler;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.rds.common.error.ErrorCode;
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

    private AwsServiceException newAwsServiceException(final ErrorCode errorCode) {
        return AwsServiceException.builder()
                .awsErrorDetails(AwsErrorDetails.builder()
                        .errorCode(errorCode.toString())
                        .build()).build();
    }

}
