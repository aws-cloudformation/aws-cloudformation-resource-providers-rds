package software.amazon.rds.common.handler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;

import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import com.fasterxml.jackson.annotation.JsonProperty;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.resource.ResourceTypeSchema;
import software.amazon.rds.common.error.ErrorCode;
import software.amazon.rds.common.error.ErrorRuleSet;
import software.amazon.rds.common.error.ErrorStatus;
import software.amazon.rds.common.error.HandlerErrorStatus;
import software.amazon.rds.common.logging.RequestLogger;
import software.amazon.rds.common.printer.FilteredJsonPrinter;

import static org.mockito.Mockito.*;
import static software.amazon.rds.common.handler.Commons.getRoot_Cause_Exception_Message;

class CommonsTest {

    @Mock
    RequestLogger requestLogger = Mockito.mock(RequestLogger.class);
    @Test
    void test_handle_ClientUnavailable() {
        final ErrorStatus status = Commons.DEFAULT_ERROR_RULE_SET.handle(newAwsServiceException(ErrorCode.ClientUnavailable));
        assertThat(status).isInstanceOf(HandlerErrorStatus.class);
        assertThat(((HandlerErrorStatus) status).getHandlerErrorCode()).isEqualTo(HandlerErrorCode.ServiceInternalError);
    }

    @Test
    void test_handle_AccessDeniedException() {
        final ErrorStatus status = Commons.DEFAULT_ERROR_RULE_SET.handle(newAwsServiceException(ErrorCode.AccessDeniedException));
        assertThat(status).isInstanceOf(HandlerErrorStatus.class);
        assertThat(((HandlerErrorStatus) status).getHandlerErrorCode()).isEqualTo(HandlerErrorCode.AccessDenied);
    }

    @Test
    void test_handle_NotAuthorized() {
        final ErrorStatus status = Commons.DEFAULT_ERROR_RULE_SET.handle(newAwsServiceException(ErrorCode.NotAuthorized));
        assertThat(status).isInstanceOf(HandlerErrorStatus.class);
        assertThat(((HandlerErrorStatus) status).getHandlerErrorCode()).isEqualTo(HandlerErrorCode.AccessDenied);
    }

    @Test
    void test_handle_AlreadyExistsException() {
        final ErrorRuleSet errorRuleSet = ErrorRuleSet.extend(ErrorRuleSet.EMPTY_RULE_SET)
                .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.AlreadyExists), RuntimeException.class)
                .build();

        final ProgressEvent<Void, Void> progress = new ProgressEvent<>();
        Mockito.doNothing().when(requestLogger).log(any(String.class));
        final ProgressEvent<Void, Void> handledExceptionProgress = Commons.handleException(progress, new RuntimeException(), errorRuleSet, requestLogger);
        assertThat(handledExceptionProgress.getResourceModel()).isNull();
        assertThat(handledExceptionProgress.getCallbackContext()).isNull();
    }

    @Test
    void test_handle_ThrottlingException() {
        final ErrorStatus status = Commons.DEFAULT_ERROR_RULE_SET.handle(newAwsServiceException(ErrorCode.ThrottlingException));
        assertThat(status).isInstanceOf(HandlerErrorStatus.class);
        assertThat(((HandlerErrorStatus) status).getHandlerErrorCode()).isEqualTo(HandlerErrorCode.Throttling);
    }

    @Test
    void test_handle_InvalidParameterCombination() {
        final ErrorStatus status = Commons.DEFAULT_ERROR_RULE_SET.handle(newAwsServiceException(ErrorCode.InvalidParameterCombination));
        assertThat(status).isInstanceOf(HandlerErrorStatus.class);
        assertThat(((HandlerErrorStatus) status).getHandlerErrorCode()).isEqualTo(HandlerErrorCode.InvalidRequest);
    }

    @Test
    void test_handle_InvalidParameterValue() {
        final ErrorStatus status = Commons.DEFAULT_ERROR_RULE_SET.handle(newAwsServiceException(ErrorCode.InvalidParameterValue));
        assertThat(status).isInstanceOf(HandlerErrorStatus.class);
        assertThat(((HandlerErrorStatus) status).getHandlerErrorCode()).isEqualTo(HandlerErrorCode.InvalidRequest);
    }

    @Test
    void test_handle_MissingParameter() {
        final ErrorStatus status = Commons.DEFAULT_ERROR_RULE_SET.handle(newAwsServiceException(ErrorCode.MissingParameter));
        assertThat(status).isInstanceOf(HandlerErrorStatus.class);
        assertThat(((HandlerErrorStatus) status).getHandlerErrorCode()).isEqualTo(HandlerErrorCode.InvalidRequest);
    }

    @Test
    void test_handle_SdkClientException() {
        final ErrorStatus status = Commons.DEFAULT_ERROR_RULE_SET.handle(SdkClientException.builder().build());
        assertThat(status).isInstanceOf(HandlerErrorStatus.class);
        assertThat(((HandlerErrorStatus) status).getHandlerErrorCode()).isEqualTo(HandlerErrorCode.ServiceInternalError);
    }

    @Test
    void test_handle_SdkServiceException() {
        final ErrorStatus status = Commons.DEFAULT_ERROR_RULE_SET.handle(SdkServiceException.builder().build());
        assertThat(status).isInstanceOf(HandlerErrorStatus.class);
        assertThat(((HandlerErrorStatus) status).getHandlerErrorCode()).isEqualTo(HandlerErrorCode.ServiceInternalError);
    }

    @Test
    void test_handleException_Ignore() {
        final ProgressEvent<Void, Void> event = new ProgressEvent<>();
        final Exception exception = new RuntimeException("test exception");
        final ErrorRuleSet ruleSet = ErrorRuleSet.extend(ErrorRuleSet.EMPTY_RULE_SET)
                .withErrorClasses(ErrorStatus.ignore(), RuntimeException.class)
                .build();
        Mockito.doNothing().when(requestLogger).log(any(String.class));
        final ProgressEvent<Void, Void> resultEvent = Commons.handleException(event, exception, ruleSet, requestLogger);
        assertThat(resultEvent).isNotNull();
        assertThat(resultEvent.isSuccess()).isTrue();
    }

    @Test
    void test_handleException_HandlerError() {
        final ProgressEvent<Void, Void> event = new ProgressEvent<>();
        final Exception exception = new RuntimeException("test exception");
        final ErrorRuleSet ruleSet = ErrorRuleSet.extend(ErrorRuleSet.EMPTY_RULE_SET)
                .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.InvalidRequest), RuntimeException.class)
                .build();
        Mockito.doNothing().when(requestLogger).log(any(String.class));
        final ProgressEvent<Void, Void> resultEvent = Commons.handleException(event, exception, ruleSet, requestLogger);
        assertThat(resultEvent).isNotNull();
        assertThat(resultEvent.isFailed()).isTrue();
        assertThat(resultEvent.getErrorCode()).isEqualTo(HandlerErrorCode.InvalidRequest);
    }

    @Test
    void test_handleException_UnknownError() {
        final ProgressEvent<Void, Void> event = new ProgressEvent<>();
        final Exception exception = new RuntimeException("test exception");
        final ErrorRuleSet ruleSet = ErrorRuleSet.extend(ErrorRuleSet.EMPTY_RULE_SET).build();
        Mockito.doNothing().when(requestLogger).log(any(String.class));
        final ProgressEvent<Void, Void> resultEvent = Commons.handleException(event, exception, ruleSet, requestLogger);
        assertThat(resultEvent).isNotNull();
        assertThat(resultEvent.isFailed()).isTrue();
        assertThat(resultEvent.getErrorCode()).isEqualTo(HandlerErrorCode.InternalFailure);
    }

    @Test
    void test_RootCauseMessage_UnexpectedErrorStatus_Without_text()
    {
        final ProgressEvent<Void, Void> progressEvent = new ProgressEvent<>();
        final Exception ex = new RuntimeException();
        final Throwable exRootCause = ExceptionUtils.getRootCause(ex);
        final String exceptionClass = ex.getClass().getCanonicalName();
        final ErrorRuleSet ruleSet = ErrorRuleSet.extend(ErrorRuleSet.EMPTY_RULE_SET).build();
        Mockito.doAnswer(invocationOnMock
                ->{Object arg0 = invocationOnMock.getArgument(0);
        return null;}).when(requestLogger).log(any(String.class));

        final ProgressEvent <Void, Void> resultEvent = Commons.handleException(progressEvent, ex, ruleSet, requestLogger);

        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        verify(requestLogger).log(captor.capture());

        final String logLine = captor.getValue();
        assertThat(resultEvent).isNotNull();
        assertThat(resultEvent.isFailed()).isTrue();

        assertThat(logLine).contains("UnexpectedErrorStatus: " + getRoot_Cause_Exception_Message(exRootCause) + " " + exceptionClass);
    }

    @Test
    void test_handleException_when_logger_is_null()
    {
        final ProgressEvent<Void, Void> progressEvent = new ProgressEvent<>();
        final Exception ex = new RuntimeException();
        final ErrorRuleSet ruleSet = ErrorRuleSet.extend(ErrorRuleSet.EMPTY_RULE_SET).build();
        final ProgressEvent <Void, Void> resultEvent = Commons.handleException(progressEvent, ex, ruleSet, null);
        assertThat(resultEvent).isNotNull();
        assertThat(resultEvent.isFailed()).isTrue();
    }

    @Test
    void test_RootCauseMessage_UnexpectedErrorStatus_With_text()
    {
        final ProgressEvent<Void, Void> progressEvent = new ProgressEvent<>();
        final Exception ex = new RuntimeException("Runtime exception test");
        final String exceptionClass = ex.getClass().getCanonicalName();
        final ErrorRuleSet ruleSet = ErrorRuleSet.extend(ErrorRuleSet.EMPTY_RULE_SET).build();
        Mockito.doNothing().when(requestLogger).log(any(String.class));
        final ProgressEvent <Void, Void> resultEvent = Commons.handleException(progressEvent, ex, ruleSet, requestLogger);

        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        verify(requestLogger).log(captor.capture());

        final String logLine = captor.getValue();
        assertThat(resultEvent).isNotNull();
        assertThat(resultEvent.isFailed()).isTrue();

        assertThat(logLine).contains("UnexpectedErrorStatus: Runtime exception test" + " " + exceptionClass);
    }

    @Test
    void test_getExceptionClass()
    {
        final ProgressEvent<Void, Void> progressEvent = new ProgressEvent<>();
        final Exception ex = new RuntimeException();
        final String exceptionClass = ex.getClass().getCanonicalName();
        final ErrorRuleSet ruleSet = ErrorRuleSet.extend(ErrorRuleSet.EMPTY_RULE_SET).build();
        Mockito.doNothing().when(requestLogger).log(any(String.class));
        final ProgressEvent <Void, Void> resultEvent = Commons.handleException(progressEvent, ex, ruleSet, requestLogger);
        assertThat(resultEvent).isNotNull();
        assertThat(resultEvent.isFailed()).isTrue();
        assertThat(exceptionClass).isEqualTo("java.lang.RuntimeException");
    }

    @Test
    void test_UnexpectedErrorStatus_Returns_Internal_Failure()
    {
        final ProgressEvent<Void, Void> progressEvent = new ProgressEvent<>();
        final Exception ex = new RuntimeException("Runtime exception test");
        final ErrorRuleSet ruleSet = ErrorRuleSet.extend(ErrorRuleSet.EMPTY_RULE_SET).build();
        Mockito.doNothing().when(requestLogger).log(any(String.class));
        final ProgressEvent <Void, Void> resultEvent = Commons.handleException(progressEvent, ex, ruleSet, requestLogger);
        assertThat(resultEvent).isNotNull();
        assertThat(resultEvent.isFailed()).isTrue();
        assertThat(resultEvent.getErrorCode()).isEqualTo(HandlerErrorCode.InternalFailure);
    }

    @Test
    void test_execOnce_invoke() {
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
    void test_execOnce_skip() {
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

    private final static ResourceTypeSchema TEST_RESOURCE_TYPE_SCHEMA = ResourceTypeSchema.load(
            new JSONObject("{" +
                    "\"typeName\":\"AWS::Test::Type\"," +
                    "\"properties\":{" +
                    "\"TestProperty\":{\"type\":\"string\"}" +
                    "}," +
                    "\"description\":\"test resource schema\"," +
                    "\"primaryIdentifier\":[\"/properties/TestProperty\"]," +
                    "\"additionalProperties\":false," +
                    "}")
    );

    static class TestResourceModel {
        @JsonProperty(value = "TestProperty")
        private String testProperty;
    }

    @Test
    void test_detectDrift_shouldLogDriftedModel() {
        final TestResourceModel input = new TestResourceModel();
        input.testProperty = "test-property-init";

        final TestResourceModel output = new TestResourceModel();
        output.testProperty = "test-property-output";

        Logger logger = Mockito.mock(Logger.class);
        Mockito.doNothing().when(logger).log(any(String.class));

        Commons.reportResourceDrift(input, ProgressEvent.<TestResourceModel, Void>progress(output, null), TEST_RESOURCE_TYPE_SCHEMA, new RequestLogger(logger, new ResourceHandlerRequest<>(), new FilteredJsonPrinter()));
        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        verify(logger).log(captor.capture());

        final String logLine = captor.getValue();
        assertThat(logLine).contains("Resource drift detected");
    }

    @Test
    void test_detectDrift_shouldNotLogIfModelIsNotDrifted() {
        final TestResourceModel input = new TestResourceModel();
        input.testProperty = "test-property";

        final TestResourceModel output = new TestResourceModel();
        output.testProperty = "test-property";

        Logger logger = Mockito.mock(Logger.class);

        Commons.reportResourceDrift(input, ProgressEvent.<TestResourceModel, Void>progress(output, null), TEST_RESOURCE_TYPE_SCHEMA, new RequestLogger(logger, new ResourceHandlerRequest<>(), new FilteredJsonPrinter()));
        verifyNoInteractions(logger);
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
