package software.amazon.rds.common.logging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.printer.FilteredJsonPrinter;

@ExtendWith(MockitoExtension.class)
public class RequestLoggerTest {

    @Captor
    ArgumentCaptor<String> captor;

    @Mock
    private Logger logger;

    public static final String AWS_ACCOUNT_ID = "123456789";
    public static final String TOKEN = "token";
    public static final String STACK_ID = "stackId";
    public static final String SIMPLE_LOG = "simple log ";

    @Test
    void test_if_customer_data_is_added() {
        ResourceHandlerRequest<Void> request = new ResourceHandlerRequest<>();
        request.setAwsAccountId(AWS_ACCOUNT_ID);
        request.setClientRequestToken(TOKEN);
        request.setStackId(STACK_ID);
        RequestLogger requestLogger = new RequestLogger(logger, request, new FilteredJsonPrinter());
        requestLogger.log(SIMPLE_LOG, new Object());
        verify(logger, atLeast(1)).log(captor.capture());
        assertThat(captor.getValue().contains(STACK_ID)).isTrue();
    }

    @Test
    void test_message_detail_log() {
        final String logMessage = "title";
        final String detailedLogMessage = "Detailed log message";
        ResourceHandlerRequest<Void> request = new ResourceHandlerRequest<>();
        request.setAwsAccountId(AWS_ACCOUNT_ID);
        request.setClientRequestToken(TOKEN);
        request.setStackId(STACK_ID);
        RequestLogger requestLogger = new RequestLogger(logger, request, new FilteredJsonPrinter());
        requestLogger.log(logMessage, detailedLogMessage);
        verify(logger, atLeast(1)).log(captor.capture());
        final String resultLogMessage = captor.getValue();
        System.out.println(resultLogMessage);
        assertThat(resultLogMessage.contains(STACK_ID)).isTrue();
        assertThat(resultLogMessage.contains(logMessage)).isTrue();
        assertThat(resultLogMessage.contains(detailedLogMessage)).isTrue();
    }

    @Test
    void test_fail_if_request_null() {
        assertThatThrownBy(() -> new RequestLogger(s -> {
        }, null, new FilteredJsonPrinter()))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void test_pass_if_logger_null() {
        ResourceHandlerRequest<Void> request = new ResourceHandlerRequest<>();
        request.setAwsAccountId(AWS_ACCOUNT_ID);
        request.setClientRequestToken(TOKEN);
        request.setStackId(STACK_ID);
        try {
            RequestLogger requestLogger = new RequestLogger(null, request, new FilteredJsonPrinter());
            requestLogger.log(SIMPLE_LOG, request);
        } catch (Throwable throwable) {
            fail("Should not throw exception");
        }
    }

    @Test
    void test_log_and_throw() {
        Throwable throwable = new Throwable("This is Exception");
        ResourceHandlerRequest<Void> request = new ResourceHandlerRequest<>();
        request.setStackId(STACK_ID);
        RequestLogger requestLogger = new RequestLogger(logger, request, new FilteredJsonPrinter());
        try {
            requestLogger.logAndThrow(throwable);
            fail("Should throw");
        } catch (Throwable throwable1) {
            verify(logger, atLeast(1)).log(captor.capture());
            assertThat(captor.getValue().contains(STACK_ID)).isTrue();
        }
    }

    @Test
    void test_handle_request() {
        ResourceHandlerRequest<Void> request = new ResourceHandlerRequest<>();
        request.setAwsAccountId(AWS_ACCOUNT_ID);
        request.setClientRequestToken(TOKEN);
        request.setStackId(STACK_ID);
        RequestLogger.handleRequest(logger, request, new FilteredJsonPrinter(), requestLogger -> null);
        verify(logger, atLeast(1)).log(captor.capture());
        assertThat(captor.getValue().contains(AWS_ACCOUNT_ID)).isTrue();
    }
}
