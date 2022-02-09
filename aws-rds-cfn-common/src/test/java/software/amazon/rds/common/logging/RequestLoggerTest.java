package software.amazon.rds.common.logging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.printer.FilteredJsonPrinter;

public class RequestLoggerTest {

    public static final String AWS_ACCOUNT_ID = "123456789";
    public static final String TOKEN = "token";
    public static final String STACK_ID = "stackId";
    public static final String SIMPLE_LOG = "simple log ";
    final Logger logger = m -> {
        assertThat(m.contains(AWS_ACCOUNT_ID)).isTrue();
        assertThat(m.contains(TOKEN)).isTrue();
        assertThat(m.contains(STACK_ID)).isTrue();
    };

    @Test
    void test_if_customer_data_is_added() {
        ResourceHandlerRequest<Void> request = new ResourceHandlerRequest<>();
        request.setAwsAccountId(AWS_ACCOUNT_ID);
        request.setClientRequestToken(TOKEN);
        request.setStackId(STACK_ID);
        RequestLogger requestLogger = new RequestLogger(logger, request, new FilteredJsonPrinter());
        requestLogger.log(SIMPLE_LOG, new Object());
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
        RequestLogger requestLogger = new RequestLogger(s -> assertThat(s.contains("Exception")).isTrue(), request, new FilteredJsonPrinter());
        try {
            requestLogger.logAndThrow(throwable);
            fail("Should throw");
        } catch (Throwable throwable1) {
            //Nothing
        }
    }

    @Test
    void test_handle_request() {
        ResourceHandlerRequest<Void> request = new ResourceHandlerRequest<>();
        request.setAwsAccountId(AWS_ACCOUNT_ID);
        request.setClientRequestToken(TOKEN);
        request.setStackId(STACK_ID);
        RequestLogger.handleRequest(logger, request, new FilteredJsonPrinter(), requestLogger -> null);
    }
}
