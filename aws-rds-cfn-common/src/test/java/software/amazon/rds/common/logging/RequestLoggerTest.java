package software.amazon.rds.common.logging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

import org.junit.jupiter.api.Test;

import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class RequestLoggerTest {

    public static final String AWS_ACCOUNT_ID = "123456789";
    public static final String TOKEN = "token";
    public static final String STACK_ID = "stackId";
    public static final String SIMPLE_LOG = "simple log ";

    @Test
    void testIfCustomerDataIsAdded() {
        ResourceHandlerRequest<Void> request = new ResourceHandlerRequest<>();
        request.setAwsAccountId(AWS_ACCOUNT_ID);
        request.setClientRequestToken(TOKEN);
        request.setStackId(STACK_ID);
        RequestLogger requestLogger = new RequestLogger(new Logger() {
            @Override
            public void log(final String s) {
                assertThat(s.contains(AWS_ACCOUNT_ID)).isTrue();
                assertThat(s.contains(TOKEN)).isTrue();
                assertThat(s.contains(STACK_ID)).isTrue();
                assertThat(s.contains(SIMPLE_LOG)).isTrue();
            }
        }, request, parameterName -> true);
        requestLogger.log(SIMPLE_LOG, new Object());
    }

    @Test
    void testFailIfRequestNull() {
        assertThatThrownBy(() -> new RequestLogger(s -> {
        }, null, parameterName -> true))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testPassIfLoggerNull() {
        ResourceHandlerRequest<Void> request = new ResourceHandlerRequest<>();
        request.setAwsAccountId(AWS_ACCOUNT_ID);
        request.setClientRequestToken(TOKEN);
        request.setStackId(STACK_ID);
        try {
            RequestLogger requestLogger = new RequestLogger(null, request, parameterName -> true);
            requestLogger.log(SIMPLE_LOG, request);
        } catch (Throwable throwable) {
            fail("Should not throw exception");
        }
    }

    @Test
    void testLogAndThrow() {
        Throwable throwable = new Throwable("This is Exception");
        ResourceHandlerRequest<Void> request = new ResourceHandlerRequest<>();
        RequestLogger requestLogger = new RequestLogger(s -> assertThat(s.contains("Exception")).isTrue(), request, parameterName -> true);
        try {
            requestLogger.logAndThrow(throwable);
            fail("Should throw");
        } catch (Throwable throwable1) {
            //Nothing
        }
    }
}
