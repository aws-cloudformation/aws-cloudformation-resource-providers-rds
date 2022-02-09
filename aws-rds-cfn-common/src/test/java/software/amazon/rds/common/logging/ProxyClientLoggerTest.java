package software.amazon.rds.common.logging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import software.amazon.awssdk.awscore.AwsRequest;
import software.amazon.awssdk.awscore.AwsResponse;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.pagination.sync.SdkIterable;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.ProxyClientTestBase;
import software.amazon.rds.common.printer.FilteredJsonPrinter;

@ExtendWith(MockitoExtension.class)
class ProxyClientLoggerTest extends ProxyClientTestBase {
    public static final String AWS_ACCOUNT_ID = "123456789";
    public static final String TOKEN = "token";
    public static final String STACK_ID = "stackId";

    @Mock
    private ProxyClient<RdsClient> proxy;

    @Mock
    private AwsRequest awsRequest;

    @Mock
    private AwsResponse awsResponse;

    @Mock
    private SdkIterable<AwsResponse> SdkIterable;

    @Mock
    private CompletableFuture<AwsResponse> awsResponseCompletableFuture;

    private ResourceHandlerRequest<Void> request;
    private Logger logger;

    @BeforeEach
    public void setup() {
        request = new ResourceHandlerRequest<>();
        request.setAwsAccountId(AWS_ACCOUNT_ID);
        request.setClientRequestToken(TOKEN);
        request.setStackId(STACK_ID);
    }

    private ProxyClient<RdsClient> getProxyRdsClient(Logger logger) {
        return ProxyClientLogger.newProxy(new RequestLogger(logger, request, new FilteredJsonPrinter()), proxy);
    }

    @Test
    void test_injectCredentialsAndInvokeV2() {
        logger = m -> assertThat(m.contains(AWS_ACCOUNT_ID)).isTrue();
        ProxyClient<RdsClient> proxyRdsClient = getProxyRdsClient(logger);
        when(proxy.injectCredentialsAndInvokeV2(any(), any())).thenReturn(awsResponse);
        proxyRdsClient.injectCredentialsAndInvokeV2(awsRequest, request -> awsResponse);
    }

    @Test
    void test_injectCredentialsAndInvokeV2Bytes() {
        logger = m -> assertThat(m.contains(TOKEN)).isTrue();
        ProxyClient<RdsClient> proxyRdsClient = getProxyRdsClient(logger);
        ResponseBytes<AwsResponse> response = ResponseBytes.fromByteArray(awsResponse, awsResponse.toString().getBytes());
        when(proxy.injectCredentialsAndInvokeV2Bytes(any(), any())).thenReturn(response);
        proxyRdsClient.injectCredentialsAndInvokeV2Bytes(awsRequest, request -> response);
    }

    @Test
    void test_injectCredentialsAndInvokeIterableV2() {
        logger = m -> assertThat(m.contains(STACK_ID)).isTrue();
        ProxyClient<RdsClient> proxyRdsClient = getProxyRdsClient(logger);
        when(proxy.injectCredentialsAndInvokeIterableV2(any(), any())).thenReturn(SdkIterable);
        proxyRdsClient.injectCredentialsAndInvokeIterableV2(awsRequest, request -> SdkIterable);
    }

    @Test
    void test_injectCredentialsAndInvokeV2Async() {
        logger = m -> assertThat(m.contains(STACK_ID)).isTrue();
        ProxyClient<RdsClient> proxyRdsClient = getProxyRdsClient(logger);
        when(proxy.injectCredentialsAndInvokeV2Async(any(), any())).thenReturn(awsResponseCompletableFuture);
        proxyRdsClient.injectCredentialsAndInvokeV2Async(awsRequest, request -> awsResponseCompletableFuture);
    }
}
