package software.amazon.rds.common.logging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import software.amazon.awssdk.awscore.AwsRequest;
import software.amazon.awssdk.awscore.AwsResponse;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.pagination.sync.SdkIterable;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesResponse;
import software.amazon.awssdk.services.rds.paginators.DescribeDBInstancesIterable;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.ProxyClientTestBase;
import software.amazon.rds.common.printer.FilteredJsonPrinter;

@ExtendWith(MockitoExtension.class)
class LoggingProxyClientTest extends ProxyClientTestBase {
    public static final String AWS_ACCOUNT_ID = "123456789";
    public static final String TOKEN = "token";
    public static final String STACK_ID = "stackId";

    @Captor
    ArgumentCaptor<String> captor;

    @Mock
    private ProxyClient<RdsClient> proxy;

    @Mock
    private Logger logger;

    private AwsRequest awsRequest;
    private AwsResponse awsResponse;
    private SdkIterable<DescribeDbInstancesResponse> sdkIterable;
    private CompletableFuture<AwsResponse> awsResponseCompletableFuture;
    private ResourceHandlerRequest<Void> request;

    @BeforeEach
    public void setup() {
        request = new ResourceHandlerRequest<>();
        request.setAwsAccountId(AWS_ACCOUNT_ID);
        request.setClientRequestToken(TOKEN);
        request.setStackId(STACK_ID);
        awsRequest = DescribeDbInstancesRequest.builder().build();
        awsResponse = DescribeDbInstancesResponse.builder().build();
        sdkIterable = new DescribeDBInstancesIterable(proxy.client(), DescribeDbInstancesRequest.builder().build());
        awsResponseCompletableFuture = new CompletableFuture<>();
    }

    private ProxyClient<RdsClient> getProxyRdsClient(Logger logger) {
        return new LoggingProxyClient<>(new RequestLogger(logger, request, new FilteredJsonPrinter()), proxy);
    }

    @Test
    void test_injectCredentialsAndInvokeV2() {
        ProxyClient<RdsClient> proxyRdsClient = getProxyRdsClient(logger);
        when(proxy.injectCredentialsAndInvokeV2(any(), any())).thenReturn(awsResponse);
        proxyRdsClient.injectCredentialsAndInvokeV2(awsRequest, request -> awsResponse);
        verify(logger, times(2)).log(captor.capture());
        assertThat(captor.getValue().contains(TOKEN)).isTrue();
    }

    @Test
    void test_injectCredentialsAndInvokeV2Bytes() {
        ProxyClient<RdsClient> proxyRdsClient = getProxyRdsClient(logger);
        ResponseBytes<AwsResponse> response = ResponseBytes.fromByteArray(awsResponse, awsResponse.toString().getBytes());
        when(proxy.injectCredentialsAndInvokeV2Bytes(any(), any())).thenReturn(response);
        proxyRdsClient.injectCredentialsAndInvokeV2Bytes(awsRequest, request -> response);
        verify(logger, times(2)).log(captor.capture());
        assertThat(captor.getValue().contains(STACK_ID)).isTrue();
    }

    @Test
    void test_injectCredentialsAndInvokeIterableV2() {
        ProxyClient<RdsClient> proxyRdsClient = getProxyRdsClient(logger);
        when(proxy.injectCredentialsAndInvokeIterableV2(any(), any())).thenReturn(sdkIterable);
        proxyRdsClient.injectCredentialsAndInvokeIterableV2(awsRequest, request -> sdkIterable);
        verify(logger, times(2)).log(captor.capture());
        assertThat(captor.getValue().contains(AWS_ACCOUNT_ID)).isTrue();
    }

    @Test
    void test_injectCredentialsAndInvokeV2Async() {
        ProxyClient<RdsClient> proxyRdsClient = getProxyRdsClient(logger);
        when(proxy.injectCredentialsAndInvokeV2Async(any(), any())).thenReturn(awsResponseCompletableFuture);
        proxyRdsClient.injectCredentialsAndInvokeV2Async(awsRequest, request -> awsResponseCompletableFuture);
        verify(logger, times(2)).log(captor.capture());
        assertThat(captor.getValue().contains(STACK_ID)).isTrue();
    }
}
