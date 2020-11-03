package software.amazon.rds.globalcluster;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DeleteGlobalClusterRequest;
import software.amazon.awssdk.services.rds.model.DeleteGlobalClusterResponse;
import software.amazon.awssdk.services.rds.model.DescribeGlobalClustersRequest;
import software.amazon.awssdk.services.rds.model.DescribeGlobalClustersResponse;
import software.amazon.awssdk.services.rds.model.GlobalClusterNotFoundException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.DelayFactory;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.WaitStrategy;

@ExtendWith(MockitoExtension.class)
public class DeleteHandlerWithProgressTest extends AbstractTestBase {

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private ProxyClient<RdsClient> proxyRdsClient;

    @Mock
    RdsClient rds;

    private DeleteHandler handler;

    @BeforeEach
    public void setup() {
        handler = new DeleteHandler();
        rds = mock(RdsClient.class);
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, DelayFactory.CONSTANT_DEFAULT_DELAY_FACTORY,
                WaitStrategy.scheduleForCallbackStrategy());
        proxyRdsClient = MOCK_PROXY(proxy, rds);
    }

    @Test
    public void handleRequest_SimpleSuccess() {
        final DeleteGlobalClusterResponse deleteGlobalClusterResponse = DeleteGlobalClusterResponse.builder().build();
        when(proxyRdsClient.client().deleteGlobalCluster(any(DeleteGlobalClusterRequest.class))).thenReturn(deleteGlobalClusterResponse);
        when(proxyRdsClient.client().describeGlobalClusters(any(DescribeGlobalClustersRequest.class))).thenThrow(GlobalClusterNotFoundException.class);
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder().desiredResourceState(RESOURCE_MODEL).build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyRdsClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyRdsClient.client()).deleteGlobalCluster(any(DeleteGlobalClusterRequest.class));
        verify(proxyRdsClient.client(), times(2)).describeGlobalClusters(any(DescribeGlobalClustersRequest.class));

        verify(rds).serviceName();
        verifyNoMoreInteractions(rds);
    }

    @Test
    public void handleRequest_ReturnsInProgress_WhenGlobalClusterIsStillDeleting() {
        DescribeGlobalClustersResponse describeResponse = DescribeGlobalClustersResponse.builder().build();
        final DeleteGlobalClusterResponse deleteGlobalClusterResponse = DeleteGlobalClusterResponse.builder().build();
        when(proxyRdsClient.client().deleteGlobalCluster(any(DeleteGlobalClusterRequest.class))).thenReturn(deleteGlobalClusterResponse);
        when(proxyRdsClient.client().describeGlobalClusters(any(DescribeGlobalClustersRequest.class))).thenReturn(describeResponse);
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder().desiredResourceState(RESOURCE_MODEL).build();

        final CallbackContext callbackContext = new CallbackContext();

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, callbackContext, proxyRdsClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.IN_PROGRESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(30);
        assertThat(response.getResourceModel()).isSameAs(RESOURCE_MODEL);
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyRdsClient.client()).deleteGlobalCluster(any(DeleteGlobalClusterRequest.class));
        verify(proxyRdsClient.client(), times(2)).describeGlobalClusters(any(DescribeGlobalClustersRequest.class));

        verify(rds).serviceName();
        verifyNoMoreInteractions(rds);
    }

    @Test
    public void handleRequest_ReturnsSuccess_WhenGlobalClusterDeletingFinishesAfterInProgressResponse() {
        when(proxyRdsClient.client().describeGlobalClusters(any(DescribeGlobalClustersRequest.class))).thenThrow(GlobalClusterNotFoundException.class);
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder().desiredResourceState(RESOURCE_MODEL).build();

        final CallbackContext callbackContext = new CallbackContext();
        callbackContext.setDeleting(true);

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, callbackContext, proxyRdsClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyRdsClient.client(), times(1)).describeGlobalClusters(any(DescribeGlobalClustersRequest.class));

        verify(rds).serviceName();
        verifyNoMoreInteractions(rds);
    }

    @Test
    public void handleRequest_ReturnsFailedResponse_WhenRdsClientThrowsClusterNotFoundException() {
        AwsErrorDetails awsErr = AwsErrorDetails.builder().sdkHttpResponse(SdkHttpResponse.builder().statusCode(404).build()).build();

        GlobalClusterNotFoundException exception = GlobalClusterNotFoundException.builder().awsErrorDetails(awsErr).build();

        when(proxyRdsClient.client().deleteGlobalCluster(any(DeleteGlobalClusterRequest.class))).thenThrow(exception);
        when(proxyRdsClient.client().describeGlobalClusters(any(DescribeGlobalClustersRequest.class))).thenThrow(GlobalClusterNotFoundException.class);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder().desiredResourceState(RESOURCE_MODEL).build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyRdsClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.NotFound);

        verify(proxyRdsClient.client()).deleteGlobalCluster(any(DeleteGlobalClusterRequest.class));
        verify(rds).serviceName();
        verifyNoMoreInteractions(rds);
    }
}
