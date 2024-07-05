package software.amazon.rds.globalcluster;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.*;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.proxy.*;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class UpdateHandlerTest extends AbstractTestBase {

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private ProxyClient<RdsClient> proxyRdsClient;

    @Mock
    RdsClient rds;

    private UpdateHandler handler;

    @AfterEach
    public void post_execute() {
    }

    @BeforeEach
    public void setup() {
        handler = new UpdateHandler();
        rds = mock(RdsClient.class);
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        proxyRdsClient = MOCK_PROXY(proxy, rds);
    }

    @Test
    public void handleRequest_SimpleSuccess() {
        final ModifyGlobalClusterResponse modifyGlobalClusterResponse = ModifyGlobalClusterResponse.builder().build();
        when(proxyRdsClient.client().modifyGlobalCluster(any(ModifyGlobalClusterRequest.class))).thenReturn(modifyGlobalClusterResponse);
        final DescribeGlobalClustersResponse describeGlobalClustersResponse = DescribeGlobalClustersResponse.builder().globalClusters(GLOBAL_CLUSTER_ACTIVE).build();
        when(proxyRdsClient.client().describeGlobalClusters(any(DescribeGlobalClustersRequest.class))).thenReturn(describeGlobalClustersResponse);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .previousResourceState(RESOURCE_MODEL)
                .desiredResourceState(RESOURCE_MODEL_UPDATE).build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyRdsClient, logger);
        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyRdsClient.client()).modifyGlobalCluster(any(ModifyGlobalClusterRequest.class));
        verify(proxyRdsClient.client(), times(2)).describeGlobalClusters(any(DescribeGlobalClustersRequest.class));
        verify(rds, times(2)).serviceName();
        verifyNoMoreInteractions(rds);
    }

    @Test
    public void handleRequest_EngineVersionUpgrade() {
        final ModifyGlobalClusterResponse modifyGlobalClusterResponse = ModifyGlobalClusterResponse.builder().build();
        when(proxyRdsClient.client().modifyGlobalCluster(any(ModifyGlobalClusterRequest.class))).thenReturn(modifyGlobalClusterResponse);
        final DescribeGlobalClustersResponse describeGlobalClustersResponse = DescribeGlobalClustersResponse.builder().globalClusters(GLOBAL_CLUSTER_ACTIVE).build();
        when(proxyRdsClient.client().describeGlobalClusters(any(DescribeGlobalClustersRequest.class))).thenReturn(describeGlobalClustersResponse);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .previousResourceState(RESOURCE_MODEL_UPDATE)
                .desiredResourceState(RESOURCE_MODEL_UPDATE_MVU)
                .build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyRdsClient, logger);
        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyRdsClient.client()).modifyGlobalCluster(any(ModifyGlobalClusterRequest.class));
        verify(proxyRdsClient.client(), times(2)).describeGlobalClusters(any(DescribeGlobalClustersRequest.class));
        verify(rds, times(2)).serviceName();
        verifyNoMoreInteractions(rds);
    }

    @Test
    public void handleRequest_ReturnsFailedResponse_WhenRdsClientThrowsClusterNotFoundException() {
        AwsErrorDetails awsErr = AwsErrorDetails.builder().sdkHttpResponse(SdkHttpResponse.builder().statusCode(404).build()).build();

        GlobalClusterNotFoundException exception = GlobalClusterNotFoundException.builder().awsErrorDetails(awsErr).build();

        when(proxyRdsClient.client().modifyGlobalCluster(any(ModifyGlobalClusterRequest.class))).thenThrow(exception);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .previousResourceState(RESOURCE_MODEL)
                .desiredResourceState(RESOURCE_MODEL_UPDATE).build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyRdsClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.NotFound);

        verify(proxyRdsClient.client()).modifyGlobalCluster(any(ModifyGlobalClusterRequest.class));

        verify(rds).serviceName();
        verifyNoMoreInteractions(rds);
    }

    @Test
    public void handleRequest_updateEngineLifecycleSupportShouldFail() {
        ResourceModel previousState = ResourceModel.builder()
                .globalClusterIdentifier(GLOBALCLUSTER_IDENTIFIER)
                .engineVersion(ENGINE_VERSION)
                .engine(ENGINE)
                .engineLifecycleSupport("open-source-rds-extended-support")
                .build();

        ResourceModel desiredState = ResourceModel.builder()
                .globalClusterIdentifier(GLOBALCLUSTER_IDENTIFIER)
                .engineVersion(ENGINE_VERSION)
                .engine(ENGINE)
                .engineLifecycleSupport("open-source-rds-extended-support-disabled")
                .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .previousResourceState(previousState)
                .desiredResourceState(desiredState).build();

        try {
            handler.handleRequest(proxy, request, new CallbackContext(), proxyRdsClient, logger);
        } catch (CfnInvalidRequestException e) {
            Assertions.assertEquals(e.getMessage(), "Invalid request provided: EngineLifecycleSupport cannot be modified.");
        }
    }
}
