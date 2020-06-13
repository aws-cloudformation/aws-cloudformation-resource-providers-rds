package software.amazon.rds.globalcluster;

import org.junit.jupiter.api.AfterEach;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.CreateGlobalClusterRequest;
import software.amazon.awssdk.services.rds.model.CreateGlobalClusterResponse;
import software.amazon.awssdk.services.rds.model.DescribeGlobalClustersRequest;
import software.amazon.awssdk.services.rds.model.DescribeGlobalClustersResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.OperationStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
public class CreateHandlerTest extends AbstractTestBase {

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private ProxyClient<RdsClient> proxyRdsClient;

    @Mock
    RdsClient rds;

    private CreateHandler handler;

    @AfterEach
    public void post_execute() {
        verify(rds, atLeastOnce()).serviceName();
        verifyNoMoreInteractions(rds);
    }

    @BeforeEach
    public void setup() {
        handler = new CreateHandler();
        rds = mock(RdsClient.class);
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        proxyRdsClient = MOCK_PROXY(proxy, rds);
    }

    @Test
    public void handleRequest_SimpleSuccess() {
        final CreateGlobalClusterResponse createGlobalClusterResponse = CreateGlobalClusterResponse.builder().build();
        when(proxyRdsClient.client().createGlobalCluster(any(CreateGlobalClusterRequest.class))).thenReturn(createGlobalClusterResponse);
        final DescribeGlobalClustersResponse describeGlobalClustersResponse = DescribeGlobalClustersResponse.builder().globalClusters(GLOBAL_CLUSTER_ACTIVE).build();
        when(proxyRdsClient.client().describeGlobalClusters(any(DescribeGlobalClustersRequest.class))).thenReturn(describeGlobalClustersResponse);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder().desiredResourceState(RESOURCE_MODEL).build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyRdsClient, logger);
        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyRdsClient.client()).createGlobalCluster(any(CreateGlobalClusterRequest.class));
    }

    @Test
    public void handleRequest_SimpleSuccessAlternative() {
        final CreateGlobalClusterResponse createGlobalClusterResponse = CreateGlobalClusterResponse.builder().build();
        when(proxyRdsClient.client().createGlobalCluster(any(CreateGlobalClusterRequest.class))).thenReturn(createGlobalClusterResponse);
        final DescribeGlobalClustersResponse describeGlobalClustersResponse = DescribeGlobalClustersResponse.builder().globalClusters(GLOBAL_CLUSTER_ACTIVE).build();
        when(proxyRdsClient.client().describeGlobalClusters(any(DescribeGlobalClustersRequest.class))).thenReturn(describeGlobalClustersResponse);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder().desiredResourceState(RESOURCE_MODEL_ALTERNATIVE).build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyRdsClient, logger);
        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyRdsClient.client()).createGlobalCluster(any(CreateGlobalClusterRequest.class));
    }

    @Test
    public void handleRequest_SimpleSuccessWithMaster() {
        final CreateGlobalClusterResponse createGlobalClusterResponse = CreateGlobalClusterResponse.builder().build();
        when(proxyRdsClient.client().createGlobalCluster(any(CreateGlobalClusterRequest.class))).thenReturn(createGlobalClusterResponse);
        final DescribeGlobalClustersResponse describeGlobalClustersResponse = DescribeGlobalClustersResponse.builder().globalClusters(GLOBAL_CLUSTER_ACTIVE).build();
        when(proxyRdsClient.client().describeGlobalClusters(any(DescribeGlobalClustersRequest.class))).thenReturn(describeGlobalClustersResponse);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder().desiredResourceState(RESOURCE_MODEL).build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyRdsClient, logger);
        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyRdsClient.client()).createGlobalCluster(any(CreateGlobalClusterRequest.class));
    }

    @Test
    public void handleRequest_FailedWithNotExists() {
        final CreateGlobalClusterResponse createGlobalClusterResponse = CreateGlobalClusterResponse.builder().build();
        when(proxyRdsClient.client().createGlobalCluster(any(CreateGlobalClusterRequest.class))).thenReturn(createGlobalClusterResponse);
        final DescribeGlobalClustersResponse describeGlobalClustersResponse = DescribeGlobalClustersResponse.builder().globalClusters(GLOBAL_CLUSTER_ACTIVE).build();
        when(proxyRdsClient.client().describeGlobalClusters(any(DescribeGlobalClustersRequest.class))).thenReturn(describeGlobalClustersResponse);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL_EMPTY_IDENTIFIER)
                .logicalResourceIdentifier("globalcluster")
                .clientRequestToken("4b90a7e4-b791-4512-a137-0cf12a23451e")
                .build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyRdsClient, logger);
        System.out.println(response);
        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyRdsClient.client()).createGlobalCluster(any(CreateGlobalClusterRequest.class));
    }
}
