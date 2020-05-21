package software.amazon.rds.globalcluster;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.*;
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
        verify(rds, atLeastOnce()).serviceName();
        verifyNoMoreInteractions(rds);
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
        final RemoveFromGlobalClusterResponse removeFromGlobalClusterResponse = RemoveFromGlobalClusterResponse.builder().build();
        when(proxyRdsClient.client().removeFromGlobalCluster(any(RemoveFromGlobalClusterRequest.class))).thenReturn(removeFromGlobalClusterResponse);
        final DescribeDbClustersResponse describeDbClustersResponse = DescribeDbClustersResponse.builder().dbClusters(DBCLUSTER_ACTIVE).build();
        when(proxyRdsClient.client().describeDBClusters(any(DescribeDbClustersRequest.class))).thenReturn(describeDbClustersResponse);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder().desiredResourceState(RESOURCE_MODEL_UPDATE).build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyRdsClient, logger);
        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyRdsClient.client()).removeFromGlobalCluster(any(RemoveFromGlobalClusterRequest.class));
        verify(proxyRdsClient.client(), times(1)).describeDBClusters(any(DescribeDbClustersRequest.class));
    }

}
