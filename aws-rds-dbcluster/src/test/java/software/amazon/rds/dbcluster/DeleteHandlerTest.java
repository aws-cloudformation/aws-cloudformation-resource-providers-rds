package software.amazon.rds.dbcluster;

import org.junit.jupiter.api.AfterEach;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DeleteDbClusterRequest;
import software.amazon.awssdk.services.rds.model.DeleteDbClusterResponse;
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
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class DeleteHandlerTest extends AbstractTestBase {


    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private ProxyClient<RdsClient> proxyRdsClient;

    @Mock
    RdsClient rds;

    private DeleteHandler handler;

    @AfterEach
    public void post_execute() {
        verify(rds, atLeastOnce()).serviceName();
        verifyNoMoreInteractions(rds);
    }

    @BeforeEach
    public void setup() {
        handler = new DeleteHandler();
        rds = mock(RdsClient.class);
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        proxyRdsClient = MOCK_PROXY(proxy, rds);
    }

    @Test
    public void handleRequest_SimpleSuccess() {
        final DeleteDbClusterResponse deleteDbClusterRequest = DeleteDbClusterResponse.builder().build();
        when(proxyRdsClient.client().deleteDBCluster(any(DeleteDbClusterRequest.class))).thenReturn(deleteDbClusterRequest);
        final DescribeDbClustersResponse describeInProgressDbClustersResponse = DescribeDbClustersResponse.builder().dbClusters(DBCLUSTER_ACTIVE).build();
        final DescribeDbClustersResponse describeDeletedDbClustersResponse = DescribeDbClustersResponse.builder().dbClusters(DBCLUSTER_DELETED).build();
        AtomicInteger attempt = new AtomicInteger(2);
        when(proxyRdsClient.client().describeDBClusters(any(DescribeDbClustersRequest.class))).then((m) -> {
            switch (attempt.getAndDecrement()) {
                case 2:
                    return describeInProgressDbClustersResponse;
                default:
                    return describeDeletedDbClustersResponse;
            }
        });
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder().desiredResourceState(RESOURCE_MODEL).logicalResourceIdentifier("dbcluster").clientRequestToken("request").build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyRdsClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyRdsClient.client()).deleteDBCluster(any(DeleteDbClusterRequest.class));
        verify(proxyRdsClient.client(), times(2)).describeDBClusters(any(DescribeDbClustersRequest.class));
    }
}
