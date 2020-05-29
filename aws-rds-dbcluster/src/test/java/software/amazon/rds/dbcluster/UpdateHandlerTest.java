package software.amazon.rds.dbcluster;

import org.junit.jupiter.api.AfterEach;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DbClusterRoleNotFoundException;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersResponse;
import software.amazon.awssdk.services.rds.model.AddRoleToDbClusterRequest;
import software.amazon.awssdk.services.rds.model.AddRoleToDBClusterResponse;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.rds.model.RemoveRoleFromDbClusterRequest;
import software.amazon.awssdk.services.rds.model.RemoveRoleFromDBClusterResponse;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceRequest;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceResponse;
import software.amazon.awssdk.services.rds.model.RemoveTagsFromResourceRequest;
import software.amazon.awssdk.services.rds.model.RemoveTagsFromResourceResponse;
import software.amazon.awssdk.services.rds.model.ModifyDbClusterRequest;
import software.amazon.awssdk.services.rds.model.ModifyDbClusterResponse;
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
    public void handleRequest_InProgress() {
        final ModifyDbClusterResponse modifyDbClusterResponse = ModifyDbClusterResponse.builder().build();
        when(proxyRdsClient.client().modifyDBCluster(any(ModifyDbClusterRequest.class))).thenReturn(modifyDbClusterResponse);

        CallbackContext callbackContext = new CallbackContext();
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder().desiredResourceState(RESOURCE_MODEL).previousResourceState(RESOURCE_MODEL).build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, callbackContext, proxyRdsClient, logger);
        callbackContext.setModified(true);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.IN_PROGRESS);
        assertThat(response.getCallbackContext()).isEqualTo(callbackContext);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(60);
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyRdsClient.client()).modifyDBCluster(any(ModifyDbClusterRequest.class));
    }

    @Test
    public void handleRequest_SimpleSuccessRoleNotFound() {

        final DescribeDbClustersResponse describeActiveDbClustersResponse = DescribeDbClustersResponse.builder().dbClusters(DBCLUSTER_ACTIVE).build();
        when(proxyRdsClient.client().removeRoleFromDBCluster(any(RemoveRoleFromDbClusterRequest.class))).thenThrow(
            DbClusterRoleNotFoundException.class);
        final DescribeDbClustersResponse describeDbClustersResponseWithNoRole = DescribeDbClustersResponse.builder().dbClusters(DBCLUSTER_ACTIVE_NO_ROLE).build();

        final AddRoleToDBClusterResponse addRoleToDBClusterResponse = AddRoleToDBClusterResponse.builder().build();
        when(proxyRdsClient.client().addRoleToDBCluster(any(AddRoleToDbClusterRequest.class))).thenReturn(addRoleToDBClusterResponse);

        when(proxyRdsClient.client().describeDBClusters(any(DescribeDbClustersRequest.class))).thenReturn(describeActiveDbClustersResponse);

        final ListTagsForResourceResponse listTagsForResourceResponse = ListTagsForResourceResponse.builder().build();
        final RemoveTagsFromResourceResponse removeTagsFromResourceResponse = RemoveTagsFromResourceResponse.builder().build();
        final AddTagsToResourceResponse addTagsToResourceResponse = AddTagsToResourceResponse.builder().build();
        when(proxyRdsClient.client().listTagsForResource(any(ListTagsForResourceRequest.class))).thenReturn(listTagsForResourceResponse);
        when(proxyRdsClient.client().removeTagsFromResource(any(RemoveTagsFromResourceRequest.class))).thenReturn(removeTagsFromResourceResponse);
        when(proxyRdsClient.client().addTagsToResource(any(AddTagsToResourceRequest.class))).thenReturn(addTagsToResourceResponse);

        CallbackContext callbackContext = new CallbackContext();
        callbackContext.setModified(true);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder().desiredResourceState(RESOURCE_MODEL).previousResourceState(RESOURCE_MODEL).build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, callbackContext, proxyRdsClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyRdsClient.client(), times(4)).describeDBClusters(any(DescribeDbClustersRequest.class));
        verify(proxyRdsClient.client(), times(2)).listTagsForResource(any(ListTagsForResourceRequest.class));
        verify(proxyRdsClient.client()).removeTagsFromResource(any(RemoveTagsFromResourceRequest.class));
        verify(proxyRdsClient.client()).addTagsToResource(any(AddTagsToResourceRequest.class));

    }

    @Test
    public void handleRequest_SimpleSuccess() {

        final DescribeDbClustersResponse describeInProgressDbClustersResponse = DescribeDbClustersResponse.builder().dbClusters(DBCLUSTER_INPROGRESS).build();
        final DescribeDbClustersResponse describeActiveDbClustersResponse = DescribeDbClustersResponse.builder().dbClusters(DBCLUSTER_ACTIVE).build();
        final RemoveRoleFromDBClusterResponse removeRoleFromDBClusterResponse = RemoveRoleFromDBClusterResponse.builder().build();
        when(proxyRdsClient.client().removeRoleFromDBCluster(any(RemoveRoleFromDbClusterRequest.class))).thenReturn(removeRoleFromDBClusterResponse);
        final DescribeDbClustersResponse describeDbClustersResponseWithNoRole = DescribeDbClustersResponse.builder().dbClusters(DBCLUSTER_ACTIVE_NO_ROLE).build();

        final AddRoleToDBClusterResponse addRoleToDBClusterResponse = AddRoleToDBClusterResponse.builder().build();
        when(proxyRdsClient.client().addRoleToDBCluster(any(AddRoleToDbClusterRequest.class))).thenReturn(addRoleToDBClusterResponse);

        AtomicInteger attempt = new AtomicInteger(2);
        when(proxyRdsClient.client().describeDBClusters(any(DescribeDbClustersRequest.class))).then((m) -> {
            switch (attempt.getAndDecrement()) {
                case 1:
                    return describeInProgressDbClustersResponse;
                case 0:
                    return describeDbClustersResponseWithNoRole;
                default:
                    return describeActiveDbClustersResponse;
            }
        });

        final ListTagsForResourceResponse listTagsForResourceResponse = ListTagsForResourceResponse.builder().build();
        final RemoveTagsFromResourceResponse removeTagsFromResourceResponse = RemoveTagsFromResourceResponse.builder().build();
        final AddTagsToResourceResponse addTagsToResourceResponse = AddTagsToResourceResponse.builder().build();
        when(proxyRdsClient.client().listTagsForResource(any(ListTagsForResourceRequest.class))).thenReturn(listTagsForResourceResponse);
        when(proxyRdsClient.client().removeTagsFromResource(any(RemoveTagsFromResourceRequest.class))).thenReturn(removeTagsFromResourceResponse);
        when(proxyRdsClient.client().addTagsToResource(any(AddTagsToResourceRequest.class))).thenReturn(addTagsToResourceResponse);

        CallbackContext callbackContext = new CallbackContext();
        callbackContext.setModified(true);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder().desiredResourceState(RESOURCE_MODEL).previousResourceState(RESOURCE_MODEL).build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, callbackContext, proxyRdsClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyRdsClient.client(), times(6)).describeDBClusters(any(DescribeDbClustersRequest.class));
        verify(proxyRdsClient.client(), times(2)).listTagsForResource(any(ListTagsForResourceRequest.class));
        verify(proxyRdsClient.client()).removeTagsFromResource(any(RemoveTagsFromResourceRequest.class));
        verify(proxyRdsClient.client()).addTagsToResource(any(AddTagsToResourceRequest.class));

    }
}
