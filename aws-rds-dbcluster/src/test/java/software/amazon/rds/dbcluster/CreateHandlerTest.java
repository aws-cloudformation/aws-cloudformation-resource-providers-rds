package software.amazon.rds.dbcluster;

import org.junit.jupiter.api.AfterEach;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.*;
import software.amazon.cloudformation.proxy.*;
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
        final CreateDbClusterResponse createDbClusterResponse = CreateDbClusterResponse.builder().build();
        when(proxyRdsClient.client().createDBCluster(any(CreateDbClusterRequest.class))).thenReturn(createDbClusterResponse);
        final DescribeDbClustersResponse describeDbClustersResponse = DescribeDbClustersResponse.builder().dbClusters(DBCLUSTER_ACTIVE).build();
        when(proxyRdsClient.client().describeDBClusters(any(DescribeDbClustersRequest.class))).thenReturn(describeDbClustersResponse);
        final AddRoleToDbClusterResponse addRoleToDBClusterResponse = AddRoleToDbClusterResponse.builder().build();
        when(proxyRdsClient.client().addRoleToDBCluster(any(AddRoleToDbClusterRequest.class))).thenReturn(addRoleToDBClusterResponse);

        final ListTagsForResourceResponse listTagsForResourceResponse = ListTagsForResourceResponse.builder().build();
        when(proxyRdsClient.client().listTagsForResource(any(ListTagsForResourceRequest.class))).thenReturn(listTagsForResourceResponse);


        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder().desiredResourceState(RESOURCE_MODEL).build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyRdsClient, logger);


        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyRdsClient.client()).createDBCluster(any(CreateDbClusterRequest.class));
        verify(proxyRdsClient.client(), times(3)).describeDBClusters(any(DescribeDbClustersRequest.class));
        verify(proxyRdsClient.client()).addRoleToDBCluster(any(AddRoleToDbClusterRequest.class));
        verify(proxyRdsClient.client()).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void handleRequest_CreateWithStabilizationSuccess() {
        final CreateDbClusterResponse createDbClusterResponse = CreateDbClusterResponse.builder().build();
        when(proxyRdsClient.client().createDBCluster(any(CreateDbClusterRequest.class))).thenReturn(createDbClusterResponse);
        final DescribeDbClustersResponse describeInProgressDbClustersResponse = DescribeDbClustersResponse.builder().dbClusters(DBCLUSTER_INPROGRESS).build();
        final DescribeDbClustersResponse describeActiveDbClustersResponse = DescribeDbClustersResponse.builder().dbClusters(DBCLUSTER_ACTIVE).build();

        AtomicInteger attempt = new AtomicInteger(2);
        when(proxyRdsClient.client().describeDBClusters(any(DescribeDbClustersRequest.class))).then((m) -> {
            switch (attempt.getAndDecrement()) {
                case 2:
                    return describeInProgressDbClustersResponse;
                default:
                    return describeActiveDbClustersResponse;
            }
        });

        final AddRoleToDbClusterResponse addRoleToDBClusterResponse = AddRoleToDbClusterResponse.builder().build();
        when(proxyRdsClient.client().addRoleToDBCluster(any(AddRoleToDbClusterRequest.class))).thenReturn(addRoleToDBClusterResponse);

        final ListTagsForResourceResponse listTagsForResourceResponse = ListTagsForResourceResponse.builder().build();
        when(proxyRdsClient.client().listTagsForResource(any(ListTagsForResourceRequest.class))).thenReturn(listTagsForResourceResponse);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder().desiredResourceState(RESOURCE_MODEL).build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyRdsClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyRdsClient.client()).createDBCluster(any(CreateDbClusterRequest.class));
        verify(proxyRdsClient.client(), times(4)).describeDBClusters(any(DescribeDbClustersRequest.class));
        verify(proxyRdsClient.client()).addRoleToDBCluster(any(AddRoleToDbClusterRequest.class));
        verify(proxyRdsClient.client()).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void handleRequest_Cluster_AlreadyExist() {
        when(proxyRdsClient.client().createDBCluster(any(CreateDbClusterRequest.class)))
                .thenThrow(DbClusterAlreadyExistsException.class);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder().desiredResourceState(RESOURCE_MODEL).build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyRdsClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.AlreadyExists);

        verify(proxyRdsClient.client()).createDBCluster(any(CreateDbClusterRequest.class));
    }

    @Test
    public void handleRequest_Cluster_handleFailure() {
        when(proxyRdsClient.client().createDBCluster(any(CreateDbClusterRequest.class)))
                .thenThrow(DbClusterEndpointAlreadyExistsException.class);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder().desiredResourceState(RESOURCE_MODEL).build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyRdsClient, logger);
        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.GeneralServiceException);

        verify(proxyRdsClient.client()).createDBCluster(any(CreateDbClusterRequest.class));
    }

    @Test
    public void handleRequest_ClusterRestoreInProgress() {
        final RestoreDbClusterFromSnapshotResponse restoreDbClusterFromSnapshotResponse = RestoreDbClusterFromSnapshotResponse.builder().build();
        when(proxyRdsClient.client().restoreDBClusterFromSnapshot(any(RestoreDbClusterFromSnapshotRequest.class))).thenReturn(restoreDbClusterFromSnapshotResponse);
        final DescribeDbClustersResponse describeDbClustersResponse = DescribeDbClustersResponse.builder().dbClusters(DBCLUSTER_ACTIVE).build();
        when(proxyRdsClient.client().describeDBClusters(any(DescribeDbClustersRequest.class))).thenReturn(describeDbClustersResponse);
        final ModifyDbClusterResponse modifyDbClusterResponse = ModifyDbClusterResponse.builder().build();
        when(proxyRdsClient.client().modifyDBCluster(any(ModifyDbClusterRequest.class))).thenReturn(modifyDbClusterResponse);

        CallbackContext callbackContext = new CallbackContext();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder().desiredResourceState(RESOURCE_MODEL_ON_RESTORE).build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, callbackContext, proxyRdsClient, logger);

        callbackContext.setModified(true);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.IN_PROGRESS);
        assertThat(response.getCallbackContext()).isEqualTo(callbackContext);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(60);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyRdsClient.client()).restoreDBClusterFromSnapshot(any(RestoreDbClusterFromSnapshotRequest.class));
        verify(proxyRdsClient.client()).describeDBClusters(any(DescribeDbClustersRequest.class));
        verify(proxyRdsClient.client()).modifyDBCluster(any(ModifyDbClusterRequest.class));
    }

    @Test
    public void handleRequest_ClusterRestoreSuccess() {
        final RestoreDbClusterFromSnapshotResponse restoreDbClusterFromSnapshotResponse = RestoreDbClusterFromSnapshotResponse.builder().build();
        when(proxyRdsClient.client().restoreDBClusterFromSnapshot(any(RestoreDbClusterFromSnapshotRequest.class))).thenReturn(restoreDbClusterFromSnapshotResponse);
        final DescribeDbClustersResponse describeDbClustersResponse = DescribeDbClustersResponse.builder().dbClusters(DBCLUSTER_ACTIVE).build();
        when(proxyRdsClient.client().describeDBClusters(any(DescribeDbClustersRequest.class))).thenReturn(describeDbClustersResponse);

        final ListTagsForResourceResponse listTagsForResourceResponse = ListTagsForResourceResponse.builder().build();
        when(proxyRdsClient.client().listTagsForResource(any(ListTagsForResourceRequest.class))).thenReturn(listTagsForResourceResponse);

        CallbackContext callbackContext = new CallbackContext();
        callbackContext.setModified(true);
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder().desiredResourceState(RESOURCE_MODEL_ON_RESTORE).build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, callbackContext, proxyRdsClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyRdsClient.client()).restoreDBClusterFromSnapshot(any(RestoreDbClusterFromSnapshotRequest.class));
        verify(proxyRdsClient.client(), times(2)).describeDBClusters(any(DescribeDbClustersRequest.class));
        verify(proxyRdsClient.client()).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void handleRequest_ClusterRestore_AlreadyExist() {
        when(proxyRdsClient.client().restoreDBClusterFromSnapshot(any(RestoreDbClusterFromSnapshotRequest.class)))
                .thenThrow(DbClusterAlreadyExistsException.class);
        CallbackContext callbackContext = new CallbackContext();
        callbackContext.setModified(true);
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder().desiredResourceState(RESOURCE_MODEL_ON_RESTORE).build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, callbackContext, proxyRdsClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.AlreadyExists);

        verify(proxyRdsClient.client()).restoreDBClusterFromSnapshot(any(RestoreDbClusterFromSnapshotRequest.class));
    }

    @Test
    public void handleRequest_ClusterRestore_handleFailure() {
        when(proxyRdsClient.client().restoreDBClusterFromSnapshot(any(RestoreDbClusterFromSnapshotRequest.class)))
                .thenThrow(DbClusterEndpointAlreadyExistsException.class);
        CallbackContext callbackContext = new CallbackContext();
        callbackContext.setModified(true);
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder().desiredResourceState(RESOURCE_MODEL_ON_RESTORE).build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, callbackContext, proxyRdsClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.GeneralServiceException);

        verify(proxyRdsClient.client()).restoreDBClusterFromSnapshot(any(RestoreDbClusterFromSnapshotRequest.class));
    }

    @Test
    public void handleRequest_ClusterRestoreInTimeSuccess() {
        final RestoreDbClusterToPointInTimeResponse restoreDbClusterToPointInTimeResponse = RestoreDbClusterToPointInTimeResponse.builder().build();
        when(proxyRdsClient.client().restoreDBClusterToPointInTime(any(RestoreDbClusterToPointInTimeRequest.class))).thenReturn(restoreDbClusterToPointInTimeResponse);
        final DescribeDbClustersResponse describeDbClustersResponse = DescribeDbClustersResponse.builder().dbClusters(DBCLUSTER_ACTIVE).build();
        when(proxyRdsClient.client().describeDBClusters(any(DescribeDbClustersRequest.class))).thenReturn(describeDbClustersResponse);

        final ListTagsForResourceResponse listTagsForResourceResponse = ListTagsForResourceResponse.builder().build();
        when(proxyRdsClient.client().listTagsForResource(any(ListTagsForResourceRequest.class))).thenReturn(listTagsForResourceResponse);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder().desiredResourceState(RESOURCE_MODEL_ON_RESTORE_IN_TIME).logicalResourceIdentifier("dbcluster").clientRequestToken("request").build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyRdsClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyRdsClient.client()).restoreDBClusterToPointInTime(any(RestoreDbClusterToPointInTimeRequest.class));
        verify(proxyRdsClient.client(), times(2)).describeDBClusters(any(DescribeDbClustersRequest.class));
        verify(proxyRdsClient.client()).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void handleRequest_ClusterRestoreInTime_AlreadyExist() {
        when(proxyRdsClient.client().restoreDBClusterToPointInTime(any(RestoreDbClusterToPointInTimeRequest.class)))
                .thenThrow(DbClusterAlreadyExistsException.class);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder().desiredResourceState(RESOURCE_MODEL_ON_RESTORE_IN_TIME).logicalResourceIdentifier("dbcluster").clientRequestToken("request").build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyRdsClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.AlreadyExists);

        verify(proxyRdsClient.client()).restoreDBClusterToPointInTime(any(RestoreDbClusterToPointInTimeRequest.class));
    }

    @Test
    public void handleRequest_ClusterRestoreInTime_handlerFailure() {
        when(proxyRdsClient.client().restoreDBClusterToPointInTime(any(RestoreDbClusterToPointInTimeRequest.class)))
                .thenThrow(DbClusterEndpointAlreadyExistsException.class);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder().desiredResourceState(RESOURCE_MODEL_ON_RESTORE_IN_TIME).logicalResourceIdentifier("dbcluster").clientRequestToken("request").build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyRdsClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.GeneralServiceException);

        verify(proxyRdsClient.client()).restoreDBClusterToPointInTime(any(RestoreDbClusterToPointInTimeRequest.class));
    }
}
