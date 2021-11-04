package software.amazon.rds.dbcluster;

import com.diffplug.common.collect.ImmutableList;
import org.junit.jupiter.api.AfterEach;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.*;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.exceptions.CfnNotStabilizedException;
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
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
        final DescribeDbClustersResponse describeDbClustersResponse = DescribeDbClustersResponse.builder().dbClusters(DBCLUSTER_ACTIVE).build();
        when(proxyRdsClient.client().describeDBClusters(any(DescribeDbClustersRequest.class)))
                .thenReturn(describeDbClustersResponse)
                .thenThrow(DbClusterNotFoundException.class);
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder().desiredResourceState(RESOURCE_MODEL).logicalResourceIdentifier("dbcluster").clientRequestToken("request").build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyRdsClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyRdsClient.client()).deleteDBCluster(any(DeleteDbClusterRequest.class));
        verify(proxyRdsClient.client(), times(2)).describeDBClusters(any(DescribeDbClustersRequest.class));
    }

    @Test
    public void handleRequest_isDeletionProtectionEnabled() {
        final DescribeDbClustersResponse describeDbClustersResponse = DescribeDbClustersResponse.builder().dbClusters(DBCLUSTER_ACTIVE_DELETION_ENABLED).build();
        when(proxyRdsClient.client().describeDBClusters(any(DescribeDbClustersRequest.class)))
                .thenReturn(describeDbClustersResponse);
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder().desiredResourceState(RESOURCE_MODEL).logicalResourceIdentifier("dbcluster").clientRequestToken("request").build();
        assertThrows(CfnGeneralServiceException.class,
                () -> handler.handleRequest(proxy, request, new CallbackContext(), proxyRdsClient, logger));
    }

    @Test
    public void handleRequest_isDeletionProtectionEnabled_failure() {
        final DescribeDbClustersResponse describeDbClustersResponse = DescribeDbClustersResponse.builder().dbClusters(Collections.emptyList()).build();
        when(proxyRdsClient.client().describeDBClusters(any(DescribeDbClustersRequest.class)))
                .thenReturn(describeDbClustersResponse);
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder().desiredResourceState(RESOURCE_MODEL).logicalResourceIdentifier("dbcluster").clientRequestToken("request").build();
        assertThrows(CfnNotFoundException.class,
                () -> handler.handleRequest(proxy, request, new CallbackContext(), proxyRdsClient, logger));
    }

    @Test
    public void handleRequest_isDeletionProtectionEnabled_notFound_failure() {
        when(proxyRdsClient.client().describeDBClusters(any(DescribeDbClustersRequest.class)))
                .thenThrow(DbClusterNotFoundException.class);
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder().desiredResourceState(RESOURCE_MODEL).logicalResourceIdentifier("dbcluster").clientRequestToken("request").build();
        assertThrows(CfnNotFoundException.class,
                () -> handler.handleRequest(proxy, request, new CallbackContext(), proxyRdsClient, logger));
    }

    @Test
    public void handleRequest_createSnapshot_beforeDelete() {
        final DeleteDbClusterResponse deleteDbClusterRequest = DeleteDbClusterResponse.builder().build();
        when(proxyRdsClient.client().deleteDBCluster(any(DeleteDbClusterRequest.class))).thenReturn(deleteDbClusterRequest);
        final DescribeDbClustersResponse describeDbClustersResponse = DescribeDbClustersResponse.builder().dbClusters(DBCLUSTER_ACTIVE).build();
        final CreateDbClusterSnapshotResponse createDbClusterSnapshotResponse = CreateDbClusterSnapshotResponse.builder()
                .dbClusterSnapshot(DBCLUSTER_SNAPSHOT).build();
        final DescribeDbClusterSnapshotsResponse describeDbClusterSnapshotsCreatingResponse = DescribeDbClusterSnapshotsResponse.builder()
                .dbClusterSnapshots(ImmutableList.of(DBCLUSTER_SNAPSHOT_CREATING)).build();
        final DescribeDbClusterSnapshotsResponse describeDbClusterSnapshotsAvailableResponse = DescribeDbClusterSnapshotsResponse.builder()
                .dbClusterSnapshots(ImmutableList.of(DBCLUSTER_SNAPSHOT_AVAILABLE)).build();

        when(proxyRdsClient.client().describeDBClusterSnapshots(any(DescribeDbClusterSnapshotsRequest.class)))
                .thenReturn(describeDbClusterSnapshotsCreatingResponse)
                .thenReturn(describeDbClusterSnapshotsAvailableResponse);

        when(proxyRdsClient.client().createDBClusterSnapshot(any(CreateDbClusterSnapshotRequest.class))).thenReturn(createDbClusterSnapshotResponse);
        when(proxyRdsClient.client().describeDBClusters(any(DescribeDbClustersRequest.class)))
                .thenReturn(describeDbClustersResponse)
                .thenThrow(DbClusterNotFoundException.class);
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL)
                .logicalResourceIdentifier("dbcluster")
                .clientRequestToken("request")
                .snapshotRequested(true)
                .build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyRdsClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyRdsClient.client()).deleteDBCluster(any(DeleteDbClusterRequest.class));
        verify(proxyRdsClient.client(), times(2)).describeDBClusters(any(DescribeDbClustersRequest.class));
    }

    @Test
    public void handleRequest_createSnapshot_notFound_Failure() {
        final DescribeDbClustersResponse describeDbClustersResponse = DescribeDbClustersResponse.builder().dbClusters(DBCLUSTER_ACTIVE).build();
        final CreateDbClusterSnapshotResponse createDbClusterSnapshotResponse = CreateDbClusterSnapshotResponse.builder()
                .dbClusterSnapshot(DBCLUSTER_SNAPSHOT).build();

        when(proxyRdsClient.client().describeDBClusterSnapshots(any(DescribeDbClusterSnapshotsRequest.class)))
                .thenThrow(DbClusterSnapshotNotFoundException.class);

        when(proxyRdsClient.client().createDBClusterSnapshot(any(CreateDbClusterSnapshotRequest.class))).thenReturn(createDbClusterSnapshotResponse);
        when(proxyRdsClient.client().describeDBClusters(any(DescribeDbClustersRequest.class)))
                .thenReturn(describeDbClustersResponse);
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL)
                .logicalResourceIdentifier("dbcluster")
                .clientRequestToken("request")
                .snapshotRequested(true)
                .build();

        assertThrows(CfnNotFoundException.class,
                () -> handler.handleRequest(proxy, request, new CallbackContext(), proxyRdsClient, logger));
    }

    @Test
    public void handleRequest_createSnapshot_snapShotNotPresent_notStabilized_Failure() {
        final DescribeDbClustersResponse describeDbClustersResponse = DescribeDbClustersResponse.builder().dbClusters(DBCLUSTER_ACTIVE).build();
        final CreateDbClusterSnapshotResponse createDbClusterSnapshotResponse = CreateDbClusterSnapshotResponse.builder()
                .dbClusterSnapshot(DBCLUSTER_SNAPSHOT).build();
        final DescribeDbClusterSnapshotsResponse describeDbClusterSnapshotsResponse = DescribeDbClusterSnapshotsResponse.builder()
                .dbClusterSnapshots(Collections.emptyList()).build();
        when(proxyRdsClient.client().describeDBClusterSnapshots(any(DescribeDbClusterSnapshotsRequest.class)))
                .thenReturn(describeDbClusterSnapshotsResponse);

        when(proxyRdsClient.client().createDBClusterSnapshot(any(CreateDbClusterSnapshotRequest.class))).thenReturn(createDbClusterSnapshotResponse);
        when(proxyRdsClient.client().describeDBClusters(any(DescribeDbClustersRequest.class)))
                .thenReturn(describeDbClustersResponse);
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL)
                .logicalResourceIdentifier("dbcluster")
                .clientRequestToken("request")
                .snapshotRequested(true)
                .build();

        assertThrows(CfnNotStabilizedException.class,
                () -> handler.handleRequest(proxy, request, new CallbackContext(), proxyRdsClient, logger));
    }

    @Test
    public void handleRequest_createSnapshot_notStabilized_Failure() {
        final DescribeDbClustersResponse describeDbClustersResponse = DescribeDbClustersResponse.builder().dbClusters(DBCLUSTER_ACTIVE).build();
        final CreateDbClusterSnapshotResponse createDbClusterSnapshotResponse = CreateDbClusterSnapshotResponse.builder()
                .dbClusterSnapshot(DBCLUSTER_SNAPSHOT).build();
        final DescribeDbClusterSnapshotsResponse describeDbClusterSnapshotsResponse = DescribeDbClusterSnapshotsResponse.builder()
                .dbClusterSnapshots(ImmutableList.of(DBCLUSTER_SNAPSHOT_FAILED)).build();
        when(proxyRdsClient.client().describeDBClusterSnapshots(any(DescribeDbClusterSnapshotsRequest.class)))
                .thenReturn(describeDbClusterSnapshotsResponse);

        when(proxyRdsClient.client().createDBClusterSnapshot(any(CreateDbClusterSnapshotRequest.class))).thenReturn(createDbClusterSnapshotResponse);
        when(proxyRdsClient.client().describeDBClusters(any(DescribeDbClustersRequest.class)))
                .thenReturn(describeDbClustersResponse);
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL)
                .logicalResourceIdentifier("dbcluster")
                .clientRequestToken("request")
                .snapshotRequested(true)
                .build();

        assertThrows(CfnNotStabilizedException.class,
                () -> handler.handleRequest(proxy, request, new CallbackContext(), proxyRdsClient, logger));
    }

    @Test
    public void handleRequest_globalClusterIdentifier_beforeDelete() {
        final DeleteDbClusterResponse deleteDbClusterRequest = DeleteDbClusterResponse.builder().build();
        when(proxyRdsClient.client().deleteDBCluster(any(DeleteDbClusterRequest.class))).thenReturn(deleteDbClusterRequest);
        final DescribeDbClustersResponse describeInProgressDbClustersResponse = DescribeDbClustersResponse.builder().dbClusters(DBCLUSTER_INPROGRESS).build();
        final DescribeDbClustersResponse describeAvailableDbClustersResponse = DescribeDbClustersResponse.builder().dbClusters(DBCLUSTER_ACTIVE).build();
        final RemoveFromGlobalClusterResponse removeFromGlobalClusterResponse = RemoveFromGlobalClusterResponse.builder().globalCluster(GLOBAL_CLUSTER).build();
        final CreateDbClusterSnapshotResponse createDbClusterSnapshotResponse = CreateDbClusterSnapshotResponse.builder()
                .dbClusterSnapshot(DBCLUSTER_SNAPSHOT).build();
        final DescribeDbClusterSnapshotsResponse describeDbClusterSnapshotsCreatingResponse = DescribeDbClusterSnapshotsResponse.builder()
                .dbClusterSnapshots(ImmutableList.of(DBCLUSTER_SNAPSHOT_CREATING)).build();
        final DescribeDbClusterSnapshotsResponse describeDbClusterSnapshotsAvailableResponse = DescribeDbClusterSnapshotsResponse.builder()
                .dbClusterSnapshots(ImmutableList.of(DBCLUSTER_SNAPSHOT_AVAILABLE)).build();
        when(proxyRdsClient.client().describeDBClusterSnapshots(any(DescribeDbClusterSnapshotsRequest.class)))
                .thenReturn(describeDbClusterSnapshotsCreatingResponse)
                .thenReturn(describeDbClusterSnapshotsAvailableResponse);
        when(proxyRdsClient.client().removeFromGlobalCluster(any(RemoveFromGlobalClusterRequest.class))).thenReturn(removeFromGlobalClusterResponse);
        when(proxyRdsClient.client().createDBClusterSnapshot(any(CreateDbClusterSnapshotRequest.class))).thenReturn(createDbClusterSnapshotResponse);
        when(proxyRdsClient.client().describeDBClusters(any(DescribeDbClustersRequest.class)))
                .thenReturn(describeInProgressDbClustersResponse)
                .thenReturn(describeAvailableDbClustersResponse)
                .thenThrow(DbClusterNotFoundException.class);
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL_WITH_GLOBAL_CLUSTER)
                .logicalResourceIdentifier("dbcluster")
                .clientRequestToken("request")
                .snapshotRequested(true)
                .build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyRdsClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyRdsClient.client()).deleteDBCluster(any(DeleteDbClusterRequest.class));
        verify(proxyRdsClient.client(), times(3)).describeDBClusters(any(DescribeDbClustersRequest.class));
    }

}
