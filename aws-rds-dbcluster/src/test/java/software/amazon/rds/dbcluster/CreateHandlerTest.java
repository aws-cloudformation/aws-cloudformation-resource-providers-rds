package software.amazon.rds.dbcluster;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import lombok.Getter;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.AddRoleToDbClusterRequest;
import software.amazon.awssdk.services.rds.model.AddRoleToDbClusterResponse;
import software.amazon.awssdk.services.rds.model.CreateDbClusterRequest;
import software.amazon.awssdk.services.rds.model.CreateDbClusterResponse;
import software.amazon.awssdk.services.rds.model.DBCluster;
import software.amazon.awssdk.services.rds.model.DbClusterAlreadyExistsException;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersRequest;
import software.amazon.awssdk.services.rds.model.ModifyDbClusterRequest;
import software.amazon.awssdk.services.rds.model.ModifyDbClusterResponse;
import software.amazon.awssdk.services.rds.model.RestoreDbClusterFromSnapshotRequest;
import software.amazon.awssdk.services.rds.model.RestoreDbClusterFromSnapshotResponse;
import software.amazon.awssdk.services.rds.model.RestoreDbClusterToPointInTimeRequest;
import software.amazon.awssdk.services.rds.model.RestoreDbClusterToPointInTimeResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.delay.Constant;
import software.amazon.rds.common.handler.HandlerConfig;

@ExtendWith(MockitoExtension.class)
public class CreateHandlerTest extends AbstractHandlerTest {

    @Mock
    @Getter
    private AmazonWebServicesClientProxy proxy;

    @Mock
    @Getter
    private ProxyClient<RdsClient> rdsProxy;

    @Mock
    @Getter
    RdsClient rdsClient;

    @Getter
    private CreateHandler handler;

    @BeforeEach
    public void setup() {
        handler = new CreateHandler(
                HandlerConfig.builder()
                        .backoff(Constant.of()
                                .delay(Duration.ofSeconds(1))
                                .timeout(Duration.ofSeconds(120))
                                .build())
                        .build()
        );
        rdsClient = mock(RdsClient.class);
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        rdsProxy = MOCK_PROXY(proxy, rdsClient);
    }

    @AfterEach
    public void tear_down() {
        verify(rdsClient, atLeastOnce()).serviceName();
        verifyNoMoreInteractions(rdsClient);
    }

    @Test
    public void handleRequest_SimpleSuccess() {
        when(rdsProxy.client().createDBCluster(any(CreateDbClusterRequest.class)))
                .thenReturn(CreateDbClusterResponse.builder().build());
        when(rdsProxy.client().addRoleToDBCluster(any(AddRoleToDbClusterRequest.class)))
                .thenReturn(AddRoleToDbClusterResponse.builder().build());

        test_handleRequest_base(
                new CallbackContext(),
                () -> DBCLUSTER_ACTIVE,
                () -> RESOURCE_MODEL,
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).createDBCluster(any(CreateDbClusterRequest.class));
        verify(rdsProxy.client(), times(1)).addRoleToDBCluster(any(AddRoleToDbClusterRequest.class));
        verify(rdsProxy.client(), times(3)).describeDBClusters(any(DescribeDbClustersRequest.class));
    }

    @Test
    public void handleRequest_CreateWithStabilizationSuccess() {
        when(rdsProxy.client().createDBCluster(any(CreateDbClusterRequest.class)))
                .thenReturn(CreateDbClusterResponse.builder().build());
        when(rdsProxy.client().addRoleToDBCluster(any(AddRoleToDbClusterRequest.class)))
                .thenReturn(AddRoleToDbClusterResponse.builder().build());

        Queue<DBCluster> transitions = new ConcurrentLinkedQueue<>();
        transitions.add(DBCLUSTER_INPROGRESS);

        test_handleRequest_base(
                new CallbackContext(),
                () -> {
                    if (transitions.size() > 0) {
                        return transitions.remove();
                    }
                    return DBCLUSTER_ACTIVE;
                },
                () -> RESOURCE_MODEL,
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).createDBCluster(any(CreateDbClusterRequest.class));
        verify(rdsProxy.client(), times(1)).addRoleToDBCluster(any(AddRoleToDbClusterRequest.class));
        verify(rdsProxy.client(), times(4)).describeDBClusters(any(DescribeDbClustersRequest.class));
    }

    @Test
    public void handleRequest_Cluster_AlreadyExist() {
        when(rdsProxy.client().createDBCluster(any(CreateDbClusterRequest.class)))
                .thenThrow(DbClusterAlreadyExistsException.builder().message("already exists").build());

        test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> RESOURCE_MODEL,
                expectFailed(HandlerErrorCode.AlreadyExists)
        );

        verify(rdsProxy.client(), times(1)).createDBCluster(any(CreateDbClusterRequest.class));
    }

    @Test
    public void handleRequest_Cluster_handleFailure() {
        when(rdsProxy.client().createDBCluster(any(CreateDbClusterRequest.class)))
                .thenThrow(new RuntimeException("test exception"));

        test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> RESOURCE_MODEL,
                expectFailed(HandlerErrorCode.InternalFailure)
        );

        verify(rdsProxy.client(), times(1)).createDBCluster(any(CreateDbClusterRequest.class));
    }

    @Test
    public void handleRequest_ClusterRestoreInProgress() {
        when(rdsProxy.client().restoreDBClusterFromSnapshot(any(RestoreDbClusterFromSnapshotRequest.class)))
                .thenReturn(RestoreDbClusterFromSnapshotResponse.builder().build());
        when(rdsProxy.client().modifyDBCluster(any(ModifyDbClusterRequest.class)))
                .thenReturn(ModifyDbClusterResponse.builder().build());

        test_handleRequest_base(
                new CallbackContext(),
                () -> DBCLUSTER_ACTIVE,
                () -> RESOURCE_MODEL_ON_RESTORE,
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).restoreDBClusterFromSnapshot(any(RestoreDbClusterFromSnapshotRequest.class));
        verify(rdsProxy.client(), times(1)).modifyDBCluster(any(ModifyDbClusterRequest.class));
        verify(rdsProxy.client(), times(3)).describeDBClusters(any(DescribeDbClustersRequest.class));
    }

    @Test
    public void handleRequest_ClusterRestoreSuccess() {
        when(rdsProxy.client().restoreDBClusterFromSnapshot(any(RestoreDbClusterFromSnapshotRequest.class)))
                .thenReturn(RestoreDbClusterFromSnapshotResponse.builder().build());

        final CallbackContext context = new CallbackContext();
        context.setModified(true);

        test_handleRequest_base(
                context,
                () -> DBCLUSTER_ACTIVE,
                () -> RESOURCE_MODEL_ON_RESTORE,
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).restoreDBClusterFromSnapshot(any(RestoreDbClusterFromSnapshotRequest.class));
        verify(rdsProxy.client(), times(2)).describeDBClusters(any(DescribeDbClustersRequest.class));
    }

    @Test
    public void handleRequest_ClusterRestore_AlreadyExists() {
        when(rdsProxy.client().restoreDBClusterFromSnapshot(any(RestoreDbClusterFromSnapshotRequest.class)))
                .thenThrow(DbClusterAlreadyExistsException.builder().message("already exists").build());

        final CallbackContext context = new CallbackContext();
        context.setModified(true);

        test_handleRequest_base(
                context,
                null,
                () -> RESOURCE_MODEL_ON_RESTORE,
                expectFailed(HandlerErrorCode.AlreadyExists)
        );

        verify(rdsProxy.client(), times(1)).restoreDBClusterFromSnapshot(any(RestoreDbClusterFromSnapshotRequest.class));
    }

    @Test
    public void handleRequest_ClusterRestore_handleFailure() {
        when(rdsProxy.client().restoreDBClusterFromSnapshot(any(RestoreDbClusterFromSnapshotRequest.class)))
                .thenThrow(new RuntimeException("test exception"));

        final CallbackContext context = new CallbackContext();
        context.setModified(true);

        test_handleRequest_base(
                context,
                null,
                () -> RESOURCE_MODEL_ON_RESTORE,
                expectFailed(HandlerErrorCode.InternalFailure)
        );

        verify(rdsProxy.client(), times(1)).restoreDBClusterFromSnapshot(any(RestoreDbClusterFromSnapshotRequest.class));
    }

    @Test
    public void handleRequest_ClusterRestoreInTimeSuccess() {
        when(rdsProxy.client().restoreDBClusterToPointInTime(any(RestoreDbClusterToPointInTimeRequest.class)))
                .thenReturn(RestoreDbClusterToPointInTimeResponse.builder().build());

        test_handleRequest_base(
                new CallbackContext(),
                () -> DBCLUSTER_ACTIVE,
                () -> RESOURCE_MODEL_ON_RESTORE_IN_TIME,
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).restoreDBClusterToPointInTime(any(RestoreDbClusterToPointInTimeRequest.class));
        verify(rdsProxy.client(), times(2)).describeDBClusters(any(DescribeDbClustersRequest.class));
    }

    @Test
    public void handleRequest_ClusterRestoreInTime_AlreadyExist() {
        when(rdsProxy.client().restoreDBClusterToPointInTime(any(RestoreDbClusterToPointInTimeRequest.class)))
                .thenThrow(DbClusterAlreadyExistsException.builder().message("already exists").build());

        test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> RESOURCE_MODEL_ON_RESTORE_IN_TIME,
                expectFailed(HandlerErrorCode.AlreadyExists)
        );

        verify(rdsProxy.client(), times(1)).restoreDBClusterToPointInTime(any(RestoreDbClusterToPointInTimeRequest.class));
    }

    @Test
    public void handleRequest_ClusterRestoreInTime_handlerFailure() {
        when(rdsProxy.client().restoreDBClusterToPointInTime(any(RestoreDbClusterToPointInTimeRequest.class)))
                .thenThrow(new RuntimeException("test exception"));

        test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> RESOURCE_MODEL_ON_RESTORE_IN_TIME,
                expectFailed(HandlerErrorCode.InternalFailure)
        );

        verify(rdsProxy.client(), times(1)).restoreDBClusterToPointInTime(any(RestoreDbClusterToPointInTimeRequest.class));
    }
}
