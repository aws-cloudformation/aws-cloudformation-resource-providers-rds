package software.amazon.rds.dbclustersnapshot;

import java.time.Duration;

import lombok.Getter;
import org.mockito.Mockito;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.CopyDbClusterSnapshotRequest;
import software.amazon.awssdk.services.rds.model.CopyDbClusterSnapshotResponse;
import software.amazon.awssdk.services.rds.model.CreateDbClusterSnapshotRequest;
import software.amazon.awssdk.services.rds.model.CreateDbClusterSnapshotResponse;
import software.amazon.awssdk.services.rds.model.DBCluster;
import software.amazon.awssdk.services.rds.model.DBClusterSnapshot;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterSnapshotsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ProxyClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.test.common.core.HandlerName;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class CreateHandlerTest extends AbstractHandlerTest {
    @Mock
    @Getter
    private AmazonWebServicesClientProxy proxy;

    @Mock
    @Getter
    private ProxyClient<RdsClient> proxyClient;

    @Mock
    RdsClient rdsClient;

    @Getter
    private CreateHandler handler;

    @Override
    public HandlerName getHandlerName() { return HandlerName.CREATE; }

    @BeforeEach
    public void setup() {
        handler = new CreateHandler(
                HandlerConfig.builder()
                        .backoff(TEST_BACKOFF_DELAY)
                        .build()
        );
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        rdsClient = mock(RdsClient.class);
        proxyClient = MOCK_PROXY(proxy, rdsClient);
    }

    @AfterEach
    public void tear_down() {
        verify(rdsClient, Mockito.atLeastOnce()).serviceName();
        verifyNoMoreInteractions(rdsClient);
        verifyAccessPermissions(rdsClient);
    }

    @Test
    public void handleRequest_CreateDbCluster_Success() {
        when(proxyClient.client().createDBClusterSnapshot(any(CreateDbClusterSnapshotRequest.class)))
                .thenReturn(CreateDbClusterSnapshotResponse.builder().dbClusterSnapshot(
                        DBClusterSnapshot.builder().build()
                ).build());

        when(proxyClient.client().describeDBClusters(any(DescribeDbClustersRequest.class)))
                .thenReturn(DescribeDbClustersResponse.builder().dbClusters(DBCluster.builder().status("available").build()).build());

        test_handleRequest_base(
                new CallbackContext(),
                () -> DB_CLUSTER_SNAPSHOT_ACTIVE(),
                () -> RESOURCE_MODEL(),
                expectSuccess()
        );

        verify(proxyClient.client(), times(1)).createDBClusterSnapshot(any(CreateDbClusterSnapshotRequest.class));
        verify(proxyClient.client(), times(2)).describeDBClusterSnapshots(any(DescribeDbClusterSnapshotsRequest.class));
        verify(proxyClient.client(), times(1)).describeDBClusters(any(DescribeDbClustersRequest.class));
    }

    @Test
    public void handleRequest_CreateDbCluster_NoClusters_Success() {
        when(proxyClient.client().createDBClusterSnapshot(any(CreateDbClusterSnapshotRequest.class)))
                .thenReturn(CreateDbClusterSnapshotResponse.builder().dbClusterSnapshot(
                        DBClusterSnapshot.builder().build()
                ).build());

        when(proxyClient.client().describeDBClusters(any(DescribeDbClustersRequest.class)))
                .thenReturn(DescribeDbClustersResponse.builder().build());

        test_handleRequest_base(
                new CallbackContext(),
                () -> DB_CLUSTER_SNAPSHOT_ACTIVE(),
                () -> RESOURCE_MODEL(),
                expectSuccess()
        );

        verify(proxyClient.client(), times(1)).createDBClusterSnapshot(any(CreateDbClusterSnapshotRequest.class));
        verify(proxyClient.client(), times(2)).describeDBClusterSnapshots(any(DescribeDbClusterSnapshotsRequest.class));
        verify(proxyClient.client(), times(1)).describeDBClusters(any(DescribeDbClustersRequest.class));
    }

    @Test
    public void handleRequest_CopyDbCluster_Success() {
        when(proxyClient.client().copyDBClusterSnapshot(any(CopyDbClusterSnapshotRequest.class)))
                .thenReturn(CopyDbClusterSnapshotResponse.builder().dbClusterSnapshot(
                        DBClusterSnapshot.builder().build()
                ).build());

        when(proxyClient.client().describeDBClusters(any(DescribeDbClustersRequest.class)))
                .thenReturn(DescribeDbClustersResponse.builder().build());

        test_handleRequest_base(
                new CallbackContext(),
                () -> DB_CLUSTER_SNAPSHOT_ACTIVE().toBuilder()
                        .dbClusterSnapshotIdentifier(null)
                        .build(),
                () -> RESOURCE_MODEL().toBuilder()
                        .dBClusterIdentifier("db-cluster-identifier")
                        .dBClusterSnapshotIdentifier(null)
                        .sourceDBClusterSnapshotIdentifier("source-db-custer-snapshot-identifier")
                        .build(),
                expectSuccess()
        );

        verify(proxyClient.client(), times(1)).copyDBClusterSnapshot(any(CopyDbClusterSnapshotRequest.class));
        verify(proxyClient.client(), times(2)).describeDBClusterSnapshots(any(DescribeDbClusterSnapshotsRequest.class));
        verify(proxyClient.client(), times(1)).describeDBClusters(any(DescribeDbClustersRequest.class));
    }
}
