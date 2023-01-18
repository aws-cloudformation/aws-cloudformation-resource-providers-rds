package software.amazon.rds.dbclustersnapshot;

import java.time.Duration;

import com.google.common.collect.ImmutableList;
import lombok.Getter;
import org.mockito.Mockito;
import software.amazon.awssdk.core.SdkClient;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.AddRoleToDbClusterRequest;
import software.amazon.awssdk.services.rds.model.AddRoleToDbClusterResponse;
import software.amazon.awssdk.services.rds.model.CreateDbClusterEndpointRequest;
import software.amazon.awssdk.services.rds.model.CreateDbClusterEndpointResponse;
import software.amazon.awssdk.services.rds.model.CreateDbClusterRequest;
import software.amazon.awssdk.services.rds.model.CreateDbClusterResponse;
import software.amazon.awssdk.services.rds.model.CreateDbClusterSnapshotRequest;
import software.amazon.awssdk.services.rds.model.CreateDbClusterSnapshotResponse;
import software.amazon.awssdk.services.rds.model.CreateOptionGroupRequest;
import software.amazon.awssdk.services.rds.model.CreateOptionGroupResponse;
import software.amazon.awssdk.services.rds.model.DBCluster;
import software.amazon.awssdk.services.rds.model.DBClusterSnapshot;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterSnapshotsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterSnapshotsResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersResponse;
import software.amazon.awssdk.services.rds.model.DescribeOptionGroupsRequest;
import software.amazon.awssdk.services.rds.model.DescribeOptionGroupsResponse;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.test.common.core.HandlerName;
import software.amazon.rds.test.common.core.TestUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.atLeastOnce;

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
                () -> DB_CLUSTER_SNAPSHOT_ACTIVE,
                () -> RESOURCE_MODEL,
                expectSuccess()
        );

        verify(proxyClient.client(), times(1)).createDBClusterSnapshot(any(CreateDbClusterSnapshotRequest.class));
        verify(proxyClient.client(), times(2)).describeDBClusterSnapshots(any(DescribeDbClusterSnapshotsRequest.class));
        verify(proxyClient.client(), times(1)).describeDBClusters(any(DescribeDbClustersRequest.class));
    }
}
