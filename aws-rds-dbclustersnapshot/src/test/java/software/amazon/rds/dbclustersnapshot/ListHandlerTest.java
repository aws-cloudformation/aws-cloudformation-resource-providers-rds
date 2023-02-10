package software.amazon.rds.dbclustersnapshot;

import lombok.Getter;
import org.junit.jupiter.api.AfterEach;
import org.mockito.Mockito;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBClusterSnapshot;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterSnapshotsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterSnapshotsResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.test.common.core.HandlerName;

import java.time.Duration;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ListHandlerTest extends AbstractHandlerTest {
    final String DB_CLUSTER_SNAPSHOT_IDENTIFIER = "test-db-cluster-snapshot-identifier";
    final String DB_CLUSTER_IDENTIFIER = "test-db-cluster-identifier";


    final String DESCRIBE_DB_CLUSTER_SNAPSHOT_MARKER = "test-describe-db-cluster-snapshots-marker";

    @Mock
    @Getter
    private AmazonWebServicesClientProxy proxy;

    @Mock
    @Getter
    private ProxyClient<RdsClient> proxyClient;

    @Mock
    RdsClient rdsClient;

    @Getter
    private ListHandler handler;

    @Override
    public HandlerName getHandlerName() { return HandlerName.LIST; }

    @BeforeEach
    public void setup() {
        handler = new ListHandler(
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
    public void handleRequest_SimpleSuccess() { // FIXME: Really not sure what this test is exactly running
        final DescribeDbClusterSnapshotsResponse describeDbClusterSnapshotsResponse = DescribeDbClusterSnapshotsResponse.builder()
                .dbClusterSnapshots(Collections.singletonList(
                        DBClusterSnapshot.builder()
                                .dbClusterSnapshotIdentifier(DB_CLUSTER_SNAPSHOT_IDENTIFIER)
                                .dbClusterIdentifier(DB_CLUSTER_IDENTIFIER)
                                .build()
                ))
                .marker(DESCRIBE_DB_CLUSTER_SNAPSHOT_MARKER)
                .build();
        when(rdsClient.describeDBClusterSnapshots(any(DescribeDbClusterSnapshotsRequest.class)))
                .thenReturn(describeDbClusterSnapshotsResponse);

        final ProgressEvent<ResourceModel, CallbackContext> response = test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> RESOURCE_MODEL().toBuilder().tags(TAG_LIST).build(),
                expectSuccess()
        );

        final ResourceModel expectedModel = ResourceModel.builder()
                .dBClusterSnapshotIdentifier(DB_CLUSTER_SNAPSHOT_IDENTIFIER)
                .dBClusterIdentifier(DB_CLUSTER_IDENTIFIER)
                .tags(TAG_LIST_EMPTY)
                .build();

        assertThat(response.getResourceModels()).isNotNull();
        assertThat(response.getResourceModels()).containsExactly(expectedModel);
        assertThat(response.getNextToken()).isEqualTo(DESCRIBE_DB_CLUSTER_SNAPSHOT_MARKER);

        verify(rdsClient).describeDBClusterSnapshots(any(DescribeDbClusterSnapshotsRequest.class));
    }
}
