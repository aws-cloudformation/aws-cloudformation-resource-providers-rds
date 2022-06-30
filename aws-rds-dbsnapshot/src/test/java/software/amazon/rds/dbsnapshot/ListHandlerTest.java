package software.amazon.rds.dbsnapshot;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Collections;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import lombok.Getter;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBSnapshot;
import software.amazon.awssdk.services.rds.model.DescribeDbSnapshotsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbSnapshotsResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.rds.common.handler.HandlerConfig;

@ExtendWith(MockitoExtension.class)
public class ListHandlerTest extends AbstractHandlerTest {
    final String DB_SNAPSHOT_IDENTIFIER = "test-db-snapshot-identifier";
    final String DB_INSTANCE_IDENTIFIER = "test-db-instance-identifier";


    final String DESCRIBE_DB_SNAPSHOT_MARKER = "test-describe-db-snapshots-marker";
    @Mock
    @Getter
    private AmazonWebServicesClientProxy proxy;
    @Mock
    @Getter
    private ProxyClient<RdsClient> rdsProxy;
    @Mock
    private RdsClient rdsClient;
    @Getter
    private ListHandler handler;

    @BeforeEach
    public void setup() {
        handler = new ListHandler(HandlerConfig.builder().backoff(TEST_BACKOFF_DELAY).build());
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        rdsClient = mock(RdsClient.class);
        rdsProxy = mockProxy(proxy, rdsClient);
    }

    @AfterEach
    public void tear_down() {
        verify(rdsClient, atLeastOnce()).serviceName();
        verifyNoMoreInteractions(rdsClient);
    }

    @Test
    public void handleRequest_SimpleSuccess() {
        final DescribeDbSnapshotsResponse describeDbSnapshotsResponse = DescribeDbSnapshotsResponse.builder()
                .dbSnapshots(Collections.singletonList(
                        DBSnapshot.builder()
                                .dbSnapshotIdentifier(DB_SNAPSHOT_IDENTIFIER)
                                .dbInstanceIdentifier(DB_INSTANCE_IDENTIFIER)
                                .build()
                ))
                .marker(DESCRIBE_DB_SNAPSHOT_MARKER)
                .build();
        when(rdsProxy.client().describeDBSnapshots(any(DescribeDbSnapshotsRequest.class)))
                .thenReturn(describeDbSnapshotsResponse);

        final ProgressEvent<ResourceModel, CallbackContext> response = test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> RESOURCE_MODEL_BUILDER().tags(TAG_LIST).build(),
                expectSuccess()
        );

        final ResourceModel expectedModel = ResourceModel.builder()
                .dBSnapshotIdentifier(DB_SNAPSHOT_IDENTIFIER)
                .dBInstanceIdentifier(DB_INSTANCE_IDENTIFIER)
                .tags(TAG_LIST_EMPTY)
                .build();

        assertThat(response.getResourceModels()).isNotNull();
        assertThat(response.getResourceModels()).containsExactly(expectedModel);
        assertThat(response.getNextToken()).isEqualTo(DESCRIBE_DB_SNAPSHOT_MARKER);

        verify(rdsProxy.client()).describeDBSnapshots(any(DescribeDbSnapshotsRequest.class));
    }
}
