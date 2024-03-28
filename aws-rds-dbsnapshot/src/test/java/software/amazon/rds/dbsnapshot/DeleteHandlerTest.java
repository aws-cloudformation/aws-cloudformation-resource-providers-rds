package software.amazon.rds.dbsnapshot;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import lombok.Getter;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DbSnapshotNotFoundException;
import software.amazon.awssdk.services.rds.model.DeleteDbSnapshotRequest;
import software.amazon.awssdk.services.rds.model.DeleteDbSnapshotResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbSnapshotsRequest;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.rds.common.handler.HandlerConfig;

@ExtendWith(MockitoExtension.class)
public class DeleteHandlerTest extends AbstractHandlerTest {

    @Mock
    @Getter
    private AmazonWebServicesClientProxy proxy;

    @Mock
    @Getter
    private ProxyClient<RdsClient> rdsProxy;

    @Mock
    RdsClient rdsClient;

    @Getter
    private DeleteHandler handler;

    @BeforeEach
    public void setup() {
        handler = new DeleteHandler(HandlerConfig.builder().backoff(TEST_BACKOFF_DELAY).build());
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
        when(rdsProxy.client().deleteDBSnapshot(any(DeleteDbSnapshotRequest.class)))
                .thenReturn(DeleteDbSnapshotResponse.builder().build());
        when(rdsProxy.client().describeDBSnapshots(any(DescribeDbSnapshotsRequest.class)))
                .thenThrow(DbSnapshotNotFoundException.builder().build());

        final ProgressEvent<ResourceModel, CallbackContext> response = test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> RESOURCE_MODEL,
                expectSuccess()
        );

        assertThat(response.getMessage()).isNull();

        verify(rdsProxy.client(), times(1)).deleteDBSnapshot(any(DeleteDbSnapshotRequest.class));
    }

    @Test
    public void handleRequest_NotFound() {
        when(rdsProxy.client().deleteDBSnapshot(any(DeleteDbSnapshotRequest.class)))
                .thenThrow(DbSnapshotNotFoundException.builder().message(MSG_NOT_FOUND).build());

        final ProgressEvent<ResourceModel, CallbackContext> response = test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> RESOURCE_MODEL,
                expectFailed(HandlerErrorCode.NotFound)
        );

        verify(rdsProxy.client(), times(1)).deleteDBSnapshot(any(DeleteDbSnapshotRequest.class));
    }

    @Test
    public void handleRequest_IsDeleting_Stabilize() {

        final DeleteDbSnapshotResponse deleteDbSnapshotResponse = DeleteDbSnapshotResponse.builder().build();
        when(rdsProxy.client().deleteDBSnapshot(any(DeleteDbSnapshotRequest.class))).thenReturn(deleteDbSnapshotResponse);

        AtomicBoolean fetchedOnce = new AtomicBoolean(false);
        final ProgressEvent<ResourceModel, CallbackContext> response = test_handleRequest_base(
                new CallbackContext(),
                () -> {
                    if (fetchedOnce.compareAndSet(false, true)) {
                        return DB_SNAPSHOT_DELETING;
                    }
                    throw DbSnapshotNotFoundException.builder().build();
                },
                () -> RESOURCE_MODEL_BUILDER().tags(TAG_LIST).build(),
                expectSuccess()
        );

        assertThat(response.getMessage()).isNull();

        verify(rdsProxy.client(), times(1)).deleteDBSnapshot(any(DeleteDbSnapshotRequest.class));
        verify(rdsProxy.client(), times(2)).describeDBSnapshots(any(DescribeDbSnapshotsRequest.class));
    }
}
