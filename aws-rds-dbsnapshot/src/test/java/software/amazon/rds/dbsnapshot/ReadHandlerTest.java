package software.amazon.rds.dbsnapshot;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import lombok.Getter;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DbSnapshotNotFoundException;
import software.amazon.awssdk.services.rds.model.DescribeDbSnapshotsRequest;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.rds.common.handler.HandlerConfig;

@ExtendWith(MockitoExtension.class)
public class ReadHandlerTest extends AbstractHandlerTest {
    @Mock
    @Getter
    private AmazonWebServicesClientProxy proxy;

    @Mock
    @Getter
    private ProxyClient<RdsClient> rdsProxy;

    @Mock
    RdsClient rdsClient;

    @Getter
    private ReadHandler handler;

    @BeforeEach
    public void setup() {
        handler = new ReadHandler(HandlerConfig.builder().backoff(TEST_BACKOFF_DELAY).build());
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
    public void handleRequest_ReadSuccess() {
        final software.amazon.cloudformation.proxy.ProgressEvent<ResourceModel, CallbackContext> progress = test_handleRequest_base(
                new CallbackContext(),
                () -> DB_SNAPSHOT_AVAILABLE,
                () -> RESOURCE_MODEL_BUILDER().tags(TAG_LIST).build(),
                expectSuccess()
        );
        progress.getResourceModel();
        verify(rdsProxy.client(), times(1)).describeDBSnapshots(any(DescribeDbSnapshotsRequest.class));
    }

    @Test
    public void handleRequest_NotFound() {
        when(rdsProxy.client().describeDBSnapshots(any(DescribeDbSnapshotsRequest.class)))
                .thenThrow(DbSnapshotNotFoundException.builder().message("NotFound").build());

        test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> RESOURCE_MODEL,
                expectFailed(HandlerErrorCode.NotFound)
        );

        verify(rdsProxy.client(), times(1)).describeDBSnapshots(any(DescribeDbSnapshotsRequest.class));
    }

    @Test
    public void handleRequest_RuntimeException() {
        when(rdsProxy.client().describeDBSnapshots(any(DescribeDbSnapshotsRequest.class)))
                .thenThrow(new RuntimeException("test exception"));

        test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> RESOURCE_MODEL,
                expectFailed(HandlerErrorCode.InternalFailure)
        );

        verify(rdsProxy.client(), times(1)).describeDBSnapshots(any(DescribeDbSnapshotsRequest.class));
    }
}
