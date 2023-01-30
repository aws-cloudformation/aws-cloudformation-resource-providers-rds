package software.amazon.rds.dbclustersnapshot;

import java.time.Duration;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableList;
import lombok.Getter;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterSnapshotsRequest;
import software.amazon.awssdk.services.rds.model.RemoveTagsFromResourceRequest;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@ExtendWith(MockitoExtension.class)
public class UpdateHandlerTest extends AbstractHandlerTest {

    @Mock
    @Getter
    private AmazonWebServicesClientProxy proxy;

    @Mock
    @Getter
    private ProxyClient<RdsClient> proxyClient;

    @Mock
    RdsClient rdsClient;

    private Boolean expectServiceInvocation;

    @Getter
    private UpdateHandler handler;

    @Override
    public HandlerName getHandlerName() {
        return HandlerName.UPDATE;
    }

    @BeforeEach
    public void setup() {
        handler = new UpdateHandler(
                HandlerConfig.builder()
                        .backoff(TEST_BACKOFF_DELAY)
                        .build()
        );
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        rdsClient = mock(RdsClient.class);
        proxyClient = MOCK_PROXY(proxy, rdsClient);

        expectServiceInvocation = true;
    }

    @AfterEach
    public void tear_down() {
        if (expectServiceInvocation) {
            verify(rdsClient, atLeastOnce()).serviceName();
        }

        verifyNoMoreInteractions(rdsClient);
        verifyAccessPermissions(rdsClient);
    }

    @Test
    public void handleRequest_SimpleSuccess() {
        expectServiceInvocation = false;

//        final CallbackContext context = new CallbackContext();
//        test_handleRequest_base(
//                context,
//                ResourceHandlerRequest.<ResourceModel>builder().rollback(true),
//                null,
//                () -> RESOURCE_MODEL,
//                () -> RESOURCE_MODEL,
//                expectFailed(HandlerErrorCode.NotUpdatable)
//        );
    }

    @Test
    public void handleRequest_AddTagToResource() {
        final CallbackContext context = new CallbackContext();

        final Map<String, String> previousTags = translateTagsToRequest(TAG_LIST);
        final Map<String, String> desiredTags = translateTagsToRequest(TAG_LIST);
        desiredTags.put("newKey", "newValue");

        test_handleRequest_base(
                context,
                ResourceHandlerRequest.<ResourceModel>builder()
                        .previousResourceTags(previousTags)
                        .desiredResourceTags(desiredTags),
                () -> DB_CLUSTER_SNAPSHOT_ACTIVE(),
                () -> RESOURCE_MODEL().toBuilder().build(),
                () -> RESOURCE_MODEL().toBuilder().build(),
                expectSuccess()
        );

        verify(rdsClient, times(1)).describeDBClusterSnapshots(any(DescribeDbClusterSnapshotsRequest.class));
        verify(rdsClient, times(1)).addTagsToResource(any(AddTagsToResourceRequest.class));
    }

    @Test
    public void handleRequest_RemoveTagFromResource() {
        final CallbackContext context = new CallbackContext();

        final Map<String, String> previousTags = translateTagsToRequest(TAG_LIST);
        previousTags.put("newKey", "newValue");
        final Map<String, String> desiredTags = translateTagsToRequest(TAG_LIST);

        test_handleRequest_base(
                context,
                ResourceHandlerRequest.<ResourceModel>builder()
                        .previousResourceTags(previousTags)
                        .desiredResourceTags(desiredTags),
                () -> DB_CLUSTER_SNAPSHOT_ACTIVE(),
                () -> RESOURCE_MODEL().toBuilder().build(),
                () -> RESOURCE_MODEL().toBuilder().build(),
                expectSuccess()
        );

        verify(rdsClient, times(1)).describeDBClusterSnapshots(any(DescribeDbClusterSnapshotsRequest.class));
        verify(rdsClient, times(1)).removeTagsFromResource(any(RemoveTagsFromResourceRequest.class));
    }

    @Test
    public void handleRequest_AddSourceClusterSnapshotIdentifier() {
        expectServiceInvocation = false;

        test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> RESOURCE_MODEL().toBuilder()
                        .sourceDBClusterSnapshotIdentifier(null)
                        .build(),
                () -> RESOURCE_MODEL().toBuilder()
                        .sourceDBClusterSnapshotIdentifier("source-db-cluster-snapshot-identifier")
                        .build(),
                expectFailed(HandlerErrorCode.NotUpdatable)
        );
    }

    @Test
    public void handleRequest_RemoveClusterSnapshotIdentifier() {
        test_handleRequest_base(
                new CallbackContext(),
                () -> DB_CLUSTER_SNAPSHOT_ACTIVE(),
                () -> RESOURCE_MODEL().toBuilder()
                        .sourceDBClusterSnapshotIdentifier("source-db-cluster-snapshot-identifier")
                        .build(),
                () -> RESOURCE_MODEL().toBuilder()
                        .sourceDBClusterSnapshotIdentifier(null)
                        .build(),
                expectSuccess()
        );

        verify(rdsClient, times(1)).describeDBClusterSnapshots(any(DescribeDbClusterSnapshotsRequest.class));
    }

    @Test
    public void handleRequest_ChangeSourceClusterSnapshotIdentifier() {
        expectServiceInvocation = false;

        test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> RESOURCE_MODEL().toBuilder()
                        .sourceDBClusterSnapshotIdentifier("source-db-cluster-snapshot-identifier")
                        .build(),
                () -> RESOURCE_MODEL().toBuilder()
                        .sourceDBClusterSnapshotIdentifier("changed-source-db-cluster-snapshot-identifier")
                        .build(),
                expectFailed(HandlerErrorCode.NotUpdatable)
        );
    }

    @Test
    public void handleRequest_NoChangeSourceClusterSnapshotIdentifier() {
        test_handleRequest_base(
                new CallbackContext(),
                () -> DB_CLUSTER_SNAPSHOT_ACTIVE(),
                () -> RESOURCE_MODEL().toBuilder()
                        .sourceDBClusterSnapshotIdentifier("source-db-cluster-snapshot-identifier")
                        .build(),
                () -> RESOURCE_MODEL().toBuilder()
                        .sourceDBClusterSnapshotIdentifier("source-db-cluster-snapshot-identifier")
                        .build(),
                expectSuccess()
        );

        verify(rdsClient, times(1)).describeDBClusterSnapshots(any(DescribeDbClusterSnapshotsRequest.class));
    }
}
