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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import lombok.Getter;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBCluster;
import software.amazon.awssdk.services.rds.model.DbClusterNotFoundException;
import software.amazon.awssdk.services.rds.model.DeleteDbClusterRequest;
import software.amazon.awssdk.services.rds.model.DeleteDbClusterResponse;
import software.amazon.awssdk.services.rds.model.RemoveFromGlobalClusterRequest;
import software.amazon.awssdk.services.rds.model.RemoveFromGlobalClusterResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.delay.Constant;
import software.amazon.rds.common.handler.HandlerConfig;

@ExtendWith(MockitoExtension.class)
public class DeleteHandlerTest extends AbstractHandlerTest {

    private static final String MSG_NOT_FOUND = "not found";

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
    private DeleteHandler handler;

    private boolean expectServiceInvocation;

    @BeforeEach
    public void setup() {
        handler = new DeleteHandler(
                HandlerConfig.builder()
                        .probingEnabled(false)
                        .backoff(Constant.of()
                                .delay(Duration.ofSeconds(1))
                                .timeout(Duration.ofSeconds(120))
                                .build())
                        .build()
        );
        rdsClient = mock(RdsClient.class);
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        rdsProxy = MOCK_PROXY(proxy, rdsClient);
        expectServiceInvocation = true;
    }

    @AfterEach
    public void tear_down() {
        if (expectServiceInvocation) {
            verify(rdsClient, atLeastOnce()).serviceName();
        }
        verifyNoMoreInteractions(rdsClient);
    }

    @Test
    public void handleRequest_SimpleSuccess() {
        when(rdsProxy.client().deleteDBCluster(any(DeleteDbClusterRequest.class)))
                .thenReturn(DeleteDbClusterResponse.builder().build());

        final Queue<DBCluster> transitions = new ConcurrentLinkedQueue<>();
        transitions.add(DBCLUSTER_ACTIVE);

        test_handleRequest_base(
                new CallbackContext(),
                () -> {
                    if (transitions.size() > 0) {
                        return transitions.remove();
                    }
                    throw DbClusterNotFoundException.builder().message(MSG_NOT_FOUND).build();
                },
                () -> RESOURCE_MODEL,
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).deleteDBCluster(any(DeleteDbClusterRequest.class));
    }

    @Test
    public void handleRequest_isDeletionProtectionEnabled() {
        expectServiceInvocation = false;
        test_handleRequest_base(
                new CallbackContext(),
                () -> DBCLUSTER_ACTIVE_DELETION_ENABLED,
                () -> RESOURCE_MODEL,
                expectFailed(HandlerErrorCode.NotUpdatable)
        );
    }

    @Test
    public void handleRequest_isDeletionProtectionEnabled_failure() {
        expectServiceInvocation = false;
        test_handleRequest_base(
                new CallbackContext(),
                () -> {
                    throw DbClusterNotFoundException.builder().message(MSG_NOT_FOUND).build();
                },
                () -> RESOURCE_MODEL,
                expectFailed(HandlerErrorCode.NotFound)
        );
    }

    @Test
    public void handleRequest_finalSnapshotIdentifierIsSet() {
        when(rdsProxy.client().deleteDBCluster(any(DeleteDbClusterRequest.class)))
                .thenReturn(DeleteDbClusterResponse.builder().build());

        final Queue<DBCluster> transitions = new ConcurrentLinkedQueue<>();
        transitions.add(DBCLUSTER_ACTIVE);

        final String snapshotIdentifier = "test-snapshot-identifier";

        test_handleRequest_base(
                new CallbackContext(),
                ResourceHandlerRequest.<ResourceModel>builder()
                        .snapshotRequested(true),
                () -> {
                    if (transitions.size() > 0) {
                        return transitions.remove();
                    }
                    throw DbClusterNotFoundException.builder().message(MSG_NOT_FOUND).build();
                },
                () -> RESOURCE_MODEL,
                () -> RESOURCE_MODEL.toBuilder()
                        .snapshotIdentifier(snapshotIdentifier)
                        .build(),
                expectSuccess()
        );

        ArgumentCaptor<DeleteDbClusterRequest> argument = ArgumentCaptor.forClass(DeleteDbClusterRequest.class);
        verify(rdsProxy.client(), times(1)).deleteDBCluster(argument.capture());
        Assertions.assertEquals(argument.getValue().finalDBSnapshotIdentifier(), snapshotIdentifier);
    }

    @Test
    public void handleRequest_NoSnapshotRequested() {
        when(rdsProxy.client().deleteDBCluster(any(DeleteDbClusterRequest.class)))
                .thenReturn(DeleteDbClusterResponse.builder().build());

        final Queue<DBCluster> transitions = new ConcurrentLinkedQueue<>();
        transitions.add(DBCLUSTER_ACTIVE);

        test_handleRequest_base(
                new CallbackContext(),
                ResourceHandlerRequest.<ResourceModel>builder()
                        .snapshotRequested(false),
                () -> {
                    if (transitions.size() > 0) {
                        return transitions.remove();
                    }
                    throw DbClusterNotFoundException.builder().message(MSG_NOT_FOUND).build();
                },
                () -> RESOURCE_MODEL,
                () -> RESOURCE_MODEL,
                expectSuccess()
        );

        ArgumentCaptor<DeleteDbClusterRequest> argument = ArgumentCaptor.forClass(DeleteDbClusterRequest.class);
        verify(rdsProxy.client(), times(1)).deleteDBCluster(argument.capture());
        Assertions.assertNull(argument.getValue().finalDBSnapshotIdentifier());
    }

    @Test
    public void handleRequest_globalClusterIdentifier_beforeDelete() {
        when(rdsProxy.client().removeFromGlobalCluster(any(RemoveFromGlobalClusterRequest.class)))
                .thenReturn(RemoveFromGlobalClusterResponse.builder().globalCluster(GLOBAL_CLUSTER).build());
        when(rdsProxy.client().deleteDBCluster(any(DeleteDbClusterRequest.class)))
                .thenReturn(DeleteDbClusterResponse.builder().build());
        Queue<DBCluster> transitions = new ConcurrentLinkedQueue<>();
        transitions.add(DBCLUSTER_INPROGRESS);
        transitions.add(DBCLUSTER_INPROGRESS);
        transitions.add(DBCLUSTER_ACTIVE);

        test_handleRequest_base(
                new CallbackContext(),
                ResourceHandlerRequest.<ResourceModel>builder().snapshotRequested(true),
                () -> {
                    if (transitions.size() > 0) {
                        return transitions.remove();
                    }
                    throw DbClusterNotFoundException.builder().message(MSG_NOT_FOUND).build();
                },
                () -> RESOURCE_MODEL_WITH_GLOBAL_CLUSTER,
                () -> RESOURCE_MODEL_WITH_GLOBAL_CLUSTER,
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).removeFromGlobalCluster(any(RemoveFromGlobalClusterRequest.class));
        verify(rdsProxy.client(), times(1)).deleteDBCluster(any(DeleteDbClusterRequest.class));
    }
}
