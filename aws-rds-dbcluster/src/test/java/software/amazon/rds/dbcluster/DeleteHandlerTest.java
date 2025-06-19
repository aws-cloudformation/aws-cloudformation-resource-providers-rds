package software.amazon.rds.dbcluster;

import static org.assertj.core.api.Assertions.assertThat;
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
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import lombok.Getter;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBCluster;
import software.amazon.awssdk.services.rds.model.DbClusterNotFoundException;
import software.amazon.awssdk.services.rds.model.DeleteDbClusterRequest;
import software.amazon.awssdk.services.rds.model.DeleteDbClusterResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersRequest;
import software.amazon.awssdk.services.rds.model.DescribeGlobalClustersRequest;
import software.amazon.awssdk.services.rds.model.GlobalClusterNotFoundException;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersResponse;
import software.amazon.awssdk.services.rds.model.InvalidDbClusterSnapshotStateException;
import software.amazon.awssdk.services.rds.model.RemoveFromGlobalClusterRequest;
import software.amazon.awssdk.services.rds.model.RemoveFromGlobalClusterResponse;
import software.amazon.awssdk.services.rds.model.SnapshotQuotaExceededException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.delay.Constant;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.test.common.core.HandlerName;

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
    private ProxyClient<Ec2Client> ec2Proxy;

    @Mock
    @Getter
    RdsClient rdsClient;

    @Getter
    private DeleteHandler handler;

    @Override
    public HandlerName getHandlerName() {
        return HandlerName.DELETE;
    }

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
        verifyAccessPermissions(rdsClient);
    }

    @Test
    public void handleRequest_Success() {
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
    public void handleRequest_DeleteAutomatedBackups() {
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
            () -> RESOURCE_MODEL.toBuilder().deleteAutomatedBackups(true).build(),
            expectSuccess()
        );

        final ArgumentCaptor<DeleteDbClusterRequest> argument = ArgumentCaptor.forClass(DeleteDbClusterRequest.class);
        verify(rdsProxy.client(), times(1)).deleteDBCluster(argument.capture());
        assertThat(argument.getValue().deleteAutomatedBackups()).isTrue();
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
        when(rdsProxy.client().describeGlobalClusters(any(DescribeGlobalClustersRequest.class)))
                .thenThrow(GlobalClusterNotFoundException.builder().build());
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


    @Test
    public void handleRequest_RequestFinalSnapshotIfNotExplicitlyRequested() {
        final DeleteDbClusterResponse deleteDbClusterResponse = DeleteDbClusterResponse.builder().build();
        when(rdsProxy.client().deleteDBCluster(any(DeleteDbClusterRequest.class))).thenReturn(deleteDbClusterResponse);

        final CallbackContext callbackContext = new CallbackContext();
        callbackContext.setDeleting(true);

        test_handleRequest_base(
                callbackContext,
                ResourceHandlerRequest.<ResourceModel>builder().snapshotRequested(null),
                () -> {
                    throw DbClusterNotFoundException.builder().message(MSG_NOT_FOUND).build();
                },
                null,
                () -> RESOURCE_MODEL,
                expectSuccess()
        );

        final ArgumentCaptor<DeleteDbClusterRequest> argument = ArgumentCaptor.forClass(DeleteDbClusterRequest.class);
        verify(rdsProxy.client(), times(1)).deleteDBCluster(argument.capture());
        verify(rdsProxy.client(), times(1)).describeDBClusters(any(DescribeDbClustersRequest.class));

        Assertions.assertFalse(argument.getValue().skipFinalSnapshot());
        Assertions.assertNotNull(argument.getValue().finalDBSnapshotIdentifier());
    }

    @Test
    public void handleRequest_InvalidDbClusterSnapshotStateException() {
        when(rdsProxy.client().deleteDBCluster(any(DeleteDbClusterRequest.class)))
                .thenThrow(InvalidDbClusterSnapshotStateException.builder().message("invalid db cluster snapshot state").build());

        test_handleRequest_base(
                new CallbackContext(),
                () -> DBCLUSTER_ACTIVE,
                () -> RESOURCE_MODEL.toBuilder().globalClusterIdentifier(null).build(),
                expectFailed(HandlerErrorCode.ResourceConflict)
        );

        verify(rdsProxy.client(), times(1)).deleteDBCluster(any(DeleteDbClusterRequest.class));
    }

    static class DeleteDBClusterExceptionArgumentsProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext) throws Exception {
            return Stream.of(
                    // Put error codes below
                    // Put exception classes below
                    Arguments.of(SnapshotQuotaExceededException.builder().message(ERROR_MSG).build(), HandlerErrorCode.ServiceLimitExceeded),
                    Arguments.of(InvalidDbClusterSnapshotStateException.builder().message(ERROR_MSG).build(), HandlerErrorCode.ResourceConflict)
            );
        }
    }

    @ParameterizedTest
    @ArgumentsSource(DeleteDBClusterExceptionArgumentsProvider.class)
    public void handleRequest_ModifyDBCluster_HandleException(
            final Object requestException,
            final HandlerErrorCode expectResponseCode
    ) {
        expectDescribeDBClustersCall().setup().thenReturn(DescribeDbClustersResponse.builder()
                .dbClusters(DBCLUSTER_ACTIVE)
                .build());

        test_handleRequest_error(
                expectDeleteDBClusterCall(),
                new CallbackContext(),
                () -> RESOURCE_MODEL,
                requestException,
                expectResponseCode
        );

        expectDescribeDBClustersCall().verify();
    }
}
