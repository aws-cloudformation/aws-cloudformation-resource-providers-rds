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
import software.amazon.awssdk.services.rds.model.AddTagsToResourceRequest;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceResponse;
import software.amazon.awssdk.services.rds.model.DBCluster;
import software.amazon.awssdk.services.rds.model.DbClusterNotFoundException;
import software.amazon.awssdk.services.rds.model.DbClusterRoleNotFoundException;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersRequest;
import software.amazon.awssdk.services.rds.model.ModifyDbClusterRequest;
import software.amazon.awssdk.services.rds.model.RemoveFromGlobalClusterRequest;
import software.amazon.awssdk.services.rds.model.RemoveFromGlobalClusterResponse;
import software.amazon.awssdk.services.rds.model.RemoveRoleFromDbClusterRequest;
import software.amazon.awssdk.services.rds.model.RemoveRoleFromDbClusterResponse;
import software.amazon.awssdk.services.rds.model.RemoveTagsFromResourceRequest;
import software.amazon.awssdk.services.rds.model.RemoveTagsFromResourceResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.delay.Constant;
import software.amazon.rds.common.handler.HandlerConfig;

@ExtendWith(MockitoExtension.class)
public class UpdateHandlerTest extends AbstractHandlerTest {

    @Mock
    @Getter
    private AmazonWebServicesClientProxy proxy;

    @Mock
    @Getter
    private ProxyClient<RdsClient> rdsProxy;

    @Mock
    RdsClient rdsClient;

    @Getter
    private UpdateHandler handler;

    private boolean expectServiceInvocation;

    @BeforeEach
    public void setup() {
        handler = new UpdateHandler(
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
    public void handleRequest_SimpleSuccessRoleNotFound() {
        when(rdsProxy.client().addRoleToDBCluster(any(AddRoleToDbClusterRequest.class)))
                .thenReturn(AddRoleToDbClusterResponse.builder().build());
        when(rdsProxy.client().removeRoleFromDBCluster(any(RemoveRoleFromDbClusterRequest.class)))
                .thenThrow(DbClusterRoleNotFoundException.builder().message("not found").build());

        final CallbackContext context = new CallbackContext();
        context.setModified(true);

        test_handleRequest_base(
                context,
                () -> DBCLUSTER_ACTIVE,
                () -> RESOURCE_MODEL,
                () -> RESOURCE_MODEL,
                expectSuccess()
        );

        verify(rdsProxy.client(), times(2)).describeDBClusters(any(DescribeDbClustersRequest.class));
    }

    @Test
    public void handleRequest_RemoveFromGlobalCluster() {
        when(rdsProxy.client().removeFromGlobalCluster(any(RemoveFromGlobalClusterRequest.class)))
                .thenReturn(RemoveFromGlobalClusterResponse.builder().build());
        when(rdsProxy.client().addRoleToDBCluster(any(AddRoleToDbClusterRequest.class)))
                .thenReturn(AddRoleToDbClusterResponse.builder().build());
        when(rdsProxy.client().removeRoleFromDBCluster(any(RemoveRoleFromDbClusterRequest.class)))
                .thenThrow(DbClusterRoleNotFoundException.builder().message("not found").build());

        final CallbackContext context = new CallbackContext();
        context.setModified(true);

        test_handleRequest_base(
                context,
                () -> DBCLUSTER_ACTIVE,
                () -> RESOURCE_MODEL.toBuilder().globalClusterIdentifier("global-cluster-identifier").build(),
                () -> RESOURCE_MODEL.toBuilder().globalClusterIdentifier("").build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(4)).describeDBClusters(any(DescribeDbClustersRequest.class));
        verify(rdsProxy.client(), times(1)).removeFromGlobalCluster(any(RemoveFromGlobalClusterRequest.class));
    }

    @Test
    public void handleRequest_AddToGlobalClusterIsImmutable() {
        expectServiceInvocation = false;
        test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> RESOURCE_MODEL.toBuilder().globalClusterIdentifier("").build(),
                () -> RESOURCE_MODEL.toBuilder().globalClusterIdentifier("global-cluster-identifier").build(),
                expectFailed(HandlerErrorCode.NotUpdatable)
        );
    }

    @Test
    public void handleRequest_SimpleSuccess() {
        when(rdsProxy.client().removeRoleFromDBCluster(any(RemoveRoleFromDbClusterRequest.class)))
                .thenReturn(RemoveRoleFromDbClusterResponse.builder().build());
        when(rdsProxy.client().addRoleToDBCluster(any(AddRoleToDbClusterRequest.class)))
                .thenReturn(AddRoleToDbClusterResponse.builder().build());
        when(rdsProxy.client().removeTagsFromResource(any(RemoveTagsFromResourceRequest.class)))
                .thenReturn(RemoveTagsFromResourceResponse.builder().build());
        when(rdsProxy.client().addTagsToResource(any(AddTagsToResourceRequest.class)))
                .thenReturn(AddTagsToResourceResponse.builder().build());
        Queue<DBCluster> transitions = new ConcurrentLinkedQueue<>();
        transitions.add(DBCLUSTER_ACTIVE);
        transitions.add(DBCLUSTER_INPROGRESS);
        transitions.add(DBCLUSTER_ACTIVE_NO_ROLE);

        final CallbackContext context = new CallbackContext();
        context.setModified(true);

        test_handleRequest_base(
                context,
                ResourceHandlerRequest.<ResourceModel>builder()
                        .previousResourceTags(Translator.translateTagsToRequest(TAG_LIST))
                        .desiredResourceTags(Translator.translateTagsToRequest(TAG_LIST_ALTER)),
                () -> {
                    if (transitions.size() > 0) {
                        return transitions.remove();
                    }
                    return DBCLUSTER_ACTIVE;
                },
                () -> RESOURCE_MODEL,
                () -> RESOURCE_MODEL,
                expectSuccess()
        );

        verify(rdsProxy.client(), times(6)).describeDBClusters(any(DescribeDbClustersRequest.class));
        verify(rdsProxy.client(), times(1)).removeRoleFromDBCluster(any(RemoveRoleFromDbClusterRequest.class));
        verify(rdsProxy.client(), times(1)).addRoleToDBCluster(any(AddRoleToDbClusterRequest.class));
        verify(rdsProxy.client(), times(1)).removeTagsFromResource(any(RemoveTagsFromResourceRequest.class));
        verify(rdsProxy.client(), times(1)).addTagsToResource(any(AddTagsToResourceRequest.class));
    }

    @Test
    public void handleRequest_DbClusterNotFound() {
        when(rdsProxy.client().modifyDBCluster(any(ModifyDbClusterRequest.class)))
                .thenThrow(DbClusterNotFoundException.builder().message("not found").build());

        test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> RESOURCE_MODEL,
                () -> RESOURCE_MODEL,
                expectFailed(HandlerErrorCode.NotFound)
        );

        verify(rdsProxy.client(), times(1)).modifyDBCluster(any(ModifyDbClusterRequest.class));
    }
}
