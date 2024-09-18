package software.amazon.rds.dbshardgroup;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import lombok.Getter;
import org.mockito.ArgumentMatchers;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.*;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.ProxyClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.times;

@ExtendWith(MockitoExtension.class)
public class DeleteHandlerTest extends AbstractHandlerTest {

    @Mock
    @Getter
    private AmazonWebServicesClientProxy proxy;

    @Mock
    @Getter
    private ProxyClient<RdsClient> proxyClient;

    @Mock
    RdsClient rdsClient;

    @Getter
    private DeleteHandler handler;
    private boolean expectClientInvocation;

    @BeforeEach
    public void setup() {
        handler = new DeleteHandler(TEST_HANDLER_CONFIG);
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        rdsClient = mock(RdsClient.class);
        proxyClient = MOCK_PROXY(proxy, rdsClient);
        expectClientInvocation = true;
    }

    @AfterEach
    public void tear_down() {
        if (expectClientInvocation){
            verify(rdsClient, atLeastOnce()).serviceName();
        }
        verifyNoMoreInteractions(rdsClient);
    }

    @Test
    public void handleRequest_SimpleSuccess() {
        when(proxyClient.client().deleteDBShardGroup(any(DeleteDbShardGroupRequest.class)))
                .thenReturn(DeleteDbShardGroupResponse.builder()
                        .dbShardGroupIdentifier(DB_SHARD_GROUP_AVAILABLE.dbShardGroupIdentifier())
                        .dbClusterIdentifier(DB_SHARD_GROUP_AVAILABLE.dbClusterIdentifier())
                        .computeRedundancy(DB_SHARD_GROUP_AVAILABLE.computeRedundancy())
                        .maxACU(DB_SHARD_GROUP_AVAILABLE.maxACU())
                        .publiclyAccessible(DB_SHARD_GROUP_AVAILABLE.publiclyAccessible())
                        .build());
        when(proxyClient.client().describeDBClusters(any(DescribeDbClustersRequest.class)))
                .thenReturn(DescribeDbClustersResponse.builder().dbClusters(
                                        DBCluster.builder()
                                                .status(String.valueOf(ResourceStatus.AVAILABLE))
                                                .build()
                                )
                                .build()
                );

        final CallbackContext context = new CallbackContext();

        final Queue<DBShardGroup> transitions = new ConcurrentLinkedQueue<>();
        transitions.add(DB_SHARD_GROUP_AVAILABLE);
        transitions.add(DB_SHARD_GROUP_AVAILABLE);

        test_handleRequest_base(
                context,
                () -> {
                    if (transitions.size() > 0) {
                        return transitions.remove();
                    }
                    throw DbShardGroupNotFoundException.builder().message("db shard group not found").build();
                },
                () -> RESOURCE_MODEL,
                expectSuccess()
        );

        verify(proxyClient.client(), times(1)).deleteDBShardGroup(
                ArgumentMatchers.any(DeleteDbShardGroupRequest.class)
        );
        verify(proxyClient.client(), times(3)).describeDBShardGroups(
                ArgumentMatchers.<DescribeDbShardGroupsRequest>argThat(req ->
                        Objects.equals(DB_SHARD_GROUP_AVAILABLE.dbShardGroupIdentifier(), req.dbShardGroupIdentifier())
                )
        );
        verify(proxyClient.client(), times(1)).describeDBClusters(
                ArgumentMatchers.<DescribeDbClustersRequest>argThat(req ->
                        Objects.equals(DB_SHARD_GROUP_AVAILABLE.dbClusterIdentifier(), req.dbClusterIdentifier())
                )
        );
    }

    @Test
    public void handleRequest_ShardGroupDeleting() {
        when(proxyClient.client().describeDBClusters(any(DescribeDbClustersRequest.class)))
                .thenThrow(DbClusterNotFoundException.builder().message("db cluster not found").build());

        final CallbackContext context = new CallbackContext();

        final Queue<DBShardGroup> transitions = new ConcurrentLinkedQueue<>();
        transitions.add(DB_SHARD_GROUP_DELETING);
        transitions.add(DB_SHARD_GROUP_DELETING);

        test_handleRequest_base(
                context,
                () -> {
                    if (transitions.size() > 0) {
                        return transitions.remove();
                    }
                    throw DbShardGroupNotFoundException.builder().message("db shard group not found").build();
                },
                () -> RESOURCE_MODEL,
                expectSuccess()
        );

        verify(proxyClient.client(), times(3)).describeDBShardGroups(
                ArgumentMatchers.<DescribeDbShardGroupsRequest>argThat(req ->
                        Objects.equals(DB_SHARD_GROUP_DELETING.dbShardGroupIdentifier(), req.dbShardGroupIdentifier())
                )
        );
        verify(proxyClient.client(), times(1)).describeDBClusters(
                ArgumentMatchers.<DescribeDbClustersRequest>argThat(req ->
                        Objects.equals(DB_SHARD_GROUP_DELETING.dbClusterIdentifier(), req.dbClusterIdentifier())
                )
        );
    }

    @Test
    public void handleRequest_ShardGroupDeleting_ClusterEmptyResponse() {
        when(proxyClient.client().describeDBClusters(any(DescribeDbClustersRequest.class)))
                .thenReturn(DescribeDbClustersResponse.builder().dbClusters(Collections.emptyList()).build());

        final CallbackContext context = new CallbackContext();

        final Queue<DBShardGroup> transitions = new ConcurrentLinkedQueue<>();
        transitions.add(DB_SHARD_GROUP_DELETING);
        transitions.add(DB_SHARD_GROUP_DELETING);

        test_handleRequest_base(
                context,
                () -> {
                    if (transitions.size() > 0) {
                        return transitions.remove();
                    }
                    throw DbShardGroupNotFoundException.builder().message("db shard group not found").build();
                },
                () -> RESOURCE_MODEL,
                expectSuccess()
        );

        verify(proxyClient.client(), times(3)).describeDBShardGroups(
                ArgumentMatchers.<DescribeDbShardGroupsRequest>argThat(req ->
                        Objects.equals(DB_SHARD_GROUP_DELETING.dbShardGroupIdentifier(), req.dbShardGroupIdentifier())
                )
        );
        verify(proxyClient.client(), times(1)).describeDBClusters(
                ArgumentMatchers.<DescribeDbClustersRequest>argThat(req ->
                        Objects.equals(DB_SHARD_GROUP_DELETING.dbClusterIdentifier(), req.dbClusterIdentifier())
                )
        );
    }

    @Test
    public void handleRequest_ShardGroupDeleted() {
        expectClientInvocation = false;
        final CallbackContext context = new CallbackContext();

        test_handleRequest_base(
                context,
                () -> {
                    throw DbShardGroupNotFoundException.builder().message("db shard group not found").build();
                },
                () -> RESOURCE_MODEL,
                expectFailed(HandlerErrorCode.NotFound)
        );

        verify(proxyClient.client(), times(1)).describeDBShardGroups(
                ArgumentMatchers.<DescribeDbShardGroupsRequest>argThat(req ->
                        Objects.equals(DB_SHARD_GROUP_DELETING.dbShardGroupIdentifier(), req.dbShardGroupIdentifier())
                )
        );
    }
}
