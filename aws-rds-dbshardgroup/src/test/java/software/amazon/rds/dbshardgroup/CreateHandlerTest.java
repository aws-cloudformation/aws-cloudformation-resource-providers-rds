package software.amazon.rds.dbshardgroup;

import java.time.Duration;
import java.util.Objects;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import lombok.Getter;
import org.mockito.ArgumentMatchers;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.CreateDbShardGroupRequest;
import software.amazon.awssdk.services.rds.model.CreateDbShardGroupResponse;
import software.amazon.awssdk.services.rds.model.DBCluster;
import software.amazon.awssdk.services.rds.model.DBShardGroup;
import software.amazon.awssdk.services.rds.model.DbShardGroupAlreadyExistsException;
import software.amazon.awssdk.services.rds.model.DbShardGroupNotFoundException;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbShardGroupsRequest;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.ProxyClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static software.amazon.rds.dbshardgroup.CreateHandler.STACK_NAME;
import static software.amazon.rds.dbshardgroup.CreateHandler.dbShardGroupIdentifierFactory;

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

    @BeforeEach
    public void setup() {
        handler = new CreateHandler(TEST_HANDLER_CONFIG);
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        rdsClient = mock(RdsClient.class);
        proxyClient = MOCK_PROXY(proxy, rdsClient);
    }

    @AfterEach
    public void tear_down() {
        verify(rdsClient, atLeastOnce()).serviceName();
        verifyNoMoreInteractions(rdsClient);
    }

    @Test
    public void handleRequest_SimpleSuccess() {
        ResourceHandlerRequest.ResourceHandlerRequestBuilder<ResourceModel> requestBuilder = ResourceHandlerRequest.builder();
        requestBuilder.region(REGION);
        requestBuilder.awsAccountId(ACCOUNT_ID);
        requestBuilder.awsPartition(PARTITION);

        when(proxyClient.client().createDBShardGroup(any(CreateDbShardGroupRequest.class)))
                .thenReturn(CreateDbShardGroupResponse.builder()
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
        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder()
                        .build());

        final CallbackContext context = new CallbackContext();

        test_handleRequest_base(
                context,
                requestBuilder,
                () -> DB_SHARD_GROUP_AVAILABLE,
                null,
                () -> RESOURCE_MODEL,
                expectSuccess()
        );

        verify(proxyClient.client(), times(1)).createDBShardGroup(
                ArgumentMatchers.<CreateDbShardGroupRequest>argThat(req ->
                        Objects.equals(DB_SHARD_GROUP_CREATING.dbShardGroupIdentifier(), req.dbShardGroupIdentifier())
                )
        );
        verify(proxyClient.client(), times(4)).describeDBShardGroups(
                ArgumentMatchers.<DescribeDbShardGroupsRequest>argThat(req ->
                        Objects.equals(DB_SHARD_GROUP_CREATING.dbShardGroupIdentifier(), req.dbShardGroupIdentifier())
                )
        );
        verify(proxyClient.client(), times(1)).describeDBClusters(
                ArgumentMatchers.<DescribeDbClustersRequest>argThat(req ->
                        Objects.equals(DB_SHARD_GROUP_CREATING.dbClusterIdentifier(), req.dbClusterIdentifier())
                )
        );
        verify(proxyClient.client(), times(1)).listTagsForResource(
                ArgumentMatchers.<ListTagsForResourceRequest>argThat(
                        req -> DB_SHARD_GROUP_ARN.equalsIgnoreCase(req.resourceName()))
        );
    }

    @Test
    public void handleRequest_NoDbShardGroupIdentifier() {
        ResourceHandlerRequest.ResourceHandlerRequestBuilder<ResourceModel> requestBuilder = ResourceHandlerRequest.builder();
        requestBuilder.region(REGION);
        requestBuilder.awsAccountId(ACCOUNT_ID);
        requestBuilder.awsPartition(PARTITION);

        String dbShardGroupIdentifier = dbShardGroupIdentifierFactory.newIdentifier()
                .withStackId(STACK_ID)
                .withResourceId(LOGICAL_RESOURCE_IDENTIFIER)
                .withRequestToken(CLIENT_REQUEST_TOKEN)
                .toString();

        when(proxyClient.client().createDBShardGroup(any(CreateDbShardGroupRequest.class)))
                .thenReturn(CreateDbShardGroupResponse.builder()
                        .dbShardGroupIdentifier(dbShardGroupIdentifier)
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
        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder()
                        .build());

        final CallbackContext context = new CallbackContext();

        test_handleRequest_base(
                context,
                requestBuilder,
                () -> DB_SHARD_GROUP_AVAILABLE,
                null,
                () -> RESOURCE_MODEL_NO_IDENT,
                expectSuccess()
        );

        verify(proxyClient.client(), times(1)).createDBShardGroup(
                ArgumentMatchers.<CreateDbShardGroupRequest>argThat(req ->
                        Objects.equals(dbShardGroupIdentifier, req.dbShardGroupIdentifier())
                )
        );
        verify(proxyClient.client(), times(4)).describeDBShardGroups(
                ArgumentMatchers.<DescribeDbShardGroupsRequest>argThat(req ->
                        Objects.equals(dbShardGroupIdentifier, req.dbShardGroupIdentifier())
                )
        );
        verify(proxyClient.client(), times(1)).describeDBClusters(
                ArgumentMatchers.<DescribeDbClustersRequest>argThat(req ->
                        Objects.equals(DB_SHARD_GROUP_CREATING.dbClusterIdentifier(), req.dbClusterIdentifier())
                )
        );
        verify(proxyClient.client(), times(1)).listTagsForResource(
                ArgumentMatchers.<ListTagsForResourceRequest>argThat(
                        req -> DB_SHARD_GROUP_ARN.equalsIgnoreCase(req.resourceName()))
        );
    }

    @Test
    public void handleRequest_Stabilize() {
        ResourceHandlerRequest.ResourceHandlerRequestBuilder<ResourceModel> requestBuilder = ResourceHandlerRequest.builder();
        requestBuilder.region(REGION);
        requestBuilder.awsAccountId(ACCOUNT_ID);
        requestBuilder.awsPartition(PARTITION);

        when(proxyClient.client().createDBShardGroup(any(CreateDbShardGroupRequest.class)))
                .thenReturn(CreateDbShardGroupResponse.builder()
                        .dbShardGroupIdentifier(DB_SHARD_GROUP_AVAILABLE.dbShardGroupIdentifier())
                        .dbClusterIdentifier(DB_SHARD_GROUP_AVAILABLE.dbClusterIdentifier())
                        .computeRedundancy(DB_SHARD_GROUP_AVAILABLE.computeRedundancy())
                        .maxACU(DB_SHARD_GROUP_AVAILABLE.maxACU())
                        .publiclyAccessible(DB_SHARD_GROUP_AVAILABLE.publiclyAccessible())
                        .build());
        when(proxyClient.client().describeDBClusters(any(DescribeDbClustersRequest.class)))
                .thenReturn(DescribeDbClustersResponse.builder().dbClusters(
                                        DBCluster.builder()
                                                .status(String.valueOf(ResourceStatus.MODIFYING))
                                                .build()
                                )
                                .build()
                ).thenReturn(DescribeDbClustersResponse.builder().dbClusters(
                                        DBCluster.builder()
                                                .status(String.valueOf(ResourceStatus.AVAILABLE))
                                                .build()
                                )
                                .build()
                );
        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder()
                        .build());

        final CallbackContext context = new CallbackContext();
        final Queue<DBShardGroup> transitions = new ConcurrentLinkedQueue<>();
        transitions.add(DB_SHARD_GROUP_CREATING);
        transitions.add(DB_SHARD_GROUP_AVAILABLE);
        transitions.add(DB_SHARD_GROUP_AVAILABLE);
        transitions.add(DB_SHARD_GROUP_MODIFYING);

        test_handleRequest_base(
                context,
                requestBuilder,
                () -> {
                    if (transitions.size() > 0) {
                        return transitions.remove();
                    }
                    return DB_SHARD_GROUP_AVAILABLE;
                },
                null,
                () -> RESOURCE_MODEL,
                expectSuccess()
        );

        verify(proxyClient.client(), times(1)).createDBShardGroup(
                ArgumentMatchers.<CreateDbShardGroupRequest>argThat(req ->
                        Objects.equals(DB_SHARD_GROUP_CREATING.dbShardGroupIdentifier(), req.dbShardGroupIdentifier())
                )
        );
        verify(proxyClient.client(), times(6)).describeDBShardGroups(
                ArgumentMatchers.<DescribeDbShardGroupsRequest>argThat(req ->
                        Objects.equals(DB_SHARD_GROUP_CREATING.dbShardGroupIdentifier(), req.dbShardGroupIdentifier())
                )
        );
        verify(proxyClient.client(), times(2)).describeDBClusters(
                ArgumentMatchers.<DescribeDbClustersRequest>argThat(req ->
                        Objects.equals(DB_SHARD_GROUP_CREATING.dbClusterIdentifier(), req.dbClusterIdentifier())
                )
        );
        verify(proxyClient.client(), times(1)).listTagsForResource(
                ArgumentMatchers.<ListTagsForResourceRequest>argThat(
                        req -> DB_SHARD_GROUP_ARN.equalsIgnoreCase(req.resourceName()))
        );
    }

    @Test
    public void handleRequest_CreateDbShardGroupException() {
        ResourceHandlerRequest.ResourceHandlerRequestBuilder<ResourceModel> requestBuilder = ResourceHandlerRequest.builder();
        requestBuilder.region(REGION);
        requestBuilder.awsAccountId(ACCOUNT_ID);
        requestBuilder.awsPartition(PARTITION);

        when(proxyClient.client().createDBShardGroup(any(CreateDbShardGroupRequest.class)))
                .thenThrow(DbShardGroupAlreadyExistsException.builder().message("error").build());

        final CallbackContext context = new CallbackContext();

        test_handleRequest_base(
                context,
                requestBuilder,
                null,
                null,
                () -> RESOURCE_MODEL,
                expectFailed(HandlerErrorCode.AlreadyExists)
        );

        verify(proxyClient.client(), times(1)).createDBShardGroup(
                ArgumentMatchers.<CreateDbShardGroupRequest>argThat(req ->
                        Objects.equals(DB_SHARD_GROUP_CREATING.dbShardGroupIdentifier(), req.dbShardGroupIdentifier())
                )
        );
    }
}
