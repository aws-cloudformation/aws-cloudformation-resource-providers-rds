package software.amazon.rds.dbshardgroup;

import java.time.Duration;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import lombok.Getter;
import org.mockito.ArgumentMatchers;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.*;
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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.times;

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

    @Getter
    private UpdateHandler handler;

    @BeforeEach
    public void setup() {
        handler = new UpdateHandler(TEST_HANDLER_CONFIG);
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
        requestBuilder.previousResourceTags(Translator.translateTagsToRequest(TAG_LIST));
        requestBuilder.desiredResourceTags(Translator.translateTagsToRequest(TAG_LIST_ALTER));

        when(proxyClient.client().removeTagsFromResource(any(RemoveTagsFromResourceRequest.class)))
                .thenReturn(RemoveTagsFromResourceResponse.builder().build());
        when(proxyClient.client().addTagsToResource(any(AddTagsToResourceRequest.class)))
                .thenReturn(AddTagsToResourceResponse.builder().build());
        when(proxyClient.client().modifyDBShardGroup(any(ModifyDbShardGroupRequest.class)))
                .thenReturn(ModifyDbShardGroupResponse.builder().build());
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

        Queue<DBShardGroup> transitions = new ConcurrentLinkedQueue<>();
        transitions.add(DB_SHARD_GROUP_MODIFYING);

        ProgressEvent<ResourceModel, CallbackContext> progressEvent = ProgressEvent.<ResourceModel, CallbackContext>builder()
                .callbackContext(new CallbackContext()).build();

        progressEvent.getCallbackContext().setWaitTime(301);

        test_handleRequest_base(
                progressEvent.getCallbackContext(),
                requestBuilder,
                () -> {
                    if (transitions.size() > 0) {
                        return transitions.remove();
                    }
                    return DB_SHARD_GROUP_AVAILABLE.toBuilder()
                            .maxACU(MAX_ACU_ALTER)
                            .build();
                },
                () -> RESOURCE_MODEL.toBuilder()
                        .build(),
                () -> RESOURCE_MODEL.toBuilder()
                        .maxACU(MAX_ACU_ALTER)
                        .build(),
                expectSuccess()
        );

        verify(proxyClient.client(), times(1)).modifyDBShardGroup(any(ModifyDbShardGroupRequest.class));
        verify(proxyClient.client(), times(3)).describeDBShardGroups(any(DescribeDbShardGroupsRequest.class));
        verify(proxyClient.client(), times(1)).removeTagsFromResource(any(RemoveTagsFromResourceRequest.class));
        verify(proxyClient.client(), times(1)).addTagsToResource(any(AddTagsToResourceRequest.class));
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
        requestBuilder.previousResourceTags(Translator.translateTagsToRequest(TAG_LIST));
        requestBuilder.desiredResourceTags(Translator.translateTagsToRequest(TAG_LIST_ALTER));

        when(proxyClient.client().removeTagsFromResource(any(RemoveTagsFromResourceRequest.class)))
                .thenReturn(RemoveTagsFromResourceResponse.builder().build());
        when(proxyClient.client().addTagsToResource(any(AddTagsToResourceRequest.class)))
                .thenReturn(AddTagsToResourceResponse.builder().build());
        when(proxyClient.client().modifyDBShardGroup(any(ModifyDbShardGroupRequest.class)))
                .thenReturn(ModifyDbShardGroupResponse.builder().build());
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

        Queue<DBShardGroup> transitions = new ConcurrentLinkedQueue<>();
        transitions.add(DB_SHARD_GROUP_MODIFYING);
        transitions.add(DB_SHARD_GROUP_MODIFYING);
        transitions.add(DB_SHARD_GROUP_MODIFYING);

        ProgressEvent<ResourceModel, CallbackContext> progressEvent = ProgressEvent.<ResourceModel, CallbackContext>builder()
                .callbackContext(new CallbackContext()).build();

        progressEvent.getCallbackContext().setWaitTime(301);

        test_handleRequest_base(
                progressEvent.getCallbackContext(),
                requestBuilder,
                () -> {
                    if (transitions.size() > 0) {
                        return transitions.remove();
                    }
                    return DB_SHARD_GROUP_AVAILABLE.toBuilder()
                            .maxACU(MAX_ACU_ALTER)
                            .build();
                },
                () -> RESOURCE_MODEL.toBuilder()
                        .build(),
                () -> RESOURCE_MODEL.toBuilder()
                        .maxACU(MAX_ACU_ALTER)
                        .build(),
                expectSuccess()
        );

        verify(proxyClient.client(), times(1)).modifyDBShardGroup(any(ModifyDbShardGroupRequest.class));
        verify(proxyClient.client(), times(5)).describeDBShardGroups(any(DescribeDbShardGroupsRequest.class));
        verify(proxyClient.client(), times(1)).removeTagsFromResource(any(RemoveTagsFromResourceRequest.class));
        verify(proxyClient.client(), times(1)).addTagsToResource(any(AddTagsToResourceRequest.class));
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
    public void handleRequest_Exception() {
        ResourceHandlerRequest.ResourceHandlerRequestBuilder<ResourceModel> requestBuilder = ResourceHandlerRequest.builder();
        requestBuilder.region(REGION);
        requestBuilder.awsAccountId(ACCOUNT_ID);
        requestBuilder.awsPartition(PARTITION);
        requestBuilder.previousResourceTags(Translator.translateTagsToRequest(TAG_LIST));
        requestBuilder.desiredResourceTags(Translator.translateTagsToRequest(TAG_LIST_ALTER));

        when(proxyClient.client().modifyDBShardGroup(any(ModifyDbShardGroupRequest.class)))
                .thenThrow(DbShardGroupNotFoundException.builder().message("error").build());

        ProgressEvent<ResourceModel, CallbackContext> progressEvent = ProgressEvent.<ResourceModel, CallbackContext>builder()
                .callbackContext(new CallbackContext()).build();

        test_handleRequest_base(
                progressEvent.getCallbackContext(),
                requestBuilder,
                null,
                () -> RESOURCE_MODEL.toBuilder()
                        .build(),
                () -> RESOURCE_MODEL.toBuilder()
                        .maxACU(MAX_ACU_ALTER)
                        .build(),
                expectFailed(HandlerErrorCode.NotFound)
        );

        verify(proxyClient.client(), times(1)).modifyDBShardGroup(any(ModifyDbShardGroupRequest.class));
    }
}
