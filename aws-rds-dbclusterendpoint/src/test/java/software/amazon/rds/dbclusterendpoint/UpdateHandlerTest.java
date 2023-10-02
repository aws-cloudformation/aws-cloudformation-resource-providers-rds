package software.amazon.rds.dbclusterendpoint;

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
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceRequest;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceResponse;
import software.amazon.awssdk.services.rds.model.DbClusterEndpointNotFoundException;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterEndpointsRequest;
import software.amazon.awssdk.services.rds.model.InvalidDbClusterStateException;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.rds.model.ModifyDbClusterEndpointRequest;
import software.amazon.awssdk.services.rds.model.ModifyDbClusterEndpointResponse;
import software.amazon.awssdk.services.rds.model.RdsException;
import software.amazon.awssdk.services.rds.model.RemoveTagsFromResourceRequest;
import software.amazon.awssdk.services.rds.model.RemoveTagsFromResourceResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.error.ErrorCode;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.test.common.core.HandlerName;

@ExtendWith(MockitoExtension.class)
public class UpdateHandlerTest extends AbstractHandlerTest {

    @Mock
    @Getter
    private AmazonWebServicesClientProxy proxy;

    @Mock
    @Getter
    private ProxyClient<RdsClient> rdsProxy;

    @Mock
    private RdsClient rdsClient;

    @Getter
    private UpdateHandler handler;

    @Override
    public HandlerName getHandlerName() {
        return HandlerName.UPDATE;
    }

    @BeforeEach
    public void setup() {
        handler = new UpdateHandler(HandlerConfig.builder().backoff(TEST_BACKOFF_DELAY).build());
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        rdsClient = mock(RdsClient.class);

        rdsProxy = mockProxy(proxy, rdsClient);
    }

    @AfterEach
    public void tear_down() {
        verify(rdsProxy.client(), atLeastOnce()).serviceName();
        verifyNoMoreInteractions(rdsClient);
        verifyAccessPermissions(rdsClient);
    }

    @Test
    public void handleRequest_ModifyDbClusterEndpoint_NotFound() {

        when(rdsProxy.client().modifyDBClusterEndpoint(any(ModifyDbClusterEndpointRequest.class)))
                .thenThrow(DbClusterEndpointNotFoundException.builder().message(MSG_NOT_FOUND).build());

        final CallbackContext context = new CallbackContext();

        test_handleRequest_base(
                context,
                null,
                () -> RESOURCE_MODEL_BUILDER().build(),
                () -> RESOURCE_MODEL_BUILDER().build(),
                expectFailed(HandlerErrorCode.NotFound)
        );

        verify(rdsProxy.client(), times(1)).modifyDBClusterEndpoint(any(ModifyDbClusterEndpointRequest.class));

    }

    @Test
    public void handleRequest_ModifyThrowsRuntimeError() {

        when(rdsProxy.client().modifyDBClusterEndpoint(any(ModifyDbClusterEndpointRequest.class)))
                .thenThrow(new RuntimeException("Internal Failure"));

        final CallbackContext context = new CallbackContext();

        test_handleRequest_base(
                context,
                null,
                () -> RESOURCE_MODEL_BUILDER().build(),
                () -> RESOURCE_MODEL_BUILDER().build(),
                expectFailed(HandlerErrorCode.InternalFailure)
        );

        verify(rdsProxy.client(), times(1)).modifyDBClusterEndpoint(any(ModifyDbClusterEndpointRequest.class));

    }

    @Test
    public void handleRequest_SimpleTagUpdate_Success() {
        when(rdsProxy.client().addTagsToResource(any(AddTagsToResourceRequest.class)))
                .thenReturn(AddTagsToResourceResponse.builder().build());
        when(rdsProxy.client().removeTagsFromResource(any(RemoveTagsFromResourceRequest.class)))
                .thenReturn(RemoveTagsFromResourceResponse.builder().build());
        when(rdsProxy.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder().build());
        when(rdsProxy.client().modifyDBClusterEndpoint(any(ModifyDbClusterEndpointRequest.class)))
                .thenReturn(ModifyDbClusterEndpointResponse.builder().build());

        final CallbackContext context = new CallbackContext();

        test_handleRequest_base(
                context,
                () -> DB_CLUSTER_ENDPOINT_AVAILABLE,
                () -> RESOURCE_MODEL_BUILDER().tags(TAG_LIST_ALTER).build(),
                () -> RESOURCE_MODEL_BUILDER().tags(TAG_LIST).build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(2)).describeDBClusterEndpoints(any(DescribeDbClusterEndpointsRequest.class));
        verify(rdsProxy.client(), times(1)).modifyDBClusterEndpoint(any(ModifyDbClusterEndpointRequest.class));
        verify(rdsProxy.client()).addTagsToResource(any(AddTagsToResourceRequest.class));
        verify(rdsProxy.client()).removeTagsFromResource(any(RemoveTagsFromResourceRequest.class));
    }

    @Test
    public void handleRequest_ModifyStabilize() {

        when(rdsProxy.client().addTagsToResource(any(AddTagsToResourceRequest.class)))
                .thenReturn(AddTagsToResourceResponse.builder().build());
        when(rdsProxy.client().removeTagsFromResource(any(RemoveTagsFromResourceRequest.class)))
                .thenReturn(RemoveTagsFromResourceResponse.builder().build());
        when(rdsProxy.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder().build());
        when(rdsProxy.client().modifyDBClusterEndpoint(any(ModifyDbClusterEndpointRequest.class)))
                .thenReturn(ModifyDbClusterEndpointResponse.builder().build());

        final CallbackContext context = new CallbackContext();
        final AtomicBoolean fetchedOnce = new AtomicBoolean(false);

        test_handleRequest_base(
                context,
                () -> {
                    if (fetchedOnce.compareAndSet(false, true)) {
                        return DB_CLUSTER_ENDPOINT_MODIFYING;
                    }
                    return DB_CLUSTER_ENDPOINT_AVAILABLE;
                },
                () -> RESOURCE_MODEL_BUILDER().tags(TAG_LIST_ALTER).build(),
                () -> RESOURCE_MODEL_BUILDER().tags(TAG_LIST).build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(3)).describeDBClusterEndpoints(any(DescribeDbClusterEndpointsRequest.class));
        verify(rdsProxy.client(), times(1)).modifyDBClusterEndpoint(any(ModifyDbClusterEndpointRequest.class));
        verify(rdsProxy.client()).addTagsToResource(any(AddTagsToResourceRequest.class));
        verify(rdsProxy.client()).removeTagsFromResource(any(RemoveTagsFromResourceRequest.class));
    }

    @Test
    public void handleRequest_ClusterIsRebooting() {
        when(rdsProxy.client().modifyDBClusterEndpoint(any(ModifyDbClusterEndpointRequest.class)))
                .thenThrow(InvalidDbClusterStateException.builder().message("Cluster is rebooting").build());

        final CallbackContext context = new CallbackContext();

        test_handleRequest_base(
                context,
null,
                () -> RESOURCE_MODEL_BUILDER().tags(TAG_LIST_ALTER).build(),
                () -> RESOURCE_MODEL_BUILDER().tags(TAG_LIST).build(),
                expectFailed(HandlerErrorCode.ResourceConflict)
        );

        verify(rdsProxy.client(), times(1)).modifyDBClusterEndpoint(any(ModifyDbClusterEndpointRequest.class));
    }

    @Test
    public void handleRequest_HardFailingTaggingOnRemoveTags() {
        when(rdsProxy.client().modifyDBClusterEndpoint(any(ModifyDbClusterEndpointRequest.class)))
                .thenReturn(ModifyDbClusterEndpointResponse.builder().build());
        when(rdsProxy.client().removeTagsFromResource(any(RemoveTagsFromResourceRequest.class)))
                .thenThrow(
                        RdsException.builder().awsErrorDetails(AwsErrorDetails.builder()
                                .errorCode(ErrorCode.AccessDeniedException.toString()).build()).build());

        final CallbackContext context = new CallbackContext();

        test_handleRequest_base(
                context,
                () -> DB_CLUSTER_ENDPOINT_AVAILABLE,
                () -> RESOURCE_MODEL_BUILDER().tags(TAG_LIST).build(),
                () -> RESOURCE_MODEL_BUILDER().build(),
                expectFailed(HandlerErrorCode.AccessDenied)
        );

        verify(rdsProxy.client(), times(1)).modifyDBClusterEndpoint(any(ModifyDbClusterEndpointRequest.class));
        verify(rdsProxy.client(), times(1)).removeTagsFromResource(any(RemoveTagsFromResourceRequest.class));
        verify(rdsProxy.client(), times(1)).describeDBClusterEndpoints(any(DescribeDbClusterEndpointsRequest.class));
    }

    @Test
    public void handleRequest_HardFailingTaggingOnAddTags() {
        when(rdsProxy.client().modifyDBClusterEndpoint(any(ModifyDbClusterEndpointRequest.class)))
                .thenReturn(ModifyDbClusterEndpointResponse.builder().build());
        when(rdsProxy.client().addTagsToResource(any(AddTagsToResourceRequest.class)))
                .thenThrow(
                        RdsException.builder().awsErrorDetails(AwsErrorDetails.builder()
                                .errorCode(ErrorCode.AccessDeniedException.toString()).build()).build());

        final CallbackContext context = new CallbackContext();

        test_handleRequest_base(
                context,
                () -> DB_CLUSTER_ENDPOINT_AVAILABLE,
                () -> RESOURCE_MODEL_BUILDER().build(),
                () -> RESOURCE_MODEL_BUILDER().tags(TAG_LIST).build(),
                expectFailed(HandlerErrorCode.AccessDenied)
        );

        verify(rdsProxy.client(), times(1)).modifyDBClusterEndpoint(any(ModifyDbClusterEndpointRequest.class));
        verify(rdsProxy.client(), times(1)).addTagsToResource(any(AddTagsToResourceRequest.class));
        verify(rdsProxy.client(), times(1)).describeDBClusterEndpoints(any(DescribeDbClusterEndpointsRequest.class));
    }

    @Test
    public void handleRequest_SoftFailingTaggingOnRemoveTags() {
        when(rdsProxy.client().modifyDBClusterEndpoint(any(ModifyDbClusterEndpointRequest.class)))
                .thenReturn(ModifyDbClusterEndpointResponse.builder().build());
        when(rdsProxy.client().removeTagsFromResource(any(RemoveTagsFromResourceRequest.class)))
                .thenThrow(
                        RdsException.builder().awsErrorDetails(AwsErrorDetails.builder()
                                .errorCode(ErrorCode.AccessDeniedException.toString()).build()).build());

        final CallbackContext context = new CallbackContext();

        test_handleRequest_base(
                context,
                ResourceHandlerRequest.<ResourceModel>builder()
                        .previousSystemTags(Translator.translateTagsToRequest(TAG_LIST))
                        .systemTags(Translator.translateTagsToRequest(TAG_LIST_EMPTY)),
                () -> DB_CLUSTER_ENDPOINT_AVAILABLE,
                () -> RESOURCE_MODEL_BUILDER().build(),
                () -> RESOURCE_MODEL_BUILDER().build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).modifyDBClusterEndpoint(any(ModifyDbClusterEndpointRequest.class));
        verify(rdsProxy.client(), times(1)).removeTagsFromResource(any(RemoveTagsFromResourceRequest.class));
        verify(rdsProxy.client(), times(1)).describeDBClusterEndpoints(any(DescribeDbClusterEndpointsRequest.class));
    }

    @Test
    public void handleRequest_SoftFailingTaggingOnAddTags() {
        when(rdsProxy.client().modifyDBClusterEndpoint(any(ModifyDbClusterEndpointRequest.class)))
                .thenReturn(ModifyDbClusterEndpointResponse.builder().build());
        when(rdsProxy.client().addTagsToResource(any(AddTagsToResourceRequest.class)))
                .thenThrow(
                        RdsException.builder().awsErrorDetails(AwsErrorDetails.builder()
                                .errorCode(ErrorCode.AccessDeniedException.toString()).build()).build());

        final CallbackContext context = new CallbackContext();

        test_handleRequest_base(
                context,
                ResourceHandlerRequest.<ResourceModel>builder()
                        .previousSystemTags(Translator.translateTagsToRequest(TAG_LIST_EMPTY))
                        .systemTags(Translator.translateTagsToRequest(TAG_LIST)),
                () -> DB_CLUSTER_ENDPOINT_AVAILABLE,
                () -> RESOURCE_MODEL_BUILDER().build(),
                () -> RESOURCE_MODEL_BUILDER().build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).modifyDBClusterEndpoint(any(ModifyDbClusterEndpointRequest.class));
        verify(rdsProxy.client(), times(1)).addTagsToResource(any(AddTagsToResourceRequest.class));
        verify(rdsProxy.client(), times(1)).describeDBClusterEndpoints(any(DescribeDbClusterEndpointsRequest.class));
    }
}
