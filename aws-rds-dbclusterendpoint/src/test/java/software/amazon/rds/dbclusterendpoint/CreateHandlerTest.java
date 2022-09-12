package software.amazon.rds.dbclusterendpoint;

import com.google.common.collect.Iterables;
import lombok.Getter;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.platform.commons.util.StringUtils;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceRequest;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceResponse;
import software.amazon.awssdk.services.rds.model.CreateDbClusterEndpointRequest;
import software.amazon.awssdk.services.rds.model.CreateDbClusterEndpointResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterEndpointsRequest;
import software.amazon.awssdk.services.rds.model.InvalidDbClusterStateException;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.rds.model.RdsException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.error.ErrorCode;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.handler.Tagging;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class CreateHandlerTest extends HandlerTest {

    @Mock
    @Getter
    private AmazonWebServicesClientProxy proxy;

    @Mock
    @Getter
    private ProxyClient<RdsClient> rdsProxy;

    @Mock
    RdsClient rdsClient;

    @Getter
    private CreateHandler handler;

    @BeforeEach
    public void setup() {
        handler = new CreateHandler(HandlerConfig.builder().backoff(TEST_BACKOFF_DELAY).build());
        rdsClient = mock(RdsClient.class);
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        rdsProxy = mockProxy(proxy, rdsClient);
    }

    @AfterEach
    public void tear_down() {
        verify(rdsClient, atLeastOnce()).serviceName();
        verifyNoMoreInteractions(rdsClient);
    }

    @Test
    public void handleRequest_CreateSuccess() {
        when(rdsProxy.client().createDBClusterEndpoint(any(CreateDbClusterEndpointRequest.class)))
                .thenReturn(CreateDbClusterEndpointResponse.builder().build());
        when(rdsProxy.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder().build());

        test_handleRequest_base(
                new CallbackContext(),
                () -> DB_CLUSTER_ENDPOINT_AVAILABLE,
                () -> RESOURCE_MODEL,
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).createDBClusterEndpoint(any(CreateDbClusterEndpointRequest.class));
    }

    @Test
    public void handleRequest_CreateWithEmptyIdentifier() {
        when(rdsProxy.client().createDBClusterEndpoint(any(CreateDbClusterEndpointRequest.class)))
                .thenReturn(CreateDbClusterEndpointResponse.builder().build());
        when(rdsProxy.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder().build());

        test_handleRequest_base(
                new CallbackContext(),
                () -> DB_CLUSTER_ENDPOINT_AVAILABLE,
                () -> RESOURCE_MODEL_BUILDER().dBClusterEndpointIdentifier("").build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).createDBClusterEndpoint(
                argThat((CreateDbClusterEndpointRequest request) -> StringUtils.isNotBlank(request.dbClusterEndpointIdentifier())));
    }

    @Test
    public void handleRequest_CreateException() {
        when(rdsProxy.client().describeDBClusterEndpoints(any(DescribeDbClusterEndpointsRequest.class)))
                .thenThrow(new RuntimeException("test exception"));

        test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> RESOURCE_MODEL,
                expectFailed(HandlerErrorCode.InternalFailure)
        );

        verify(rdsProxy.client(), times(1)).createDBClusterEndpoint(any(CreateDbClusterEndpointRequest.class));
    }

    @Test
    public void handleRequest_CreateStabilize() {
        when(rdsProxy.client().createDBClusterEndpoint(any(CreateDbClusterEndpointRequest.class)))
                .thenReturn(CreateDbClusterEndpointResponse.builder().build());
        when(rdsProxy.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder().build());

        final AtomicBoolean fetchedOnce = new AtomicBoolean(false);

        test_handleRequest_base(
                new CallbackContext(),
                () -> {
                    if (fetchedOnce.compareAndSet(false, true)) {
                        return DB_CLUSTER_ENDPOINT_CREATING;
                    }
                    return DB_CLUSTER_ENDPOINT_AVAILABLE;
                },
                () -> RESOURCE_MODEL,
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).createDBClusterEndpoint(any(CreateDbClusterEndpointRequest.class));
        verify(rdsProxy.client(), times(3)).describeDBClusterEndpoints(any(DescribeDbClusterEndpointsRequest.class));
    }

    @Test
    public void handleRequest_ClusterIsRebooting() {
        when(rdsProxy.client().createDBClusterEndpoint(any(CreateDbClusterEndpointRequest.class)))
                .thenThrow(InvalidDbClusterStateException.builder().message("Cluster is rebooting").build());

        test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> RESOURCE_MODEL,
                expectFailed(HandlerErrorCode.ResourceConflict)
        );

        verify(rdsProxy.client(), times(1)).createDBClusterEndpoint(any(CreateDbClusterEndpointRequest.class));
    }

    @Test
    public void handleRequest_AccessDeniedTagging() {
        when(rdsProxy.client().createDBClusterEndpoint(any(CreateDbClusterEndpointRequest.class)))
                .thenThrow(
                        RdsException.builder()
                                .awsErrorDetails(AwsErrorDetails.builder()
                                        .errorCode(ErrorCode.AccessDeniedException.toString())
                                        .build()
                                ).build())
                .thenReturn(CreateDbClusterEndpointResponse.builder().build());
        when(rdsProxy.client().addTagsToResource(any(AddTagsToResourceRequest.class)))
                .thenReturn(AddTagsToResourceResponse.builder().build());

        when(rdsProxy.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder().build());

        final Tagging.TagSet extraTags = Tagging.TagSet.builder()
                .stackTags(TAG_SET.getStackTags())
                .resourceTags(TAG_SET.getResourceTags())
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> progress = test_handleRequest_base(
                new CallbackContext(),
                ResourceHandlerRequest.<ResourceModel>builder()
                        .systemTags(Translator.translateTagsToRequest(Translator.translateTagsFromSdk(TAG_SET.getSystemTags())))
                        .desiredResourceTags(Translator.translateTagsToRequest(Translator.translateTagsFromSdk(TAG_SET.getStackTags()))),
                () -> DB_CLUSTER_ENDPOINT_AVAILABLE,
                null,
                () -> RESOURCE_MODEL.toBuilder()
                        .tags(Translator.translateTagsFromSdk(TAG_SET.getResourceTags()))
                        .build(),
                expectSuccess()
        );

        Assertions.assertThat(progress.getCallbackContext().isAddTagsComplete()).isTrue();
        Assertions.assertThat(progress.getCallbackContext().getTaggingContext().isSoftFailTags()).isTrue();

        ArgumentCaptor<CreateDbClusterEndpointRequest> createCaptor = ArgumentCaptor.forClass(CreateDbClusterEndpointRequest.class);
        verify(rdsProxy.client(), times(2)).createDBClusterEndpoint(createCaptor.capture());
        final CreateDbClusterEndpointRequest requestWithAllTags = createCaptor.getAllValues().get(0);
        final CreateDbClusterEndpointRequest requestWithSystemTags = createCaptor.getAllValues().get(1);
        Assertions.assertThat(requestWithAllTags.tags()).containsExactlyInAnyOrder(
                Iterables.toArray(Tagging.translateTagsToSdk(TAG_SET), software.amazon.awssdk.services.rds.model.Tag.class));
        Assertions.assertThat(requestWithSystemTags.tags()).containsExactlyInAnyOrder(
                Iterables.toArray(TAG_SET.getSystemTags(), software.amazon.awssdk.services.rds.model.Tag.class));

        verify(rdsProxy.client(), times(2)).describeDBClusterEndpoints(any(DescribeDbClusterEndpointsRequest.class));

        ArgumentCaptor<AddTagsToResourceRequest> addTagsCaptor = ArgumentCaptor.forClass(AddTagsToResourceRequest.class);
        verify(rdsProxy.client(), times(1)).addTagsToResource(addTagsCaptor.capture());
        Assertions.assertThat(addTagsCaptor.getValue().tags()).containsExactlyInAnyOrder(
                Iterables.toArray(Tagging.translateTagsToSdk(extraTags), software.amazon.awssdk.services.rds.model.Tag.class));
    }


    @Test
    public void handleRequest_HardFailTagging() {
        when(rdsProxy.client().createDBClusterEndpoint(any(CreateDbClusterEndpointRequest.class)))
                .thenThrow(
                        RdsException.builder()
                                .awsErrorDetails(AwsErrorDetails.builder()
                                        .errorCode(ErrorCode.AccessDeniedException.toString())
                                        .build()
                                ).build())
                .thenReturn(CreateDbClusterEndpointResponse.builder().build());

        when(rdsProxy.client().addTagsToResource(any(AddTagsToResourceRequest.class)))
                .thenThrow(
                        RdsException.builder()
                                .awsErrorDetails(AwsErrorDetails.builder()
                                        .errorCode(ErrorCode.AccessDeniedException.toString())
                                        .build()
                                ).build());

        final Tagging.TagSet extraTags = Tagging.TagSet.builder()
                .stackTags(TAG_SET.getStackTags())
                .resourceTags(TAG_SET.getResourceTags())
                .build();

        test_handleRequest_base(
                new CallbackContext(),
                ResourceHandlerRequest.<ResourceModel>builder()
                        .systemTags(Translator.translateTagsToRequest(Translator.translateTagsFromSdk(TAG_SET.getSystemTags())))
                        .desiredResourceTags(Translator.translateTagsToRequest(Translator.translateTagsFromSdk(TAG_SET.getStackTags()))),
                () -> DB_CLUSTER_ENDPOINT_AVAILABLE,
                null,
                () -> RESOURCE_MODEL.toBuilder()
                        .tags(Translator.translateTagsFromSdk(TAG_SET.getResourceTags()))
                        .build(),
                expectFailed(HandlerErrorCode.AccessDenied)
        );

        ArgumentCaptor<CreateDbClusterEndpointRequest> createCaptor = ArgumentCaptor.forClass(CreateDbClusterEndpointRequest.class);
        verify(rdsProxy.client(), times(2)).createDBClusterEndpoint(createCaptor.capture());
        final CreateDbClusterEndpointRequest requestWithAllTags = createCaptor.getAllValues().get(0);
        final CreateDbClusterEndpointRequest requestWithSystemTags = createCaptor.getAllValues().get(1);
        Assertions.assertThat(requestWithAllTags.tags()).containsExactlyInAnyOrder(
                Iterables.toArray(Tagging.translateTagsToSdk(TAG_SET), software.amazon.awssdk.services.rds.model.Tag.class));
        Assertions.assertThat(requestWithSystemTags.tags()).containsExactlyInAnyOrder(
                Iterables.toArray(TAG_SET.getSystemTags(), software.amazon.awssdk.services.rds.model.Tag.class));

        verify(rdsProxy.client(), times(1)).describeDBClusterEndpoints(any(DescribeDbClusterEndpointsRequest.class));

        ArgumentCaptor<AddTagsToResourceRequest> addTagsCaptor = ArgumentCaptor.forClass(AddTagsToResourceRequest.class);
        verify(rdsProxy.client(), times(1)).addTagsToResource(addTagsCaptor.capture());
        Assertions.assertThat(addTagsCaptor.getValue().tags()).containsExactlyInAnyOrder(
                Iterables.toArray(Tagging.translateTagsToSdk(extraTags), software.amazon.awssdk.services.rds.model.Tag.class));
    }
}
