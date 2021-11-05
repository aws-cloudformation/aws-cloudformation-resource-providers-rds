package software.amazon.rds.eventsubscription;

import static org.assertj.core.api.Assertions.assertThat;
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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.AddSourceIdentifierToSubscriptionRequest;
import software.amazon.awssdk.services.rds.model.AddSourceIdentifierToSubscriptionResponse;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceRequest;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceResponse;
import software.amazon.awssdk.services.rds.model.DescribeEventSubscriptionsRequest;
import software.amazon.awssdk.services.rds.model.DescribeEventSubscriptionsResponse;
import software.amazon.awssdk.services.rds.model.EventSubscription;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.rds.model.ModifyEventSubscriptionRequest;
import software.amazon.awssdk.services.rds.model.ModifyEventSubscriptionResponse;
import software.amazon.awssdk.services.rds.model.RemoveSourceIdentifierFromSubscriptionRequest;
import software.amazon.awssdk.services.rds.model.RemoveSourceIdentifierFromSubscriptionResponse;
import software.amazon.awssdk.services.rds.model.RemoveTagsFromResourceRequest;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

@ExtendWith(MockitoExtension.class)
public class UpdateHandlerTest extends AbstractTestBase {

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private ProxyClient<RdsClient> proxyRdsClient;

    @Mock
    RdsClient rds;

    @BeforeEach
    public void setup() {
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        rds = mock(RdsClient.class);
        proxyRdsClient = MOCK_PROXY(proxy, rds);
    }

    @AfterEach
    public void post_execute() {
        verify(rds, atLeastOnce()).serviceName();
        verifyNoMoreInteractions(rds);
    }

    @Test
    public void handleRequest_SimpleSuccess() {

        final UpdateHandler handler = new UpdateHandler();

        final ModifyEventSubscriptionResponse modifyEventSubscriptionResponse = ModifyEventSubscriptionResponse.builder().build();
        when(proxyRdsClient.client().modifyEventSubscription(any(
                ModifyEventSubscriptionRequest.class))).thenReturn(modifyEventSubscriptionResponse);

        final DescribeEventSubscriptionsResponse describeEventSubscriptionsResponse = DescribeEventSubscriptionsResponse.builder()
                .eventSubscriptionsList(EventSubscription.builder()
                        .enabled(true)
                        .eventCategoriesList("sampleCategory")
                        .snsTopicArn("sampleSnsArn")
                        .sourceType("sampleSourceType")
                        .sourceIdsList("sampleSourceId")
                        .status("active").build())
                .build();
        when(proxyRdsClient.client().describeEventSubscriptions(any(
                DescribeEventSubscriptionsRequest.class))).thenReturn(describeEventSubscriptionsResponse);

        final ListTagsForResourceResponse listTagsForResourceResponse = ListTagsForResourceResponse.builder().build();
        when(proxyRdsClient.client().listTagsForResource(any(
                ListTagsForResourceRequest.class))).thenReturn(listTagsForResourceResponse);

        final ResourceModel model = ResourceModel.builder().build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .previousResourceState(ResourceModel.builder().build())
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyRdsClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyRdsClient.client()).modifyEventSubscription(any(ModifyEventSubscriptionRequest.class));
        verify(proxyRdsClient.client(), times(4)).describeEventSubscriptions(any(DescribeEventSubscriptionsRequest.class));
        verify(proxyRdsClient.client(), times(1)).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void handleRequest_SimpleSuccessV2() {

        final UpdateHandler handler = new UpdateHandler();

        final ModifyEventSubscriptionResponse modifyEventSubscriptionResponse = ModifyEventSubscriptionResponse.builder().build();
        when(proxyRdsClient.client().modifyEventSubscription(any(
                ModifyEventSubscriptionRequest.class))).thenReturn(modifyEventSubscriptionResponse);

        final DescribeEventSubscriptionsResponse describeEventSubscriptionsResponse = DescribeEventSubscriptionsResponse.builder()
                .eventSubscriptionsList(EventSubscription.builder()
                        .enabled(true)
                        .eventCategoriesList("sampleCategory")
                        .snsTopicArn("sampleSnsArn")
                        .sourceType("sampleSourceType")
                        .sourceIdsList("sampleSourceId")
                        .status("active").build())
                .build();
        when(proxyRdsClient.client().describeEventSubscriptions(any(
                DescribeEventSubscriptionsRequest.class))).thenReturn(describeEventSubscriptionsResponse);

        final ListTagsForResourceResponse listTagsForResourceResponse = ListTagsForResourceResponse.builder().build();
        when(proxyRdsClient.client().listTagsForResource(any(
                ListTagsForResourceRequest.class))).thenReturn(listTagsForResourceResponse);

        final AddSourceIdentifierToSubscriptionResponse addSourceIdentifierToSubscriptionResponse = AddSourceIdentifierToSubscriptionResponse.builder().build();
        when(proxyRdsClient.client().addSourceIdentifierToSubscription(any(AddSourceIdentifierToSubscriptionRequest.class)))
                .thenReturn(addSourceIdentifierToSubscriptionResponse);

        final RemoveSourceIdentifierFromSubscriptionResponse removeSourceIdentifierFromSubscriptionResponse = RemoveSourceIdentifierFromSubscriptionResponse.builder().build();
        when(proxyRdsClient.client().removeSourceIdentifierFromSubscription(any(RemoveSourceIdentifierFromSubscriptionRequest.class)))
                .thenReturn(removeSourceIdentifierFromSubscriptionResponse);

        final AddTagsToResourceResponse addTagsToResourceResponse = AddTagsToResourceResponse.builder().build();
        when(proxyRdsClient.client().addTagsToResource(any(
                AddTagsToResourceRequest.class))).thenReturn(addTagsToResourceResponse);

        final ResourceModel model = ResourceModel.builder()
                .subscriptionName("sampleId")
                .sourceIds(Sets.newHashSet("sampleNewId")).build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .desiredResourceTags(ImmutableMap.of("sampleNewKey", "sampleNewValue"))
                .previousResourceTags(ImmutableMap.of("oldKey", "oldValue"))
                .previousResourceState(ResourceModel.builder()
                        .sourceIds(Sets.newHashSet("sampleId"))
                        .build())
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyRdsClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyRdsClient.client()).modifyEventSubscription(any(ModifyEventSubscriptionRequest.class));
        verify(proxyRdsClient.client(), times(4)).describeEventSubscriptions(any(DescribeEventSubscriptionsRequest.class));
        verify(proxyRdsClient.client(), times(1)).listTagsForResource(any(ListTagsForResourceRequest.class));
        verify(proxyRdsClient.client(), times(1)).addSourceIdentifierToSubscription(any(AddSourceIdentifierToSubscriptionRequest.class));
        verify(proxyRdsClient.client(), times(1)).removeSourceIdentifierFromSubscription(any(RemoveSourceIdentifierFromSubscriptionRequest.class));
        verify(proxyRdsClient.client()).removeTagsFromResource(any(RemoveTagsFromResourceRequest.class));
        verify(proxyRdsClient.client()).addTagsToResource(any(AddTagsToResourceRequest.class));
    }
}
