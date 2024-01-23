package software.amazon.rds.eventsubscription;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
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
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.google.common.collect.ImmutableMap;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.CreateEventSubscriptionRequest;
import software.amazon.awssdk.services.rds.model.CreateEventSubscriptionResponse;
import software.amazon.awssdk.services.rds.model.DescribeEventSubscriptionsRequest;
import software.amazon.awssdk.services.rds.model.DescribeEventSubscriptionsResponse;
import software.amazon.awssdk.services.rds.model.EventSubscription;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.rds.model.SourceNotFoundException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.test.common.core.HandlerName;

@ExtendWith(MockitoExtension.class)
public class CreateHandlerTest extends AbstractTestBase {

    @Captor
    ArgumentCaptor<CreateEventSubscriptionRequest> captor;

    @Mock
    RdsClient rds;

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private ProxyClient<RdsClient> proxyRdsClient;

    @Override
    public HandlerName getHandlerName() {
        return HandlerName.CREATE;
    }

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
        verifyAccessPermissions(rds);
    }

    @Test
    public void handleRequest_Success() {
        final CreateHandler handler = new CreateHandler();

        final CreateEventSubscriptionResponse createEventSubscriptionResponse = CreateEventSubscriptionResponse.builder().build();
        when(proxyRdsClient.client().createEventSubscription(any(CreateEventSubscriptionRequest.class))).thenReturn(createEventSubscriptionResponse);

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

        final ResourceModel model = ResourceModel.builder()
                .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .clientRequestToken("sampleToken")
                .logicalResourceIdentifier("sampleResource")
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, proxyRdsClient, request, new CallbackContext());

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyRdsClient.client()).createEventSubscription(any(CreateEventSubscriptionRequest.class));
        verify(proxyRdsClient.client(), times(2)).describeEventSubscriptions(any(DescribeEventSubscriptionsRequest.class));
        verify(proxyRdsClient.client()).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void handleRequest_SuccessV2() {
        final CreateHandler handler = new CreateHandler();

        final CreateEventSubscriptionResponse createEventSubscriptionResponse = CreateEventSubscriptionResponse.builder().build();
        when(proxyRdsClient.client().createEventSubscription(any(CreateEventSubscriptionRequest.class))).thenReturn(createEventSubscriptionResponse);

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

        final ResourceModel model = ResourceModel.builder().subscriptionName("subscriptionName")
                .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .clientRequestToken("sampleToken")
                .desiredResourceTags(ImmutableMap.of("sampleNewKey", "sampleNewValue"))
                .logicalResourceIdentifier("sampleResource")
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, proxyRdsClient, request, new CallbackContext());

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyRdsClient.client()).createEventSubscription(any(CreateEventSubscriptionRequest.class));
        verify(proxyRdsClient.client(), times(2)).describeEventSubscriptions(any(DescribeEventSubscriptionsRequest.class));
        verify(proxyRdsClient.client()).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void handleRequest_FailWithAccessDenied() {
        final String message = "AccessDenied on create request";

        final CreateHandler handler = new CreateHandler();

        when(proxyRdsClient.client().createEventSubscription(any(CreateEventSubscriptionRequest.class)))
                .thenThrow(AwsServiceException.builder()
                        .awsErrorDetails(AwsErrorDetails.builder().errorMessage(message).errorCode("AccessDenied").build())
                        .build());

        final ResourceModel model = ResourceModel.builder().subscriptionName("subscriptionName")
                .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .clientRequestToken("sampleToken")
                .desiredResourceTags(ImmutableMap.of("sampleNewKey", "sampleNewValue"))
                .logicalResourceIdentifier("sampleResource")
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, proxyRdsClient, request, new CallbackContext());

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).contains(message);
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.AccessDenied);
    }

    @Test
    public void handleRequest_FailWithNotFound() {
        final CreateHandler handler = new CreateHandler();

        when(proxyRdsClient.client().createEventSubscription(any(CreateEventSubscriptionRequest.class)))
                .thenThrow(SourceNotFoundException.builder()
                        .message("Could not find source : test")
                        .build());

        final ResourceModel model = ResourceModel.builder().subscriptionName("subscriptionName")
                .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .clientRequestToken("sampleToken")
                .desiredResourceTags(ImmutableMap.of("sampleNewKey", "sampleNewValue"))
                .logicalResourceIdentifier("sampleResource")
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, proxyRdsClient, request, new CallbackContext());

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.NotFound);
    }

    @Test
    public void handleRequest_DefaultEnabledValue() {
        final CreateHandler handler = new CreateHandler();

        final CreateEventSubscriptionResponse createEventSubscriptionResponse = CreateEventSubscriptionResponse.builder().build();
        when(proxyRdsClient.client().createEventSubscription(captor.capture())).thenReturn(createEventSubscriptionResponse);

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

        final ResourceModel model = ResourceModel.builder()
                .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .clientRequestToken("sampleToken")
                .logicalResourceIdentifier("sampleResource")
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, proxyRdsClient, request, new CallbackContext());

        assertThat(captor.getValue().enabled()).isTrue();
        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getResourceModel().getEnabled()).isTrue();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyRdsClient.client()).createEventSubscription(any(CreateEventSubscriptionRequest.class));
        verify(proxyRdsClient.client(), times(2)).describeEventSubscriptions(any(DescribeEventSubscriptionsRequest.class));
        verify(proxyRdsClient.client()).listTagsForResource(any(ListTagsForResourceRequest.class));
    }
}
