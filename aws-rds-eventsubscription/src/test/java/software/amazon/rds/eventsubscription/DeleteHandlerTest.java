package software.amazon.rds.eventsubscription;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
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

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DeleteEventSubscriptionRequest;
import software.amazon.awssdk.services.rds.model.DeleteEventSubscriptionResponse;
import software.amazon.awssdk.services.rds.model.DescribeEventSubscriptionsRequest;
import software.amazon.awssdk.services.rds.model.SubscriptionNotFoundException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.test.common.core.HandlerName;

@ExtendWith(MockitoExtension.class)
public class DeleteHandlerTest extends AbstractTestBase {

    @Mock
    RdsClient rds;

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private ProxyClient<RdsClient> proxyRdsClient;

    @Override
    public HandlerName getHandlerName() {
        return HandlerName.DELETE;
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
    public void handleRequest_SimpleSuccess() {

        final DeleteHandler handler = new DeleteHandler();

        final DeleteEventSubscriptionResponse deleteEventSubscriptionResponse = DeleteEventSubscriptionResponse.builder().build();
        when(proxyRdsClient.client().deleteEventSubscription(any(
                DeleteEventSubscriptionRequest.class))).thenReturn(deleteEventSubscriptionResponse);


        when(proxyRdsClient.client().describeEventSubscriptions(any(
                DescribeEventSubscriptionsRequest.class)))
                .thenThrow(SubscriptionNotFoundException.class);


        final ResourceModel model = ResourceModel.builder().build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, proxyRdsClient, request, new CallbackContext());

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyRdsClient.client()).deleteEventSubscription(any(DeleteEventSubscriptionRequest.class));
        verify(proxyRdsClient.client()).describeEventSubscriptions(any(DescribeEventSubscriptionsRequest.class));
    }
}
