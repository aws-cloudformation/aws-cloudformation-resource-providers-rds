package software.amazon.rds.eventsubscription;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.SubscriptionNotFoundException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class DeleteHandler extends BaseHandlerStd {
    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
      final AmazonWebServicesClientProxy proxy,
      final ResourceHandlerRequest<ResourceModel> request,
      final CallbackContext callbackContext,
      final ProxyClient<RdsClient> proxyClient,
      final Logger logger) {
        return proxy.initiate("rds::delete-event-subscription", proxyClient, request.getDesiredResourceState(), callbackContext)
            .request(Translator::deleteEventSubscriptionRequest)
            .call((deleteEventSubscriptionRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(deleteEventSubscriptionRequest, proxyInvocation.client()::deleteEventSubscription))
            .stabilize((deleteEventSubscriptionRequest, deleteEventSubscriptionResponse, proxyInvocation, model, context) ->
                isDeleted(model, proxyInvocation))
            .success();
    }
    protected boolean isDeleted(final ResourceModel model,
        final ProxyClient<RdsClient> proxyClient) {
        try {
            proxyClient.injectCredentialsAndInvokeV2(
                Translator.describeEventSubscriptionsRequest(model),
                proxyClient.client()::describeEventSubscriptions);
            return false;
        } catch (SubscriptionNotFoundException e) {
            return true;
        }
    }
}
