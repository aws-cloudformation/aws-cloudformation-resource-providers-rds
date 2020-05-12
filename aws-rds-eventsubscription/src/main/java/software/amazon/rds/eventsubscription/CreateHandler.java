package software.amazon.rds.eventsubscription;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.resource.IdentifierUtils;

public class CreateHandler extends BaseHandlerStd {
    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final ProxyClient<RdsClient> proxyClient,
        final Logger logger) {
        ResourceModel model = request.getDesiredResourceState();

        model.setSubscriptionName(IdentifierUtils
            .generateResourceIdentifier(request.getLogicalResourceIdentifier(), request.getClientRequestToken(), 255).toLowerCase());

        return proxy.initiate("rds::create-event-subscription", proxyClient, model, callbackContext)
            .translateToServiceRequest((resourceModel) -> Translator.createEventSubscriptionRequest(model, request.getDesiredResourceTags()))
            .makeServiceCall((createEventSubscriptionRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(createEventSubscriptionRequest, proxyInvocation.client()::createEventSubscription))
            .stabilize((createEventSubscriptionRequest, createEventSubscriptionResponse, proxyInvocation, resourceModel, context) ->
                isStabilized(resourceModel, proxyInvocation))
            .progress()
            .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }
}
