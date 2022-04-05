package software.amazon.rds.eventsubscription;

import java.util.Optional;

import com.amazonaws.util.StringUtils;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.resource.IdentifierUtils;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.Tagging;

public class CreateHandler extends BaseHandlerStd {

    private static final int MAX_LENGTH_EVENT_SUBSCRIPTION = 255;

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final Logger logger) {

        final ResourceModel model = request.getDesiredResourceState();
        if (StringUtils.isNullOrEmpty(model.getSubscriptionName())) {
            model.setSubscriptionName(IdentifierUtils.generateResourceIdentifier(
                    Optional.ofNullable(request.getStackId()).orElse("rds"),
                    Optional.ofNullable(request.getLogicalResourceIdentifier()).orElse("eventsubscription"),
                    request.getClientRequestToken(),
                    MAX_LENGTH_EVENT_SUBSCRIPTION
            ).toLowerCase());
        }

        return proxy.initiate("rds::create-event-subscription", proxyClient, model, callbackContext)
                .translateToServiceRequest((resourceModel) -> Translator.createEventSubscriptionRequest(
                        resourceModel,
                        Tagging.mergeTags(request.getSystemTags(), request.getDesiredResourceTags())))
                .makeServiceCall((createEventSubscriptionRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(createEventSubscriptionRequest, proxyInvocation.client()::createEventSubscription))
                .stabilize((createEventSubscriptionRequest, createEventSubscriptionResponse, proxyInvocation, resourceModel, context) ->
                        isStabilized(resourceModel, proxyInvocation))
                .handleError((createRequest, exception, client, resourceModel, ctx) -> Commons.handleException(
                        ProgressEvent.progress(resourceModel, ctx),
                        exception,
                        DEFAULT_EVENT_SUBSCRIPTION_ERROR_RULE_SET))
                .progress()
                .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }
}
