package software.amazon.rds.eventsubscription;


import java.util.List;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.EventSubscription;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.Tagging;

public class ReadHandler extends BaseHandlerStd {
    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final Logger logger
    ) {
        return proxy.initiate("rds::read-event-subscription", proxyClient, request.getDesiredResourceState(), callbackContext)
                .translateToServiceRequest(Translator::describeEventSubscriptionsRequest)
                .makeServiceCall((describeEventSubscriptionsRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(describeEventSubscriptionsRequest, proxyInvocation.client()::describeEventSubscriptions))
                .handleError((describeRequest, exception, client, resourceModel, ctx) -> Commons.handleException(
                        ProgressEvent.progress(resourceModel, ctx),
                        exception,
                        DEFAULT_EVENT_SUBSCRIPTION_ERROR_RULE_SET, requestLogger))
                .done((describeEventSubscriptionsRequest, describeEventSubscriptionsResponse, proxyInvocation, model, context) -> {
                    final EventSubscription eventSubscription = describeEventSubscriptionsResponse.eventSubscriptionsList().stream().findFirst().get();
                    context.setEventSubscriptionArn(eventSubscription.eventSubscriptionArn());
                    return ProgressEvent.progress(Translator.translateToModel(model.getSubscriptionName(), eventSubscription), context);
                })
                .then(progress -> readTags(proxyClient, progress));
    }

    protected ProgressEvent<ResourceModel, CallbackContext> readTags(
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress) {
        ResourceModel model = progress.getResourceModel();
        CallbackContext context = progress.getCallbackContext();
        try {
            String arn = progress.getCallbackContext().getEventSubscriptionArn();
            List<software.amazon.rds.eventsubscription.Tag> resourceTags = Translator.translateTags(Tagging.listTagsForResource(proxyClient, arn));
            model.setTags(resourceTags);
        } catch (Exception exception) {
            return Commons.handleException(
                    ProgressEvent.progress(model, context),
                    exception,
                    DEFAULT_EVENT_SUBSCRIPTION_ERROR_RULE_SET.extendWith(Tagging.IGNORE_LIST_TAGS_PERMISSION_DENIED_ERROR_RULE_SET),
                    requestLogger
            );
        }
        return ProgressEvent.success(model, context);
    }

}
