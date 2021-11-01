package software.amazon.rds.eventsubscription;

import static software.amazon.rds.eventsubscription.Translator.translateTagsFromSdk;

import java.util.HashSet;
import java.util.Set;

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
            final Logger logger) {
        return proxy.initiate("rds::read-event-subscription", proxyClient, request.getDesiredResourceState(), callbackContext)
                .translateToServiceRequest(Translator::describeEventSubscriptionsRequest)
                .makeServiceCall((describeEventSubscriptionsRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(describeEventSubscriptionsRequest, proxyInvocation.client()::describeEventSubscriptions))
                .handleError((deleteRequest, exception, client, resourceModel, ctx) -> Commons.handleException(
                        ProgressEvent.progress(resourceModel, ctx),
                        exception,
                        DEFAULT_EVENT_SUBSCRIPTION_ERROR_RULE_SET))
                .done((describeEventSubscriptionsRequest, describeEventSubscriptionsResponse, proxyInvocation, model, context) -> {
                    try {
                        final EventSubscription eventSubscription = describeEventSubscriptionsResponse.eventSubscriptionsList().stream().findFirst().get();
                        final Set<Tag> tags = translateTagsFromSdk(Tagging.listTagsForResource(proxyInvocation, eventSubscription.eventSubscriptionArn()));

                        return ProgressEvent.success(
                                ResourceModel.builder()
                                        .enabled(eventSubscription.enabled())
                                        .eventCategories(eventSubscription.eventCategoriesList())
                                        .subscriptionName(model.getSubscriptionName())
                                        .snsTopicArn(eventSubscription.snsTopicArn())
                                        .sourceType(eventSubscription.sourceType())
                                        .sourceIds(new HashSet<>(eventSubscription.sourceIdsList()))
                                        .tags(tags)
                                        .build(),
                                context);
                    } catch (Exception exception) {
                        return Commons.handleException(
                                ProgressEvent.progress(model, context),
                                exception,
                                DEFAULT_EVENT_SUBSCRIPTION_ERROR_RULE_SET);
                    }
                });
    }
}
