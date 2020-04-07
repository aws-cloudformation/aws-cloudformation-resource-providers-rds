package software.amazon.rds.eventsubscription;

import static software.amazon.rds.eventsubscription.Translator.translateTagsFromSdk;

import java.util.HashSet;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.EventSubscription;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class ReadHandler extends BaseHandlerStd {
    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
      final AmazonWebServicesClientProxy proxy,
      final ResourceHandlerRequest<ResourceModel> request,
      final CallbackContext callbackContext,
      final ProxyClient<RdsClient> proxyClient,
      final Logger logger) {
        return proxy.initiate("rds::read-event-subscription", proxyClient, request.getDesiredResourceState(), callbackContext)
            .request(Translator::describeEventSubscriptionsRequest)
            .call((describeEventSubscriptionsRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(describeEventSubscriptionsRequest, proxyInvocation.client()::describeEventSubscriptions))
            .done((describeEventSubscriptionsRequest, describeEventSubscriptionsResponse, proxyInvocation, model, context) -> {
                final EventSubscription eventSubscription = describeEventSubscriptionsResponse.eventSubscriptionsList().stream().findFirst().get();
                final ListTagsForResourceResponse listTagsForResourceResponse = proxyInvocation.injectCredentialsAndInvokeV2(Translator.listTagsForResourceRequest(eventSubscription.eventSubscriptionArn()), proxyInvocation.client()::listTagsForResource);

                return ProgressEvent.success(
                    ResourceModel.builder()
                        .enabled(eventSubscription.enabled())
                        .eventCategories(eventSubscription.eventCategoriesList())
                        .id(model.getId())
                        .snsTopicArn(eventSubscription.snsTopicArn())
                        .sourceType(eventSubscription.sourceType())
                        .sourceIds(new HashSet<>(eventSubscription.sourceIdsList()))
                        .tags(translateTagsFromSdk(listTagsForResourceResponse.tagList()))
                        .build(),
                    context);
            });
    }
}
