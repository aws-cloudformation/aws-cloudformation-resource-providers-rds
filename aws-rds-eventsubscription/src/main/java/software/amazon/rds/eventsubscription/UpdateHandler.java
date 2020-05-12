package software.amazon.rds.eventsubscription;

import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class UpdateHandler extends BaseHandlerStd {
    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
      final AmazonWebServicesClientProxy proxy,
      final ResourceHandlerRequest<ResourceModel> request,
      final CallbackContext callbackContext,
      final ProxyClient<RdsClient> proxyClient,
      final Logger logger) {
      final ResourceModel model = request.getDesiredResourceState();
      final ResourceModel previousModel = request.getPreviousResourceState();
      final Set<String> currentSourceIds = Optional.ofNullable(model.getSourceIds()).orElse(Collections.emptySet());
      final Set<String> previousSourceIds = Optional.ofNullable(previousModel.getSourceIds()).orElse(Collections.emptySet());

      return proxy.initiate("rds::update-event-subscription", proxyClient, model, callbackContext)
          .translateToServiceRequest(Translator::modifyEventSubscriptionRequest)
          .makeServiceCall((modifyEventSubscriptionRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(modifyEventSubscriptionRequest, proxyInvocation.client()::modifyEventSubscription))
          .progress()
          .then(progress -> {
            Sets.difference(currentSourceIds, previousSourceIds).forEach(
                sourceId -> proxy.initiate("rds::add-source-id-event-subscription", proxyClient, model, callbackContext)
                    .translateToServiceRequest((resourceModel) -> Translator.addSourceIdentifierToSubscriptionRequest(resourceModel, sourceId))
                    .makeServiceCall((addSourceIdentifierToSubscriptionRequest, proxyCall) -> proxyCall.injectCredentialsAndInvokeV2(addSourceIdentifierToSubscriptionRequest, proxyCall.client()::addSourceIdentifierToSubscription))
            );
            return progress;
          })
          .then(progress -> {
            Sets.difference(previousSourceIds, currentSourceIds).forEach(
                sourceId -> proxy.initiate("rds::remove-source-id-event-subscription", proxyClient, model, callbackContext)
                    .translateToServiceRequest((resourceModel) -> Translator.removeSourceIdentifierFromSubscriptionRequest(resourceModel, sourceId))
                    .makeServiceCall((removeSourceIdentifierFromSubscriptionRequest, proxyCall) -> proxyCall.injectCredentialsAndInvokeV2(removeSourceIdentifierFromSubscriptionRequest, proxyCall.client()::removeSourceIdentifierFromSubscription))
            );
            return progress;
          })
          .then(progress -> waitForEventSubscription(proxy, proxyClient, progress))
          .then(progress -> tagResource(proxy, proxyClient, progress, request.getDesiredResourceTags()))
          .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }
}
