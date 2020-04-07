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
        return proxy.initiate("rds::update-event-subscription", proxyClient, request.getDesiredResourceState(), callbackContext)
            .request(Translator::modifyEventSubscriptionRequest)
            .call((modifyEventSubscriptionRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(modifyEventSubscriptionRequest, proxyInvocation.client()::modifyEventSubscription))
            .done((modifyEventSubscriptionRequest, modifyEventSubscriptionResponse, proxyInvocation, model, context) -> {
              final ResourceModel previousModel = request.getPreviousResourceState();

              final Set<String> currentSourceIds = Optional.ofNullable(model.getSourceIds()).orElse(
                  Collections.emptySet());

              final Set<String> previousSourceIds = Optional
                  .ofNullable(previousModel.getSourceIds()).orElse(
                      Collections.emptySet());

              final Set<String> sourceIdsToAdd = Sets
                  .difference(currentSourceIds, previousSourceIds);
              final Set<String> sourceIdsToRemove = Sets
                  .difference(previousSourceIds, currentSourceIds);

              sourceIdsToAdd.forEach(
                  sourceId ->
                      proxy
                          .initiate("rds::add-source-id-event-subscription", proxyInvocation, model,
                              context)
                          .request((resourceModel) -> Translator
                              .addSourceIdentifierToSubscriptionRequest(resourceModel.getId(), sourceId))
                          .call((addSourceIdentifierToSubscriptionRequest, proxyCall) -> {
                            System.out.println(sourceId);
                            return proxyCall.injectCredentialsAndInvokeV2(
                                addSourceIdentifierToSubscriptionRequest,
                                proxyCall.client()::addSourceIdentifierToSubscription);
                          })
              );

              sourceIdsToRemove.forEach(
                  sourceId ->
                      proxy.initiate("rds::remove-source-id-event-subscription", proxyInvocation,
                          model, context)
                          .request((resourceModel) -> Translator
                              .removeSourceIdentifierFromSubscriptionRequest(resourceModel.getId(),
                                  sourceId))
                          .call(
                              (removeSourceIdentifierFromSubscriptionRequest, proxyCall) -> proxyCall
                                  .injectCredentialsAndInvokeV2(
                                      removeSourceIdentifierFromSubscriptionRequest,
                                      proxyCall.client()::removeSourceIdentifierFromSubscription))
              );

              return ProgressEvent.progress(model, context);
            })
            .then(progress -> waitForEventSubscription(proxy, proxyClient, progress))
            .then(progress -> tagResource(proxy, proxyClient, progress, request.getDesiredResourceTags()))
            .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }
}
