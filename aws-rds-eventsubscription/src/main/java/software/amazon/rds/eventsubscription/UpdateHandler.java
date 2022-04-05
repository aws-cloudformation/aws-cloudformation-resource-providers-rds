package software.amazon.rds.eventsubscription;

import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.Sets;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.Tagging;

public class UpdateHandler extends BaseHandlerStd {
    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final Logger logger
    ) {
        final ResourceModel desiredModel = request.getDesiredResourceState();
        final ResourceModel previousModel = request.getPreviousResourceState();
        final Set<String> desiredSourceIds = Optional.ofNullable(desiredModel.getSourceIds()).orElse(Collections.emptySet());
        final Set<String> previousSourceIds = Optional.ofNullable(previousModel.getSourceIds()).orElse(Collections.emptySet());

        final Tagging.TagSet previousTags = Tagging.TagSet.builder()
                .systemTags(Tagging.translateTagsToSdk(request.getPreviousSystemTags()))
                .stackTags(Tagging.translateTagsToSdk(request.getPreviousResourceTags()))
                .resourceTags(new HashSet<>(Translator.translateTagsToSdk(request.getPreviousResourceState().getTags())))
                .build();

        final Tagging.TagSet desiredTags = Tagging.TagSet.builder()
                .systemTags(Tagging.translateTagsToSdk(request.getSystemTags()))
                .stackTags(Tagging.translateTagsToSdk(request.getDesiredResourceTags()))
                .resourceTags(new HashSet<>(Translator.translateTagsToSdk(request.getDesiredResourceState().getTags())))
                .build();

        return ProgressEvent.progress(desiredModel, callbackContext)
                .then(progress -> updateEventSubscription(proxy, callbackContext, proxyClient, desiredModel))
                .then(progress -> addSourceIds(proxy, proxyClient, desiredSourceIds, previousSourceIds, progress))
                .then(progress -> removeSourceIds(proxy, proxyClient, desiredSourceIds, previousSourceIds, progress))
                .then(progress -> waitForEventSubscription(proxy, proxyClient, progress))
                .then(progress -> updateTags(proxy, proxyClient, progress, previousTags, desiredTags))
                .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }

    private ProgressEvent<ResourceModel, CallbackContext> updateEventSubscription(final AmazonWebServicesClientProxy proxy,
                                                                                  final CallbackContext callbackContext,
                                                                                  final ProxyClient<RdsClient> proxyClient,
                                                                                  final ResourceModel desiredModel
    ) {
        return proxy.initiate("rds::update-event-subscription", proxyClient, desiredModel, callbackContext)
                .translateToServiceRequest(Translator::modifyEventSubscriptionRequest)
                .makeServiceCall((modifyEventSubscriptionRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(modifyEventSubscriptionRequest, proxyInvocation.client()::modifyEventSubscription))
                .stabilize((modifyEventSubscriptionRequest, modifyEventSubscriptionResponse, proxyInvocation, resourceModel, context) ->
                        isStabilized(resourceModel, proxyInvocation))
                .handleError((modifyRequest, exception, client, resourceModel, ctx) -> Commons.handleException(
                        ProgressEvent.progress(resourceModel, ctx),
                        exception,
                        DEFAULT_EVENT_SUBSCRIPTION_ERROR_RULE_SET))
                .progress();
    }

    private ProgressEvent<ResourceModel, CallbackContext> removeSourceIds(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final Set<String> desiredSourceIds,
            final Set<String> previousSourceIds,
            final ProgressEvent<ResourceModel, CallbackContext> progress
    ) {
        final Set<String> sourceIdsToRemove = Sets.difference(previousSourceIds, desiredSourceIds);
        return sourceIdsToRemove.stream()
                .map(sourceId -> proxy
                        .initiate("rds::remove-source-id-event-subscription",
                                proxyClient,
                                progress.getResourceModel(),
                                progress.getCallbackContext())
                        .translateToServiceRequest((resourceModel) -> Translator.removeSourceIdentifierFromSubscriptionRequest(resourceModel, sourceId))
                        .makeServiceCall((removeSourceIdentifierFromSubscriptionRequest, proxyCall) -> proxyCall.injectCredentialsAndInvokeV2(
                                removeSourceIdentifierFromSubscriptionRequest,
                                proxyCall.client()::removeSourceIdentifierFromSubscription))
                        .handleError((removeSourceIdentifierFromSubscriptionRequest, exception, client, resourceModel, ctx) -> Commons.handleException(
                                ProgressEvent.progress(resourceModel, ctx),
                                exception,
                                DEFAULT_EVENT_SUBSCRIPTION_ERROR_RULE_SET))
                        .progress())
                .filter(ProgressEvent::isFailed)
                .findFirst()
                .orElse(progress);
    }

    private ProgressEvent<ResourceModel, CallbackContext> addSourceIds(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final Set<String> desiredSourceIds,
            final Set<String> previousSourceIds,
            final ProgressEvent<ResourceModel, CallbackContext> progress
    ) {
        final Set<String> sourceIdsToAdd = Sets.difference(desiredSourceIds, previousSourceIds);
        return sourceIdsToAdd.stream()
                .map(sourceId -> proxy
                        .initiate("rds::add-source-id-event-subscription",
                                proxyClient,
                                progress.getResourceModel(),
                                progress.getCallbackContext())
                        .translateToServiceRequest((resourceModel) -> Translator.addSourceIdentifierToSubscriptionRequest(resourceModel, sourceId))
                        .makeServiceCall((addSourceIdentifierToSubscriptionRequest, proxyCall) -> proxyCall.injectCredentialsAndInvokeV2(
                                addSourceIdentifierToSubscriptionRequest,
                                proxyCall.client()::addSourceIdentifierToSubscription))
                        .handleError((addSourceIdentifierToSubscriptionRequest, exception, client, resourceModel, ctx) -> Commons.handleException(
                                ProgressEvent.progress(resourceModel, ctx),
                                exception,
                                DEFAULT_EVENT_SUBSCRIPTION_ERROR_RULE_SET))
                        .progress())
                .filter(ProgressEvent::isFailed)
                .findFirst()
                .orElse(progress);
    }
}
