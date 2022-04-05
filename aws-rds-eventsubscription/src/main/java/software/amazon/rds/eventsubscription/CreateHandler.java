package software.amazon.rds.eventsubscription;

import java.util.HashSet;
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

        final Tagging.TagSet systemTags = Tagging.TagSet.builder()
                .systemTags(Tagging.translateTagsToSdk(request.getSystemTags()))
                .build();

        final Tagging.TagSet extraTags = Tagging.TagSet.builder()
                .stackTags(Tagging.translateTagsToSdk(request.getDesiredResourceTags()))
                .resourceTags(new HashSet<>(Translator.translateTagsToSdk(request.getDesiredResourceState().getTags())))
                .build();

        return ProgressEvent.progress(model, callbackContext)
                .then(progress -> setEventSubscriptionNameIfEmpty(request, progress))
                .then(progress -> createEventSubscription(proxy, proxyClient, progress, systemTags))
                .then(progress -> updateTags(proxy, proxyClient, progress, Tagging.TagSet.emptySet(), extraTags))
                .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }

    private ProgressEvent<ResourceModel, CallbackContext> createEventSubscription(final AmazonWebServicesClientProxy proxy,
                                                                                  final ProxyClient<RdsClient> proxyClient,
                                                                                  final ProgressEvent<ResourceModel, CallbackContext> progress,
                                                                                  final Tagging.TagSet systemTags
    ) {
        return proxy.initiate("rds::create-event-subscription", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest((resourceModel) -> Translator.createEventSubscriptionRequest(resourceModel, systemTags))
                .makeServiceCall((createEventSubscriptionRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(createEventSubscriptionRequest, proxyInvocation.client()::createEventSubscription))
                .stabilize((createEventSubscriptionRequest, createEventSubscriptionResponse, proxyInvocation, resourceModel, context) ->
                        isStabilized(resourceModel, proxyInvocation))
                .handleError((createRequest, exception, client, resourceModel, ctx) -> Commons.handleException(
                        ProgressEvent.progress(resourceModel, ctx),
                        exception,
                        DEFAULT_EVENT_SUBSCRIPTION_ERROR_RULE_SET))
                .progress();
    }

    private ProgressEvent<ResourceModel, CallbackContext> setEventSubscriptionNameIfEmpty(final ResourceHandlerRequest<ResourceModel> request,
                                                                                          final ProgressEvent<ResourceModel, CallbackContext> progress
    ) {
        ResourceModel model = progress.getResourceModel();
        if (StringUtils.isNullOrEmpty(model.getSubscriptionName())) {
            model.setSubscriptionName(IdentifierUtils.generateResourceIdentifier(
                    Optional.ofNullable(request.getStackId()).orElse("rds"),
                    Optional.ofNullable(request.getLogicalResourceIdentifier()).orElse("eventsubscription"),
                    request.getClientRequestToken(),
                    MAX_LENGTH_EVENT_SUBSCRIPTION
            ).toLowerCase());
        }
        return progress;
    }
}
