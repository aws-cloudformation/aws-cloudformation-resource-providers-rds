package software.amazon.rds.eventsubscription;

import java.util.Collections;
import java.util.HashSet;

import com.amazonaws.util.StringUtils;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.Tagging;
import software.amazon.rds.common.util.IdentifierFactory;

public class CreateHandler extends BaseHandlerStd {

    private final static IdentifierFactory subscriptionNameFactory = new IdentifierFactory(
            STACK_NAME,
            RESOURCE_IDENTIFIER,
            MAX_LENGTH_EVENT_SUBSCRIPTION
    );

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final Logger logger) {

        final ResourceModel model = request.getDesiredResourceState();

        final Tagging.TagSet allTags = Tagging.TagSet.builder()
                .systemTags(Tagging.translateTagsToSdk(request.getSystemTags()))
                .stackTags(Tagging.translateTagsToSdk(request.getDesiredResourceTags()))
                .resourceTags(new HashSet<>(Translator.translateTagsToSdk(request.getDesiredResourceState().getTags())))
                .build();

        return ProgressEvent.progress(model, callbackContext)
                .then(progress -> setEnabledDefaultValue(progress))
                .then(progress -> setEventSubscriptionNameIfEmpty(request, progress))
                .then(progress -> safeCreateEventSubscription(proxy, proxyClient, progress, allTags))
                .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }

    private ProgressEvent<ResourceModel, CallbackContext> safeCreateEventSubscription(final AmazonWebServicesClientProxy proxy,
                                                                                     final ProxyClient<RdsClient> proxyClient,
                                                                                     final ProgressEvent<ResourceModel, CallbackContext> progress,
                                                                                     final Tagging.TagSet allTags) {
        ProgressEvent<ResourceModel, CallbackContext> progressEvent = createEventSubscription(proxy, proxyClient, progress, allTags);
        if (HandlerErrorCode.AccessDenied.equals(progressEvent.getErrorCode())) { //Resource is subject to soft fail on stack level tags.
            Tagging.TagSet systemTags = Tagging.TagSet.builder().systemTags(allTags.getSystemTags()).build();
            Tagging.TagSet extraTags = allTags.toBuilder().systemTags(Collections.emptySet()).build();
            return createEventSubscription(proxy, proxyClient, progress, systemTags)
                    .then(prog -> updateTags(proxy, proxyClient, prog, Tagging.TagSet.emptySet(), extraTags));
        }
        return progressEvent;
    }

    private ProgressEvent<ResourceModel, CallbackContext> createEventSubscription(final AmazonWebServicesClientProxy proxy,
                                                                                  final ProxyClient<RdsClient> proxyClient,
                                                                                  final ProgressEvent<ResourceModel, CallbackContext> progress,
                                                                                  final Tagging.TagSet tags
    ) {
        return proxy.initiate("rds::create-event-subscription", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest((resourceModel) -> Translator.createEventSubscriptionRequest(resourceModel, tags))
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
            model.setSubscriptionName(subscriptionNameFactory.newIdentifier()
                    .withStackId(request.getStackId())
                    .withResourceId(request.getLogicalResourceIdentifier())
                    .withRequestToken(request.getClientRequestToken())
                    .toString());
        }
        return progress;
    }
}
