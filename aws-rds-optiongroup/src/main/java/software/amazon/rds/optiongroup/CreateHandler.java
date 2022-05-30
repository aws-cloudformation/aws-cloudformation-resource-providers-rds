package software.amazon.rds.optiongroup;

import com.amazonaws.util.StringUtils;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.util.IdentifierFactory;

import software.amazon.rds.common.handler.Tagging;

import java.util.Collections;
import java.util.LinkedHashSet;

public class CreateHandler extends BaseHandlerStd {

    private static final IdentifierFactory groupIdentifierFactory = new IdentifierFactory(
            STACK_NAME,
            RESOURCE_IDENTIFIER,
            RESOURCE_ID_MAX_LENGTH
    );

    public CreateHandler() {
        this(HandlerConfig.builder()
                .backoff(BACKOFF_DELAY)
                .build());
    }

    public CreateHandler(final HandlerConfig config) {
        super(config);
    }

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final Logger logger
    ) {
        final ResourceModel desiredModel = request.getDesiredResourceState();

        final Tagging.TagSet allTags = Tagging.TagSet.builder()
                .systemTags(Tagging.translateTagsToSdk(request.getSystemTags()))
                .stackTags(Tagging.translateTagsToSdk(request.getDesiredResourceTags()))
                .resourceTags(new LinkedHashSet<>(Translator.translateTagsToSdk(desiredModel.getTags())))
                .build();

        return ProgressEvent.progress(request.getDesiredResourceState(), callbackContext)
                .then(progress -> setOptionGroupNameIfEmpty(request, progress))
                .then(progress -> safeCreateOptionGroup(proxy, proxyClient, progress, allTags))
                .then(progress -> updateOptionGroupIfRequired(proxy, proxyClient, progress))
                .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }

    private ProgressEvent<ResourceModel, CallbackContext> safeCreateOptionGroup(final AmazonWebServicesClientProxy proxy,
                                                                                final ProxyClient<RdsClient> proxyClient,
                                                                                final ProgressEvent<ResourceModel, CallbackContext> progress,
                                                                                final Tagging.TagSet allTags) {
        ProgressEvent<ResourceModel, CallbackContext> progressEvent = createOptionGroup(proxy, proxyClient, progress, allTags);
        if (HandlerErrorCode.AccessDenied.equals(progressEvent.getErrorCode())) { //Resource is subject to soft fail on stack level tags.
            Tagging.TagSet systemTags = Tagging.TagSet.builder().systemTags(allTags.getSystemTags()).build();
            Tagging.TagSet extraTags = allTags.toBuilder().systemTags(Collections.emptySet()).build();
            return createOptionGroup(proxy, proxyClient, progress, systemTags)
                    .then(prog -> updateTags(proxy, proxyClient, prog, Tagging.TagSet.emptySet(), extraTags));
        }
        return progressEvent;
    }

    private ProgressEvent<ResourceModel, CallbackContext> createOptionGroup(final AmazonWebServicesClientProxy proxy,
                                                                                final ProxyClient<RdsClient> proxyClient,
                                                                                final ProgressEvent<ResourceModel, CallbackContext> progress,
                                                                                final Tagging.TagSet tags) {
        return proxy.initiate("rds::create-option-group", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest(model -> Translator.createOptionGroupRequest(
                        model,
                        tags
                ))
                .backoffDelay(config.getBackoff())
                .makeServiceCall((createRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                        createRequest,
                        proxyInvocation.client()::createOptionGroup
                ))
                .handleError((createRequest, exception, client, resourceModel, ctx) -> Commons.handleException(
                        ProgressEvent.progress(resourceModel, ctx),
                        exception,
                        DEFAULT_OPTION_GROUP_ERROR_RULE_SET
                ))
                .progress();
    }

    private ProgressEvent<ResourceModel, CallbackContext> setOptionGroupNameIfEmpty(final ResourceHandlerRequest<ResourceModel> request,
                                                                                      final ProgressEvent<ResourceModel, CallbackContext> progress) {
        final ResourceModel model = request.getDesiredResourceState();
        if (StringUtils.isNullOrEmpty(model.getOptionGroupName())) {
            model.setOptionGroupName(groupIdentifierFactory.newIdentifier()
                    .withStackId(request.getStackId())
                    .withResourceId(request.getLogicalResourceIdentifier())
                    .withRequestToken(request.getClientRequestToken())
                    .toString());
        }
        return ProgressEvent.progress(model, progress.getCallbackContext());
    }
}
