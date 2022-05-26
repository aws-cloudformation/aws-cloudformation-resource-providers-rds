package software.amazon.rds.optiongroup;

import com.amazonaws.util.CollectionUtils;
import com.amazonaws.util.StringUtils;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.handler.HandlerMethod;
import software.amazon.rds.common.util.IdentifierFactory;

import software.amazon.rds.common.handler.Tagging;

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
        final Tagging.TagSet systemTags = Tagging.TagSet.builder()
                .systemTags(Tagging.translateTagsToSdk(request.getSystemTags()))
                .build();

        final Tagging.TagSet extraTags = Tagging.TagSet.builder()
                .stackTags(Tagging.translateTagsToSdk(request.getDesiredResourceTags()))
                .resourceTags(Translator.translateTagsToSdk(request.getDesiredResourceState().getTags()))
                .build();

        return ProgressEvent.progress(request.getDesiredResourceState(), callbackContext)
                .then(progress -> {
                    final ResourceModel model = request.getDesiredResourceState();
                    if (StringUtils.isNullOrEmpty(model.getOptionGroupName())) {
                        model.setOptionGroupName(groupIdentifierFactory.newIdentifier()
                                .withStackId(request.getStackId())
                                .withResourceId(request.getLogicalResourceIdentifier())
                                .withRequestToken(request.getClientRequestToken())
                                .toString());
                    }
                    return ProgressEvent.progress(model, progress.getCallbackContext());
                })
                .then(progress -> createDbClusterParameterGroup(proxy, proxyClient, progress, systemTags))
                .then(progress -> updateTags(proxy, proxyClient, progress, Tagging.TagSet.emptySet(), extraTags))
                .then(progress -> {
                    if (CollectionUtils.isNullOrEmpty(progress.getResourceModel().getOptionConfigurations())) {
                        return progress;
                    }
                    return updateOptionGroup(proxy, proxyClient, progress);
                })
                .then(progress -> {
                    return progress;
                })
                .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }

    private ProgressEvent<ResourceModel, CallbackContext> createDbClusterParameterGroup(final AmazonWebServicesClientProxy proxy,
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
}
