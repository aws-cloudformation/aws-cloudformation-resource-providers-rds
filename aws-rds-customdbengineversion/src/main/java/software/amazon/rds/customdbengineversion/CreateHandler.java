package software.amazon.rds.customdbengineversion;

import java.util.HashSet;

import com.amazonaws.util.StringUtils;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.CustomEngineVersionStatus;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.handler.Tagging;
import software.amazon.rds.common.util.IdentifierFactory;

public class CreateHandler extends BaseHandlerStd {

    private final static IdentifierFactory dbCustomEngineVersionIdentifierFactory = new IdentifierFactory(
            STACK_NAME,
            RESOURCE_IDENTIFIER,
            RESOURCE_ID_MAX_LENGTH
    );

    public CreateHandler() {
        this(HandlerConfig.builder()
                .backoff(BACKOFF_DELAY)
                .build());
    }

    public CreateHandler(HandlerConfig config) {
        super(config);
    }

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final Logger logger) {

        final ResourceModel model = request.getDesiredResourceState();

        if (StringUtils.isNullOrEmpty(model.getEngineVersion())) {
            model.setEngineVersion(DEFAULT_ENGINE_NAME_PREFIX +
                    dbCustomEngineVersionIdentifierFactory.newIdentifier()
                            .withStackId(request.getStackId())
                            .withResourceId(request.getLogicalResourceIdentifier())
                            .withRequestToken(request.getClientRequestToken())
                            .toString());
        }

        final Tagging.TagSet allTags = Tagging.TagSet.builder()
                .systemTags(Tagging.translateTagsToSdk(request.getSystemTags()))
                .stackTags(Tagging.translateTagsToSdk(request.getDesiredResourceTags()))
                .resourceTags(new HashSet<>(Translator.translateTagsToSdk(request.getDesiredResourceState().getTags())))
                .build();

        return ProgressEvent.progress(model, callbackContext)
                .then(progress -> safeCreateCustomEngineVersion(proxy, proxyClient, progress, allTags))
                .then(progress -> modifyAfterCreate(proxy, proxyClient, progress))
                .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }

    private ProgressEvent<ResourceModel, CallbackContext> modifyAfterCreate(final AmazonWebServicesClientProxy proxy,
                                                                            final ProxyClient<RdsClient> proxyClient,
                                                                            final ProgressEvent<ResourceModel, CallbackContext> progress) {
        if (shouldUpdateCustomEngineVersion(progress)) {
            return Commons.execOnce(
                    progress,
                    () -> updateCustomEngineVersion(proxy, proxyClient, progress),
                    CallbackContext::isModified,
                    CallbackContext::setModified
            );
        }
        return progress;
    }

    private boolean shouldUpdateCustomEngineVersion(final ProgressEvent<ResourceModel, CallbackContext> progress) {
        return !CustomEngineVersionStatus.AVAILABLE.toString().equalsIgnoreCase(progress.getResourceModel().getStatus());
    }

    private ProgressEvent<ResourceModel, CallbackContext> safeCreateCustomEngineVersion(final AmazonWebServicesClientProxy proxy,
                                                                                        final ProxyClient<RdsClient> proxyClient,
                                                                                        final ProgressEvent<ResourceModel, CallbackContext> progress,
                                                                                        final Tagging.TagSet allTags) {
        return Tagging.safeCreate(proxy, proxyClient, this::createCustomEngineVersion, progress, allTags)
                .then(p -> Commons.execOnce(p, () -> {
                    final Tagging.TagSet extraTags = Tagging.TagSet.builder()
                            .stackTags(allTags.getStackTags())
                            .resourceTags(allTags.getResourceTags())
                            .build();
                    return updateTags(proxy, proxyClient, p, Tagging.TagSet.emptySet(), extraTags);
                }, CallbackContext::isAddTagsComplete, CallbackContext::setAddTagsComplete));
    }

    private ProgressEvent<ResourceModel, CallbackContext> createCustomEngineVersion(final AmazonWebServicesClientProxy proxy,
                                                                                    final ProxyClient<RdsClient> proxyClient,
                                                                                    final ProgressEvent<ResourceModel, CallbackContext> progress,
                                                                                    final Tagging.TagSet tags
    ) {
        return proxy.initiate("rds::create-custom-db-engine-version", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest((resourceModel) -> Translator.createCustomDbEngineVersionRequest(resourceModel, tags))
                .backoffDelay(config.getBackoff())
                .makeServiceCall((createCustomDbEngineVersionRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(createCustomDbEngineVersionRequest, proxyInvocation.client()::createCustomDBEngineVersion))
                .stabilize((createCustomDbEngineVersionRequest, createCustomDbEngineVersionResponse, proxyInvocation, resourceModel, context) ->
                        isStabilized(resourceModel, proxyInvocation))
                .handleError((createRequest, exception, client, resourceModel, ctx) -> Commons.handleException(
                        ProgressEvent.progress(resourceModel, ctx),
                        exception,
                        DEFAULT_CUSTOM_DB_ENGINE_VERSION_ERROR_RULE_SET))
                .done((createRequest, createResponse, proxyInvocation, model, context) ->
                {
                    model.setDBEngineVersionArn(createResponse.dbEngineVersionArn());
                    return ProgressEvent.progress(model, context);
                });
    }

}
