package software.amazon.rds.integration;

import org.apache.commons.codec.binary.StringUtils;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.handler.Tagging;

import java.util.HashSet;

public class UpdateHandler extends BaseHandlerStd {
    /** Default constructor w/ default backoff */
    public UpdateHandler() {}

    /** Default constructor w/ custom config */
    public UpdateHandler(HandlerConfig config) {
        super(config);
    }

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final Logger logger
    ) {
        // Currently Integration resource only supports Tags update.
        final ResourceModel desiredModel = request.getDesiredResourceState();
        final ResourceModel previousModel = request.getPreviousResourceState();


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
                .then(progress -> {
                    if (shouldModifyIntegration(previousModel, progress.getResourceModel())) {
                        return modifyIntegration(proxy, proxyClient, progress);
                    }
                    return progress;
                })
                .then(progress -> updateTags(proxy, proxyClient, progress, previousTags, desiredTags))
                .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }

    private boolean shouldModifyIntegration(final ResourceModel previousModel, final ResourceModel desiredModel) {
        return !StringUtils.equals(previousModel.getDataFilter(), desiredModel.getDataFilter()) ||
                !StringUtils.equals(previousModel.getDescription(), desiredModel.getDescription()) ||
                !StringUtils.equals(previousModel.getIntegrationName(), desiredModel.getIntegrationName());
    }
    private ProgressEvent<ResourceModel, CallbackContext> modifyIntegration(final AmazonWebServicesClientProxy proxy,                                                                             final ProxyClient<RdsClient> proxyClient,                                                                             final ProgressEvent<ResourceModel, CallbackContext> progress) {
        return proxy.initiate("rds::modify-integration", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest(Translator::modifyIntegrationRequest)
                .backoffDelay(config.getBackoff())
                .makeServiceCall((modifyIntegrationRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(modifyIntegrationRequest, proxyInvocation.client()::modifyIntegration))
                .stabilize((modifyIntegrationRequest, modifyIntegrationResponse, proxyInvocation, resourceModel, context) -> isStabilized(resourceModel, proxyInvocation))
                .handleError((awsRequest, exception, client, resourceModel, context) -> Commons.handleException(
                        ProgressEvent.progress(resourceModel, context),
                        exception,
                        DEFAULT_INTEGRATION_ERROR_RULE_SET))
                .progress();
    }
}
