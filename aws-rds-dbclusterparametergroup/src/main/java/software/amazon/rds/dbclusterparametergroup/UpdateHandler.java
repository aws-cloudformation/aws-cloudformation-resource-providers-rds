package software.amazon.rds.dbclusterparametergroup;

import java.util.HashSet;
import java.util.Map;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.handler.Tagging;

public class UpdateHandler extends BaseHandlerStd {

    public UpdateHandler() {
        this(HandlerConfig.builder().build());
    }

    public UpdateHandler(final HandlerConfig config) {
        super(config);
    }

    @Override
    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(final AmazonWebServicesClientProxy proxy,
                                                                          final ResourceHandlerRequest<ResourceModel> request,
                                                                          final CallbackContext callbackContext,
                                                                          final ProxyClient<RdsClient> proxyClient,
                                                                          final Logger logger) {
        final ResourceModel model = request.getDesiredResourceState();

        final Tagging.TagSet previousTags = Tagging.TagSet.builder()
                .systemTags(Tagging.translateTagsToSdk(request.getPreviousSystemTags()))
                .stackTags(Tagging.translateTagsToSdk(request.getPreviousResourceTags()))
                .resourceTags(Translator.translateTagsToSdk(request.getPreviousResourceState().getTags()))
                .build();

        final Tagging.TagSet desiredTags = Tagging.TagSet.builder()
                .systemTags(Tagging.translateTagsToSdk(request.getSystemTags()))
                .stackTags(Tagging.translateTagsToSdk(request.getDesiredResourceTags()))
                .resourceTags(Translator.translateTagsToSdk(request.getDesiredResourceState().getTags()))
                .build();

        return ProgressEvent.progress(model, callbackContext)
                .then(progress -> updateTags(proxy, proxyClient, progress, previousTags, desiredTags))
                .then(progress -> resetAllParameters(progress, proxy, proxyClient))
                .then(progress -> applyParameters(proxy, proxyClient, progress.getResourceModel(), progress.getCallbackContext()))
                .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }
}
