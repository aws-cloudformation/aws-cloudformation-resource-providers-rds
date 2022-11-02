package software.amazon.rds.customdbengineversion;

import java.util.HashSet;

import org.apache.commons.lang3.StringUtils;

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
        this(CUSTOM_ENGINE_VERSION_HANDLER_CONFIG_10H);
    }

    public UpdateHandler(final HandlerConfig config) {
        super(config);
    }

    @Override
    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final Logger logger) {

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

        if (desiredModel.getStatus() == null) {
            desiredModel.setStatus(CustomDBEngineVersionStatus.Available.toString());
        }

        return ProgressEvent.progress(desiredModel, callbackContext)
                .then(progress -> {
                    if (shouldModifyEngineVersion(previousModel, progress.getResourceModel())) {
                        return modifyCustomEngineVersion(proxy, proxyClient, previousModel, progress);
                    }
                    return progress;
                })
                .then(progress -> updateTags(proxy, proxyClient, progress, previousTags, desiredTags))
                .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }


    private boolean shouldModifyEngineVersion(final ResourceModel previousModel, final ResourceModel desiredModel) {
        return !StringUtils.equals(previousModel.getStatus(), desiredModel.getStatus()) ||
                !StringUtils.equals(previousModel.getDescription(), desiredModel.getDescription());
    }
}
