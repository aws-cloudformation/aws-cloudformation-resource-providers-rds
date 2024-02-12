package software.amazon.rds.dbparametergroup;

import java.util.HashSet;
import java.util.Map;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
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

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext
    ) {
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

        final Map<String, Object> previousParams = request.getPreviousResourceState().getParameters();
        final Map<String, Object> desiredParams = request.getDesiredResourceState().getParameters();

        return ProgressEvent.progress(request.getDesiredResourceState(), callbackContext)
                .then(progress -> updateTags(proxy, proxyClient, progress, previousTags, desiredTags))
                .then(progress -> applyParametersWithReset(proxy, proxyClient, progress, previousParams, desiredParams))
                .then(progress -> new ReadHandler().handleRequest(proxy, proxyClient, request, callbackContext, requestLogger));
    }
}
