package software.amazon.rds.dbparametergroup;

import java.util.HashSet;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.handler.Tagging;
import software.amazon.rds.common.logging.RequestLogger;

public class UpdateHandler extends BaseHandlerStd {

    public UpdateHandler() {
        this(HandlerConfig.builder().build());
    }

    public UpdateHandler(final HandlerConfig config) {
        super(config);
    }

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final RequestLogger requestLogger
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

        return ProgressEvent.progress(request.getDesiredResourceState(), callbackContext)
                .then(progress -> Commons.execOnceAndSaveContext(progress,
                        () -> verifyDbParameterGroupExists(proxy, proxyClient, progress),
                        CallbackContext::isVerifiedParameterGroupExists,
                        CallbackContext::setVerifiedParameterGroupExists))
                .then(progress -> updateTags(proxy, proxyClient, progress, previousTags, desiredTags, requestLogger))
                .then(progress -> applyParametersWithReset(proxy, proxyClient, progress, requestLogger))
                .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, requestLogger));
    }
}
