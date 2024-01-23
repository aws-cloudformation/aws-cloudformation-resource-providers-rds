package software.amazon.rds.dbclusterparametergroup;

import java.util.ArrayList;
import java.util.Map;

import com.google.common.collect.Maps;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.Parameter;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.handler.Tagging;
import software.amazon.rds.common.util.DifferenceUtils;

public class UpdateHandler extends BaseHandlerStd {

    public UpdateHandler() {
        this(DEFAULT_HANDLER_CONFIG);
    }

    public UpdateHandler(final HandlerConfig config) {
        super(config);
    }

    @Override
    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient, final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext
    ) {
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

        final Map<String, Object> previousParams = request.getPreviousResourceState().getParameters();
        final Map<String, Object> desiredParams = request.getDesiredResourceState().getParameters();
        final boolean shouldUpdateParameters = !DifferenceUtils.diff(previousParams, desiredParams).isEmpty();

        final Map<String, Parameter> currentClusterParameters = Maps.newHashMap();

        return ProgressEvent.progress(model, callbackContext)
                .then(progress -> {
                    if (shouldUpdateParameters) {
                        return describeCurrentDBClusterParameters(proxy, proxyClient, progress, new ArrayList<>(desiredParams.keySet()), currentClusterParameters, requestLogger);
                    }
                    return progress;
                }).then(progress -> Commons.execOnce(progress, () -> updateTags(proxy, proxyClient, progress, previousTags, desiredTags), CallbackContext::isAddTagsComplete, CallbackContext::setAddTagsComplete))
                .then(progress -> {
                    if (shouldUpdateParameters) {
                        return resetParameters(progress, proxy, proxyClient, currentClusterParameters, desiredParams);
                    }
                    return progress;
                }).then(progress -> Commons.execOnce(progress, () -> {
                    if (shouldUpdateParameters) {
                        return applyParameters(proxy, proxyClient, progress, currentClusterParameters, requestLogger);
                    }
                    return progress;
                }, CallbackContext::isParametersApplied, CallbackContext::setParametersApplied))
                .then(progress -> new ReadHandler().handleRequest(proxy, proxyClient, request, callbackContext));
    }
}
