package software.amazon.rds.dbclusterparametergroup;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
            final ProxyClient<RdsClient> proxyClient,
            final ResourceHandlerRequest<ResourceModel> request,
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

        final Map<String, Object> previousModelParams = request.getPreviousResourceState().getParameters() == null ? Map.of() : request.getPreviousResourceState().getParameters();
        final Map<String, Object> desiredModelParams = request.getDesiredResourceState().getParameters() == null ? Map.of() : request.getDesiredResourceState().getParameters();
        final boolean shouldUpdateParameters = !DifferenceUtils.diff(previousModelParams, desiredModelParams).isEmpty();

        final Map<String, Parameter> currentClusterParameters = Maps.newHashMap();
        final Map<String, Parameter> desiredClusterParameters = Maps.newHashMap();

        // contains cluster parameters from current and desired parameters so we can use to access the
        // metadata from the describeDbClusterParameters response
        final Map<String, Parameter> allClusterParameters = Maps.newHashMap();

        return ProgressEvent.progress(model, callbackContext)
            .then(progress -> {
                if (shouldUpdateParameters) {
                    // we need to query for all parameters in the filter from current resource and desired model
                    // because it's possible for a parameter to be removed when updating to desired model
                    final Set<String> filter = Stream.concat(desiredModelParams.keySet().stream(), previousModelParams.keySet().stream()).collect(Collectors.toSet());
                    return describeDBClusterParameters(proxy, proxyClient, progress, filter.stream().toList(), allClusterParameters);
                }
                return progress;
            }).then(progress -> {
                for (final Map.Entry<String, Parameter> entry : allClusterParameters.entrySet()) {
                    if (previousModelParams.containsKey(entry.getKey())) {
                        currentClusterParameters.put(entry.getKey(), entry.getValue());
                    }
                    if (desiredModelParams.containsKey(entry.getKey())) {
                        desiredClusterParameters.put(entry.getKey(), entry.getValue());
                    }
                }
                return progress;
            })
            .then(progress -> Commons.execOnce(progress, () -> updateTags(proxy, proxyClient, progress, previousTags, desiredTags), CallbackContext::isAddTagsComplete, CallbackContext::setAddTagsComplete))
            .then(progress -> {
                if (shouldUpdateParameters) {
                    return resetParameters(progress, proxy, proxyClient, previousModelParams, currentClusterParameters, desiredClusterParameters);
                }
                return progress;
            }).then(progress -> Commons.execOnce(progress, () -> {
                if (shouldUpdateParameters) {
                    return applyParameters(proxyClient, progress, currentClusterParameters, desiredClusterParameters);
                }
                return progress;
            }, CallbackContext::isParametersApplied, CallbackContext::setParametersApplied))
            .then(progress -> new ReadHandler().handleRequest(proxy, proxyClient, request, callbackContext));
    }
}
