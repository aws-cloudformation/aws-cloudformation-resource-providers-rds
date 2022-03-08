package software.amazon.rds.dbclusterparametergroup;

import java.util.Map;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Tagging;

public class UpdateHandler extends BaseHandlerStd {

    @Override
    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(final AmazonWebServicesClientProxy proxy,
                                                                          final ResourceHandlerRequest<ResourceModel> request,
                                                                          final CallbackContext callbackContext,
                                                                          final ProxyClient<RdsClient> proxyClient,
                                                                          final Logger logger) {
        final ResourceModel model = request.getDesiredResourceState();
        final Map<String, String> previousTags = Tagging.mergeTags(
                request.getPreviousSystemTags(),
                request.getPreviousResourceTags()
        );
        final Map<String, String> desiredTags = Tagging.mergeTags(
                request.getSystemTags(),
                request.getDesiredResourceTags()
        );
        return ProgressEvent.progress(model, callbackContext)
                .then(progress -> resetAllParameters(progress, proxy, proxyClient))
                .then(progress -> applyParameters(proxy, proxyClient, progress.getResourceModel(), progress.getCallbackContext()))
                .then(progress -> tagResource(proxy, proxyClient, progress, previousTags, desiredTags))
                .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }

    private ProgressEvent<ResourceModel, CallbackContext> tagResource(final AmazonWebServicesClientProxy proxy,
                                                                      final ProxyClient<RdsClient> proxyClient,
                                                                      final ProgressEvent<ResourceModel, CallbackContext> progress,
                                                                      final Map<String, String> previousTags,
                                                                      final Map<String, String> desiredTags) {
        return describeDbClusterParameterGroup(proxy, proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .done((describeDbClusterParameterGroupsRequest, describeDbClusterParameterGroupsResponse, invocation, resourceModel, context) -> {
                    final String arn = describeDbClusterParameterGroupsResponse.dbClusterParameterGroups().stream().findFirst().get().dbClusterParameterGroupArn();
                    return Tagging.updateTags(
                            invocation,
                            ProgressEvent.progress(resourceModel, context),
                            arn,
                            previousTags,
                            desiredTags,
                            DEFAULT_DB_CLUSTER_PARAMETER_GROUP_ERROR_RULE_SET);
                });
    }
}
