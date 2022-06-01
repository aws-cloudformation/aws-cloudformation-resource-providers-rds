package software.amazon.rds.dbclusterendpoint;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.*;
import software.amazon.rds.common.handler.Commons;

import java.util.Map;

public class CreateHandler extends BaseHandlerStd {

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final Logger logger) {

        final ResourceModel model = request.getDesiredResourceState();

        final Map<String, String> allTags = mergeMaps(request.getSystemTags(), request.getDesiredResourceTags());

        return ProgressEvent.progress(model, callbackContext)
                .then(progress -> createDbClusterEndpoint(proxy, proxyClient, progress, allTags))
                .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }


    private ProgressEvent<ResourceModel, CallbackContext> createDbClusterEndpoint(final AmazonWebServicesClientProxy proxy,
                                                                                  final ProxyClient<RdsClient> proxyClient,
                                                                                  final ProgressEvent<ResourceModel, CallbackContext> progress,
                                                                                  final Map<String, String> tags
    ) {
        return proxy.initiate("rds::create-db-cluster-endpoint", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest((resourceModel) -> Translator.createDbClusterEndpointRequest(resourceModel, tags))
                .makeServiceCall((createDbClusterEndpointRequest, proxyInvocation) ->
                        proxyInvocation.injectCredentialsAndInvokeV2(createDbClusterEndpointRequest, proxyInvocation.client()::createDBClusterEndpoint))
                .stabilize((createDbClusterEndpointRequest, createDbClusterEndpointResponse, proxyInvocation, resourceModel, context) ->
                        isStabilized(resourceModel, proxyInvocation))
                .handleError((createRequest, exception, client, resourceModel, ctx) -> Commons.handleException(
                        ProgressEvent.progress(resourceModel, ctx),
                        exception,
                        DEFAULT_DB_CLUSTER_ENDPOINT_ERROR_RULE_SET))
                .progress();
    }
}
