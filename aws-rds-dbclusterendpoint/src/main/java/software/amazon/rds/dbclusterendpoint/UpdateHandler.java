package software.amazon.rds.dbclusterendpoint;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;


public class UpdateHandler extends BaseHandlerStd {

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final Logger logger
    ) {
        final ResourceModel desiredModel = request.getDesiredResourceState();

        return ProgressEvent.progress(desiredModel, callbackContext)
                .then(progress -> updateDbClusterEndpoint(proxy, callbackContext, proxyClient, desiredModel, logger))
                .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }

    private ProgressEvent<ResourceModel, CallbackContext> updateDbClusterEndpoint(final AmazonWebServicesClientProxy proxy,
                                                                                  final CallbackContext callbackContext,
                                                                                  final ProxyClient<RdsClient> proxyClient,
                                                                                  final ResourceModel desiredModel,
                                                                                  final Logger logger
    ) {
        return proxy.initiate("rds::modify-db-cluster-endpoint", proxyClient, desiredModel, callbackContext)
                .translateToServiceRequest(Translator::modifyDbClusterEndpoint)
                .makeServiceCall((modifyDbClusterEndpointRequest, proxyInvocation) ->
                        proxyInvocation.injectCredentialsAndInvokeV2(modifyDbClusterEndpointRequest, proxyInvocation.client()::modifyDBClusterEndpoint))
                .stabilize((modifyDbClusterEndpointRequest, modifyDbClusterEndpointResponse, proxyInvocation, resourceModel, context) ->
                        isStabilized(resourceModel, proxyInvocation, logger))
                .handleError((modifyRequest, exception, client, resourceModel, ctx) -> Commons.handleException(
                        ProgressEvent.progress(resourceModel, ctx),
                        exception,
                        DEFAULT_DB_CLUSTER_ENDPOINT_ERROR_RULE_SET))
                .progress();
    }

}
