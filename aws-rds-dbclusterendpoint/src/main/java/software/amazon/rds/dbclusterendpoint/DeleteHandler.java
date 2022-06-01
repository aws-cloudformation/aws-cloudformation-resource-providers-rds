package software.amazon.rds.dbclusterendpoint;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterEndpointsResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;

public class DeleteHandler extends BaseHandlerStd {
    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final Logger logger) {
        return proxy.initiate("rds::delete-db-cluster-endpoint", proxyClient, request.getDesiredResourceState(), callbackContext)
                .translateToServiceRequest(Translator::deleteDbClusterEndpointRequest)
                .makeServiceCall((deleteDbClusterEndpointRequest, proxyInvocation) ->
                        proxyInvocation.injectCredentialsAndInvokeV2(deleteDbClusterEndpointRequest, proxyInvocation.client()::deleteDBClusterEndpoint))
                .stabilize((deleteDbClusterEndpointRequest, deleteDbClusterEndpointResponse, proxyInvocation, model, context) ->
                        isDeleted(model, proxyInvocation))
                .handleError((deleteRequest, exception, client, resourceModel, ctx) -> Commons.handleException(
                        ProgressEvent.progress(resourceModel, ctx),
                        exception,
                        DEFAULT_DB_CLUSTER_ENDPOINT_ERROR_RULE_SET))
                .success();
    }

    protected boolean isDeleted(final ResourceModel model,
                                final ProxyClient<RdsClient> proxyClient) {

        final DescribeDbClusterEndpointsResponse resp = proxyClient.injectCredentialsAndInvokeV2(Translator.describeDbClustersEndpointRequest(model),
                proxyClient.client()::describeDBClusterEndpoints);

        if (!resp.hasDbClusterEndpoints()) {
            return true;
        }
        if (!resp.dbClusterEndpoints().stream().findFirst().isPresent()) {
            return true;
        }
        return false;

    }
}
