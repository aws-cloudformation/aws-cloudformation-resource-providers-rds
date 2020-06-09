package software.amazon.rds.globalcluster;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DescribeGlobalClustersRequest;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.ProxyClient;

public class UpdateHandler extends BaseHandlerStd {
    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(final AmazonWebServicesClientProxy proxy,
                                                                          final ResourceHandlerRequest<ResourceModel> request,
                                                                          final CallbackContext callbackContext,
                                                                          final ProxyClient<RdsClient> proxyClient,
                                                                          final Logger logger) {

        ResourceModel model = request.getDesiredResourceState();

        return proxy.initiate("rds::update-global-cluster", proxyClient, request.getDesiredResourceState(), callbackContext)
                // request to update global cluster
                .translateToServiceRequest(Translator::modifyGlobalClusterRequest)
                .backoffDelay(BACKOFF_STRATEGY)
                .makeServiceCall((modifyGlobalClusterRequest, proxyClient1) -> proxyClient1.injectCredentialsAndInvokeV2(modifyGlobalClusterRequest, proxyClient1.client()::modifyGlobalCluster))
                .stabilize(((modifyGlobalClusterRequest, modifyGlobalClusterResponse, proxyClient1, resourceModel, callbackContext1) ->
                        isGlobalClusterStabilized(proxyClient1, model)))
                .progress()
                .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }
}
