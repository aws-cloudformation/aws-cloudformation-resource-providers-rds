package software.amazon.rds.globalcluster;

import software.amazon.awssdk.services.rds.RdsClient;
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

        ResourceModel previousModel = request.getPreviousResourceState();
        ResourceModel desiredModel = request.getDesiredResourceState();

        return proxy.initiate("rds::update-global-cluster", proxyClient, request.getDesiredResourceState(), callbackContext)
                // request to update global cluster
                .translateToServiceRequest(model -> Translator.modifyGlobalClusterRequest(previousModel, desiredModel))
                .backoffDelay(BACKOFF_STRATEGY)
                .makeServiceCall((modifyGlobalClusterRequest, proxyClient1) -> proxyClient1.injectCredentialsAndInvokeV2(modifyGlobalClusterRequest, proxyClient1.client()::modifyGlobalCluster))
                .stabilize(((modifyGlobalClusterRequest, modifyGlobalClusterResponse, proxyClient1, resourceModel, callbackContext1) ->
                        isGlobalClusterStabilized(proxyClient1, desiredModel)))
                .progress()
                .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }
}
