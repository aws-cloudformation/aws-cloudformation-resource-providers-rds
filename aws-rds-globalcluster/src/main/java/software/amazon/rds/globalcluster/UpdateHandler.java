package software.amazon.rds.globalcluster;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.*;


public class UpdateHandler extends BaseHandlerStd {
    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(final AmazonWebServicesClientProxy proxy,
                                                                          final ResourceHandlerRequest<ResourceModel> request,
                                                                          final CallbackContext callbackContext,
                                                                          final ProxyClient<RdsClient> proxyClient,
                                                                          final Logger logger) {

        ResourceModel model = request.getDesiredResourceState();

        return proxy.initiate("rds::update-global-cluster", proxyClient, request.getDesiredResourceState(), callbackContext)
                // request to update global cluster
                .translateToServiceRequest(Translator::removeFromGlobalClusterRequest)
                .makeServiceCall((removeFromGlobalClusterRequest, proxyClient1) -> proxyClient1.injectCredentialsAndInvokeV2(removeFromGlobalClusterRequest, proxyClient1.client()::removeFromGlobalCluster))
                .stabilize(((removeFromGlobalClusterRequest, removeFromGlobalClusterResponse, proxyClient1, resourceModel, callbackContext1) ->
                        isDBClusterStabilized(proxyClient1, model, DBClusterStatus.Available)))
                .success();
    }
}
