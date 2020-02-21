package software.amazon.rds.dbcluster;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.rds.dbcluster.DBClusterStatus.Status;

public class DeleteHandler extends BaseHandlerStd {

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(final AmazonWebServicesClientProxy proxy,
                                                                          final ResourceHandlerRequest<ResourceModel> request,
                                                                          final CallbackContext callbackContext,
                                                                          final ProxyClient<RdsClient> proxyClient,
                                                                          final Logger logger) {
        return proxy.initiate("rds::delete-dbcluster", proxyClient, request.getDesiredResourceState(), callbackContext)
                .request(Translator::deleteDbClusterRequest)
                .retry(EXPONENTIAL)
                .call((req, cli) -> cli.injectCredentialsAndInvokeV2(req, cli.client()::deleteDBCluster))
                .stabilize((deleteDbClusterRequestCallback, deleteDbClusterResponseCallback, proxyInvocationCallback, modelCallback, callbackContextCallback) ->
                        isDBClusterStabilized(proxyInvocationCallback, modelCallback, Status.Deleted))
                .success();
    }
}
