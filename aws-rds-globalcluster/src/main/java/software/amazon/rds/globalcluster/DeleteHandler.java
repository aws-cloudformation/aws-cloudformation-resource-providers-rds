package software.amazon.rds.globalcluster;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DeleteGlobalClusterRequest;
import software.amazon.awssdk.services.rds.model.DeleteGlobalClusterResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class DeleteHandler extends BaseHandlerStd {

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(final AmazonWebServicesClientProxy proxy,
                                                                          final ResourceHandlerRequest<ResourceModel> request,
                                                                          final CallbackContext callbackContext,
                                                                          final ProxyClient<RdsClient> proxyClient,
                                                                          final Logger logger) {

        ResourceModel model = request.getDesiredResourceState();

        ProgressEvent<ResourceModel, CallbackContext> result;

        result = ProgressEvent.progress(model, callbackContext)
                .then(progress -> removeFromGlobalCluster(proxy, proxyClient, progress))
                .then(progress -> waitForDBClusterAvailableStatus(proxy, proxyClient, progress))
                .then(progress -> proxy.initiate("rds::delete-global-cluster", proxyClient, request.getDesiredResourceState(), callbackContext)
                        .translateToServiceRequest(Translator::deleteGlobalClusterRequest)
                        .backoffDelay(BACKOFF_STRATEGY)
                        .makeServiceCall((deleteGlobalClusterRequest1, proxyInvocation) -> deleteGlobalCluster(deleteGlobalClusterRequest1, proxyInvocation, callbackContext))
                        // wait until deleted
                        .stabilize((deleteGlobalClusterRequest, deleteGlobalClusterResponse, stabilizeProxy, stabilizeModel, context)
                                -> isDeleted(stabilizeModel, stabilizeProxy))
                        .success());

        if (result.isSuccess()) {
            result.setResourceModel(null);
        }

        return result;
    }

    private static DeleteGlobalClusterResponse deleteGlobalCluster(DeleteGlobalClusterRequest deleteGlobalClusterRequest, ProxyClient<RdsClient> proxyInvocation, CallbackContext callbackContext) {
        DeleteGlobalClusterResponse deleteResponse;

        if (callbackContext.isDeleting()) {
            deleteResponse = callbackContext.response("rds::delete-global-cluster");
        } else {
            deleteResponse = proxyInvocation.injectCredentialsAndInvokeV2(deleteGlobalClusterRequest, proxyInvocation.client()::deleteGlobalCluster);

            callbackContext.setDeleting(true);
        }

        return deleteResponse;
    }
}
