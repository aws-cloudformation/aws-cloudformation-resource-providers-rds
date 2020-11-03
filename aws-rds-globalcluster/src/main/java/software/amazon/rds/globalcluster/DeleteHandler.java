package software.amazon.rds.globalcluster;

import java.util.function.Function;

import software.amazon.awssdk.services.rds.RdsClient;
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

        if (callbackContext.isDeleting()) {
            result = ProgressEvent.progress(model, callbackContext)
                    .then(progress -> proxy.initiate("rds::stabilize-dbcluster" + getClass().getSimpleName(), proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                            .translateToServiceRequest(Function.identity())
                            .makeServiceCall(EMPTY_CALL)
                            .stabilize((deleteGlobalClusterRequest, deleteGlobalClusterResponse, stabilizeProxy, stabilizeModel, context)
                                    -> isDeleted(stabilizeModel, stabilizeProxy))
                            .success());
        } else {
            result = ProgressEvent.progress(model, callbackContext)
                    .then(progress -> removeFromGlobalCluster(proxy, proxyClient, progress))
                    .then(progress -> waitForDBClusterAvailableStatus(proxy, proxyClient, progress))
                    .then(progress -> proxy.initiate("rds::delete-global-cluster", proxyClient, request.getDesiredResourceState(), callbackContext)
                            .translateToServiceRequest(Translator::deleteGlobalClusterRequest)
                            .backoffDelay(BACKOFF_STRATEGY)
                            .makeServiceCall((deleteGlobalClusterRequest, proxyInvocation)
                                    -> proxyInvocation.injectCredentialsAndInvokeV2(deleteGlobalClusterRequest, proxyInvocation.client()::deleteGlobalCluster))
                            // wait until deleted
                            .stabilize((deleteGlobalClusterRequest, deleteGlobalClusterResponse, stabilizeProxy, stabilizeModel, context)
                                    -> isDeleted(stabilizeModel, stabilizeProxy))
                            .success());

            callbackContext.setDeleting(true);
        }

        if (result.isSuccess()) {
            result.setResourceModel(null);
        }

        return result;
    }
}
