package software.amazon.rds.dbcluster;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DeleteDbClusterRequest;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.resource.IdentifierUtils;

import java.util.function.Function;

import static software.amazon.rds.dbcluster.Translator.deleteDbClusterRequest;

public class DeleteHandler extends BaseHandlerStd {

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(final AmazonWebServicesClientProxy proxy,
                                                                          final ResourceHandlerRequest<ResourceModel> request,
                                                                          final CallbackContext callbackContext,
                                                                          final ProxyClient<RdsClient> proxyClient,
                                                                          final Logger logger) {
        return proxy.initiate("rds::delete-dbcluster", proxyClient, request.getDesiredResourceState(), callbackContext)
                // request to delete db cluster
                .translateToServiceRequest(Translator::deleteDbClusterRequest)
                .backoffDelay(BACKOFF_STRATEGY)
                .makeServiceCall((deleteDbClusterRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(deleteDbClusterRequest, proxyInvocation.client()::deleteDBCluster))
                // wait until deleted
                .stabilize((deleteDbClusterRequest, deleteDbClusterResponse, proxyInvocation, model, context) -> isDBClusterStabilized(proxyInvocation, model, DBClusterStatus.Deleted))
                .success();
    }
}
