package software.amazon.rds.dbclusterparametergroup;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;

public class DeleteHandler extends BaseHandlerStd {
    @Override
    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(final AmazonWebServicesClientProxy proxy,
                                                                          final ResourceHandlerRequest<ResourceModel> request,
                                                                          final CallbackContext callbackContext,
                                                                          final ProxyClient<RdsClient> proxyClient,
                                                                          final Logger logger) {
        return proxy.initiate("rds::delete-db-cluster-parameter-group", proxyClient, request.getDesiredResourceState(), callbackContext)
                .request(Translator::deleteDbClusterParameterGroupRequest)
                .call((deleteGroupRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(deleteGroupRequest, proxyInvocation.client()::deleteDBClusterParameterGroup))
                .done((paramGroupRequest, paramGroupResponse, proxyInvocation, resourceModel, context) -> ProgressEvent.defaultSuccessHandler(resourceModel));
    }
}
