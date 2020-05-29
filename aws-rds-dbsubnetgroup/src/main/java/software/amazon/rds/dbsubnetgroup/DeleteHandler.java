package software.amazon.rds.dbsubnetgroup;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class DeleteHandler extends BaseHandlerStd {
    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final ProxyClient<RdsClient> proxyClient,
        final Logger logger) {
        return proxy.initiate("rds::delete-dbsubnet-group", proxyClient, request.getDesiredResourceState(), callbackContext)
            .translateToServiceRequest(Translator::deleteDbSubnetGroupRequest)
            .backoffDelay(CONSTANT)
            .makeServiceCall((deleteDbSubnetGroupRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(deleteDbSubnetGroupRequest, proxyInvocation.client()::deleteDBSubnetGroup))
            .stabilize((deleteDbSubnetGroupRequest, deleteDbSubnetGroupResponse, proxyInvocation, resourceModel, context) -> isDeleted(resourceModel, proxyInvocation))
            .success();
    }
}
