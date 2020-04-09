package software.amazon.rds.optiongroup;

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
        return proxy.initiate("rds::delete-option-group", proxyClient, request.getDesiredResourceState(), callbackContext)
            .request(Translator::deleteOptionGroupRequest)
            .call((deleteOptionGroupRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(deleteOptionGroupRequest, proxyInvocation.client()::deleteOptionGroup))
            .success();
    }
}
