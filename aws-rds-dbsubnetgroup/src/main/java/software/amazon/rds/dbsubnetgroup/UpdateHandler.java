package software.amazon.rds.dbsubnetgroup;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class UpdateHandler extends BaseHandlerStd {

  protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
      final AmazonWebServicesClientProxy proxy,
      final ResourceHandlerRequest<ResourceModel> request,
      final CallbackContext callbackContext,
      final ProxyClient<RdsClient> proxyClient,
      final Logger logger) {
        return proxy.initiate("rds::update-dbsubnet-group", proxyClient, request.getDesiredResourceState(), callbackContext)
                .translateToServiceRequest(Translator::modifyDbSubnetGroupRequest)
                .backoffDelay(CONSTANT)
                .makeServiceCall((modifyDbSubnetGroupRequest, proxyInvocation) ->
                    proxyInvocation.injectCredentialsAndInvokeV2(modifyDbSubnetGroupRequest, proxyInvocation.client()::modifyDBSubnetGroup))
                .stabilize((modifyDbSubnetGroupRequest, modifyDbSubnetGroupResponse, proxyInvocation, resourceModel, context) ->
                    isStabilized(resourceModel, proxyInvocation)).progress()
                .then(progress -> tagResource(proxy, proxyClient, progress))
                .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }
}
