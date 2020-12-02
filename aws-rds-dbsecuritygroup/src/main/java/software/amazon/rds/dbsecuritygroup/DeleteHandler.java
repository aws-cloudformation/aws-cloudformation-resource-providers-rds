package software.amazon.rds.dbsecuritygroup;

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

        return proxy.initiate("rds::delete-db-security-group", proxyClient, request.getDesiredResourceState(), callbackContext)
                .translateToServiceRequest(Translator::deleteDBSecurityGroupRequest)
                .backoffDelay(BACK_OFF)
                .makeServiceCall((deleteRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                        deleteRequest,
                        proxyInvocation.client()::deleteDBSecurityGroup
                ))
                .stabilize((deleteRequest, deleteResponse, proxyInvocation, model, context) -> isDBSecurityGroupDeleted(
                        model, proxyInvocation
                ))
                .handleError((deleteRequest, exception, client, model, context) -> handleException(
                        ProgressEvent.progress(model, context),
                        exception
                ))
                .done((deleteRequest, deleteResponse, proxyInvocation, model, context) -> ProgressEvent.defaultSuccessHandler(null));
    }
}
