package software.amazon.rds.dbparametergroup;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DbParameterGroupNotFoundException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
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

        return proxy.initiate("rds::delete-db-parameter-group", proxyClient, request.getDesiredResourceState(), callbackContext)
                .translateToServiceRequest(Translator::deleteDbParameterGroupRequest)
                .makeServiceCall((deleteGroupRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(deleteGroupRequest, proxyInvocation.client()::deleteDBParameterGroup))
                .handleError((deleteGroupRequest, exception, client, resourceModel, cxt) -> {
                    if (exception instanceof DbParameterGroupNotFoundException)
                        return ProgressEvent.defaultFailureHandler(exception, HandlerErrorCode.NotFound);
                    throw exception;
                })
                .done((deleteGroupRequest, deleteGroupResponse, proxyInvocation, resourceModel, context) -> ProgressEvent.defaultSuccessHandler(null));
    }
}
