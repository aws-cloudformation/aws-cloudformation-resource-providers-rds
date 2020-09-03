package software.amazon.rds.optiongroup;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.OptionGroupNotFoundException;
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

        return proxy.initiate("rds::delete-option-group", proxyClient, request.getDesiredResourceState(), callbackContext)
                .translateToServiceRequest(Translator::deleteOptionGroupRequest)
                .backoffDelay(CONSTANT)
                .makeServiceCall((deleteOptionGroupRequest, proxyInvocation) -> {
                    return proxyInvocation.injectCredentialsAndInvokeV2(
                            deleteOptionGroupRequest,
                            proxyInvocation.client()::deleteOptionGroup
                    );
                })
                .handleError((deleteOptionGroupRequest, exception, client, resourceModel, ctx) -> {
                    if (exception instanceof OptionGroupNotFoundException) {
                        return ProgressEvent.defaultFailureHandler(exception, HandlerErrorCode.NotFound);
                    }
                    throw exception;
                })
                .done((deleteOptionGroupRequest, deleteOptionGroupResponse, client, model, ctx) -> {
                    return ProgressEvent.defaultSuccessHandler(model);
                });
    }
}
