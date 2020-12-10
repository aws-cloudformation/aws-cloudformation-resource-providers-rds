package software.amazon.rds.dbparametergroup;

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

        final ResourceModel model = request.getDesiredResourceState();
        //Parameters are the same. No need to make reset and modify parameter requests. We need only to update tags
        final boolean skipUpdatingParameters = model.getParameters().equals(request.getPreviousResourceState().getParameters());
        return ProgressEvent.progress(model, callbackContext)
                .then(progress -> {
                    if (skipUpdatingParameters) return progress;
                    proxy.initiate("rds::update-db-parameter-group", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                            .translateToServiceRequest(Translator::resetDbParameterGroupRequest)
                            .backoffDelay(CONSTANT)
                            .makeServiceCall((resetGroupRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(resetGroupRequest, proxyInvocation.client()::resetDBParameterGroup))
                            .done((resetGroupRequest, resetGroupResponse, proxyInvocation, resourceModel, context) -> applyParameters(proxy, proxyInvocation, resourceModel, context));
                    return ProgressEvent.progress(model, callbackContext);
                })
                .then(progress -> tagResource(proxy, proxyClient, progress, request.getDesiredResourceTags()))
                .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }
}
