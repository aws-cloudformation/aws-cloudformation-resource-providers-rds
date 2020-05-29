package software.amazon.rds.dbclusterparametergroup;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;

public class UpdateHandler extends BaseHandlerStd {
    @Override
    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(final AmazonWebServicesClientProxy proxy,
                                                                          final ResourceHandlerRequest<ResourceModel> request,
                                                                          final CallbackContext callbackContext,
                                                                          final ProxyClient<RdsClient> proxyClient,
                                                                          final Logger logger) {
        final ResourceModel model = request.getDesiredResourceState();

        if (model.getParameters().equals(request.getPreviousResourceState().getParameters()))
            return describeDbClusterParameterGroup(proxy, proxyClient, request.getDesiredResourceState(), callbackContext)
                    .done((paramGroupRequest, paramGroupResponse, rdsProxyClient, resourceModel, cxt) -> tagResource(paramGroupResponse, proxyClient, resourceModel, request.getDesiredResourceTags()));

        return proxy.initiate("rds::update-db-cluster-parameter-group", proxyClient, model, callbackContext)
                .translateToServiceRequest(Translator::resetDbClusterParameterGroupRequest)
                .makeServiceCall((resetGroupRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(resetGroupRequest, proxyInvocation.client()::resetDBClusterParameterGroup))
                .done((resetGroupRequest, resetGroupResponse, proxyInvocation, resourceModel, context) -> applyParameters(proxy, proxyInvocation, resourceModel, context))
                .then(progress -> describeDbClusterParameterGroup(proxy, proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                                .done((paramGroupRequest, paramGroupResponse, rdsProxyClient, resourceModel, cxt) -> tagResource(paramGroupResponse, proxyClient, resourceModel, request.getDesiredResourceTags())));
    }
}
