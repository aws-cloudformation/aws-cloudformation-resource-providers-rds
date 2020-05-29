package software.amazon.rds.dbclusterparametergroup;

import com.amazonaws.util.StringUtils;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.resource.IdentifierUtils;



public class CreateHandler extends BaseHandlerStd {
    @Override
    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(final AmazonWebServicesClientProxy proxy,
                                                                          final ResourceHandlerRequest<ResourceModel> request,
                                                                          final CallbackContext callbackContext,
                                                                          final ProxyClient<RdsClient> proxyClient,
                                                                          final Logger logger) {
        final ResourceModel model = request.getDesiredResourceState();
        if (StringUtils.isNullOrEmpty(model.getId()))
            model.setId(IdentifierUtils.generateResourceIdentifier(request.getLogicalResourceIdentifier(), request.getClientRequestToken(), MAX_LENGTH_GROUP_NAME).toLowerCase());


        return proxy.initiate("rds::create-db-cluster-parameter-group", proxyClient, model, callbackContext)
                .translateToServiceRequest((resourceModel) -> Translator.createDbClusterParameterGroupRequest(resourceModel, request.getDesiredResourceTags()))
                .makeServiceCall((paramGroupRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(paramGroupRequest, proxyInvocation.client()::createDBClusterParameterGroup))
                .done((paramGroupRequest, paramGroupResponse, proxyInvocation, resourceModel, context) -> applyParameters(proxy, proxyInvocation, resourceModel, context))
                .then(progress -> ProgressEvent.defaultSuccessHandler(progress.getResourceModel()));
    }
}
