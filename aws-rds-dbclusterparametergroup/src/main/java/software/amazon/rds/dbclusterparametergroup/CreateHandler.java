package software.amazon.rds.dbclusterparametergroup;

import com.amazonaws.util.StringUtils;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DbParameterGroupAlreadyExistsException;
import software.amazon.awssdk.services.rds.model.DbParameterGroupNotFoundException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
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

        return ProgressEvent.progress(request.getDesiredResourceState(), callbackContext)
                .then(progress -> {
                    final ResourceModel model = progress.getResourceModel();
                    if (StringUtils.isNullOrEmpty(model.getDBClusterParameterGroupName()))
                        model.setDBClusterParameterGroupName(IdentifierUtils.generateResourceIdentifier(
                                request.getStackId(),
                                request.getLogicalResourceIdentifier(),
                                request.getClientRequestToken(),
                                MAX_LENGTH_GROUP_NAME).toLowerCase());
                    return ProgressEvent.progress(model, progress.getCallbackContext());
                })
                .then(progress -> proxy.initiate("rds::create-db-cluster-parameter-group", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                        .translateToServiceRequest((resourceModel) -> Translator.createDbClusterParameterGroupRequest(resourceModel, request.getDesiredResourceTags()))
                        .makeServiceCall((paramGroupRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(paramGroupRequest, proxyInvocation.client()::createDBClusterParameterGroup))
                        .done((paramGroupRequest, paramGroupResponse, proxyInvocation, resourceModel, context) -> applyParameters(proxy, proxyInvocation, resourceModel, context)))
                .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));

    }
}
