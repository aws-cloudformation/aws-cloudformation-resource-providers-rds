package software.amazon.rds.dbparametergroup;

import java.util.Optional;

import com.amazonaws.util.StringUtils;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.resource.IdentifierUtils;


public class CreateHandler extends BaseHandlerStd {

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final Logger logger) {

        final ResourceModel model = request.getDesiredResourceState();
        return ProgressEvent.progress(request.getDesiredResourceState(), callbackContext)
                .then(progress -> {
                    if (StringUtils.isNullOrEmpty(model.getDBParameterGroupName()))
                        model.setDBParameterGroupName(IdentifierUtils.generateResourceIdentifier(
                                request.getStackId(),
                                Optional.ofNullable(request.getLogicalResourceIdentifier()).orElse("dbparametergroup"),
                                request.getClientRequestToken(),
                                MAX_LENGTH_GROUP_NAME
                        ).toLowerCase());
                    return ProgressEvent.progress(model, progress.getCallbackContext());
                })
                .then(progress -> proxy.initiate("rds::create-db-parameter-group", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                        .translateToServiceRequest(resourceModel -> Translator.createDbParameterGroupRequest(resourceModel, request.getDesiredResourceTags()))
                        .backoffDelay(CONSTANT)
                        .makeServiceCall((createDBParameterGroupRequest, proxyInvocation) ->
                                proxyInvocation.injectCredentialsAndInvokeV2(createDBParameterGroupRequest, proxyInvocation.client()::createDBParameterGroup))
                        .handleError((createDBParameterGroupRequest, exception, client, resourceModel, ctx) -> handleException(exception))
                        .done((paramGroupRequest, paramGroupResponse, proxyInvocation, resourceModel, context) -> applyParameters(proxy, proxyInvocation, resourceModel, context)))
                .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }
}
