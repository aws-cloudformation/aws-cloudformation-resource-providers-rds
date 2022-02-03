package software.amazon.rds.dbparametergroup;

import java.util.Optional;

import com.amazonaws.util.StringUtils;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.resource.IdentifierUtils;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.Tagging;
import software.amazon.rds.common.logging.RequestLogger;


public class CreateHandler extends BaseHandlerStd {

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final RequestLogger requestLogger) {
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
                        .translateToServiceRequest(resourceModel -> Translator.createDbParameterGroupRequest(resourceModel, Tagging.mergeTags(request.getSystemTags(), request.getDesiredResourceTags())))
                        .backoffDelay(CONSTANT)
                        .makeServiceCall((createDBParameterGroupRequest, proxyInvocation) ->
                                proxyInvocation.injectCredentialsAndInvokeV2(createDBParameterGroupRequest, proxyInvocation.client()::createDBParameterGroup))
                        .handleError((createDBParameterGroupRequest, exception, client, resourceModel, ctx) ->
                                Commons.handleException(
                                        ProgressEvent.progress(resourceModel, ctx),
                                        exception,
                                        DEFAULT_DB_PARAMETER_GROUP_ERROR_RULE_SET))
                        .done((paramGroupRequest, paramGroupResponse, proxyInvocation, resourceModel, context) -> applyParameters(proxy, proxyInvocation, resourceModel, context, requestLogger)))
                .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, requestLogger));
    }
}
