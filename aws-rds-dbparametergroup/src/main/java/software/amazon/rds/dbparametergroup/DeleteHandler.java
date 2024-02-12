package software.amazon.rds.dbparametergroup;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.logging.RequestLogger;

public class DeleteHandler extends BaseHandlerStd {

    public DeleteHandler() {
        this(HandlerConfig.builder().build());
    }

    public DeleteHandler(final HandlerConfig config) {
        super(config);
    }

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext) {
        return proxy.initiate("rds::delete-db-parameter-group", proxyClient, request.getDesiredResourceState(), callbackContext)
                .translateToServiceRequest(Translator::deleteDbParameterGroupRequest)
                .makeServiceCall((deleteGroupRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(deleteGroupRequest, proxyInvocation.client()::deleteDBParameterGroup))
                .handleError((deleteGroupRequest, exception, client, resourceModel, ctx) ->
                        Commons.handleException(
                                ProgressEvent.progress(resourceModel, ctx),
                                exception,
                                DEFAULT_DB_PARAMETER_GROUP_ERROR_RULE_SET,
                                requestLogger
                        ))
                .done((deleteGroupRequest, deleteGroupResponse, proxyInvocation, resourceModel, context) -> ProgressEvent.defaultSuccessHandler(null));
    }
}
