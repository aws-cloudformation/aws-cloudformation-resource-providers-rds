package software.amazon.rds.dbclusterparametergroup;

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
        this(DEFAULT_HANDLER_CONFIG);
    }

    public DeleteHandler(final HandlerConfig config) {
        super(config);
    }

    @Override
    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final RequestLogger logger
    ) {
        return proxy.initiate("rds::delete-db-cluster-parameter-group", proxyClient, request.getDesiredResourceState(), callbackContext)
                .translateToServiceRequest(Translator::deleteDbClusterParameterGroupRequest)
                .makeServiceCall((deleteGroupRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(deleteGroupRequest, proxyInvocation.client()::deleteDBClusterParameterGroup))
                .handleError((deleteGroupRequest, exception, client, resourceModel, cxt) -> Commons.handleException(
                        ProgressEvent.progress(resourceModel, cxt),
                        exception,
                        DEFAULT_DB_CLUSTER_PARAMETER_GROUP_ERROR_RULE_SET, requestLogger))
                .done((deleteGroupRequest, deleteGroupResponse, proxyInvocation, resourceModel, context) -> ProgressEvent.defaultSuccessHandler(null));
    }
}
