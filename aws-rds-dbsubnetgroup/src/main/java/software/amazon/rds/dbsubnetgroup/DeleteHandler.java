package software.amazon.rds.dbsubnetgroup;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;

public class DeleteHandler extends BaseHandlerStd {

    public DeleteHandler() {
        this(HandlerConfig.builder().build());
    }

    public DeleteHandler(final HandlerConfig config) {
        super(config);
    }

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final Logger logger) {
        return proxy.initiate("rds::delete-dbsubnet-group", proxyClient, request.getDesiredResourceState(), callbackContext)
                .translateToServiceRequest(Translator::deleteDbSubnetGroupRequest)
                .backoffDelay(config.getBackoff())
                .makeServiceCall((deleteDbSubnetGroupRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(deleteDbSubnetGroupRequest, proxyInvocation.client()::deleteDBSubnetGroup))
                .stabilize((deleteDbSubnetGroupRequest, deleteDbSubnetGroupResponse, proxyInvocation, resourceModel, context) -> isDeleted(resourceModel, proxyInvocation))
                .handleError((deleteDbSubnetGroupRequest, exception, client, resourceModel, cxt) -> Commons.handleException(
                        ProgressEvent.progress(resourceModel, cxt),
                        exception,
                        DEFAULT_DB_SUBNET_GROUP_ERROR_RULE_SET))
                .done((deleteDbSubnetGroupRequest, deleteDbSubnetGroupResponse, client, model, cxt) -> ProgressEvent.defaultSuccessHandler(null));
    }
}
