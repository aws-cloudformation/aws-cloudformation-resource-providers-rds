package software.amazon.rds.dbsnapshot;

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
            final ProxyClient<RdsClient> client,
            final Logger logger
    ) {
        return proxy.initiate("rds::delete-db-snapshot", client, request.getDesiredResourceState(), callbackContext)
                .translateToServiceRequest(Translator::deleteDBSnapshotRequest)
                .makeServiceCall((deleteRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                        deleteRequest,
                        proxyInvocation.client()::deleteDBSnapshot
                ))
                .stabilize((deleteRequest, deleteResponse, proxyClient, model, context) -> isDBSnapshotDeleted(proxyClient, model))
                .handleError((deleteRequest, exception, rdsClient, model, context) -> Commons.handleException(
                        ProgressEvent.progress(model, context),
                        exception,
                        DEFAULT_DB_SNAPSHOT_ERROR_RULE_SET
                ))
                .done((deleteRequest, deleteResponse, proxyInvocation, model, context) -> ProgressEvent.defaultSuccessHandler(null));
    }
}
