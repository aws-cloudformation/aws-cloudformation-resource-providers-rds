package software.amazon.rds.dbsnapshot;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DbSnapshotNotFoundException;
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
        return proxy.initiate("rds::delete-db-snapshot", proxyClient, request.getDesiredResourceState(), callbackContext)
                .translateToServiceRequest(Translator::deleteDbSnapshotRequest)
                .backoffDelay(config.getBackoff())
                .makeServiceCall((deleteRequest, proxyInvocation) ->
                        proxyInvocation.injectCredentialsAndInvokeV2(deleteRequest, proxyInvocation.client()::deleteDBSnapshot))
                .stabilize((deleteRequest, deleteResponse, proxyInvocation, model, context) ->
                        isDeleted(model, proxyInvocation))
                .handleError((deleteRequest, exception, client, resourceModel, ctx) -> Commons.handleException(
                        ProgressEvent.progress(resourceModel, ctx),
                        exception,
                        DEFAULT_DB_SNAPSHOT_ERROR_RULE_SET))
                .done(progress -> ProgressEvent.defaultSuccessHandler(null));
    }

    protected boolean isDeleted(final ResourceModel model,
                                final ProxyClient<RdsClient> proxyClient) {
        try {
            fetchDBSnapshot(model, proxyClient);
            return false;
        } catch (DbSnapshotNotFoundException e) {
            return true;
        }
    }
}
