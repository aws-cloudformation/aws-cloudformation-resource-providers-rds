package software.amazon.rds.dbclustersnapshot;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBClusterSnapshot;
import software.amazon.awssdk.services.rds.model.DbClusterSnapshotAlreadyExistsException;
import software.amazon.awssdk.services.rds.model.DbClusterSnapshotNotFoundException;
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
        return proxy.initiate("rds::delete-db-cluster-snapshot", proxyClient, request.getDesiredResourceState(), callbackContext)
                .translateToServiceRequest(Translator::deleteDbClusterSnapshotRequest)
                .backoffDelay(config.getBackoff())
                .makeServiceCall((deleteRequest, proxyInvocation) ->
                        proxyInvocation.injectCredentialsAndInvokeV2(deleteRequest, proxyInvocation.client()::deleteDBClusterSnapshot))
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
            fetchDBClusterSnapshot(model, proxyClient);
            return false;
        } catch (DbClusterSnapshotNotFoundException e) {
            return true;
        }
    }
}
