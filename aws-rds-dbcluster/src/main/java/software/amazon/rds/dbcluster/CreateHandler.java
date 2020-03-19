package software.amazon.rds.dbcluster;

import com.amazonaws.util.StringUtils;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.CloudwatchLogsExportConfiguration;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.CallChain;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.resource.IdentifierUtils;

import static software.amazon.rds.dbcluster.ModelAdapter.setDefaults;

public class CreateHandler extends BaseHandlerStd {

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(final AmazonWebServicesClientProxy proxy,
                                                                          final ResourceHandlerRequest<ResourceModel> request,
                                                                          final CallbackContext callbackContext,
                                                                          final ProxyClient<RdsClient> proxyClient,
                                                                          final Logger logger) {
        ResourceModel model = request.getDesiredResourceState();
        // setting up primary id if not there yet
        if (StringUtils.isNullOrEmpty(model.getDBClusterIdentifier()))
            model.setDBClusterIdentifier(IdentifierUtils.generateResourceIdentifier(request.getLogicalResourceIdentifier(), request.getClientRequestToken(), DBCLUSTER_ID_MAX_LENGTH).toLowerCase());

        return createDBCluster(proxy, proxyClient, setDefaults(model), callbackContext)
                .stabilize((createDbClusterRequest, createDbClusterResponse, proxyInvocation, resourceModel, context) -> isDBClusterStabilized(proxyInvocation, model, DBClusterStatus.Available))
                .progress()
                .then(progress -> {
                    // check if db cluster was restored and needs post-restore update | escapes if regular create or was already updated
                    if (StringUtils.isNullOrEmpty(progress.getResourceModel().getSnapshotIdentifier()) || progress.getCallbackContext().isModified())
                        return ProgressEvent.defaultInProgressHandler(progress.getCallbackContext(), NO_CALLBACK_DELAY, progress.getResourceModel());
                    return modifyDBCluster(proxy, proxyClient, progress, CloudwatchLogsExportConfiguration.builder().build());
                })
                .then(progress -> waitForDBCluster(proxy, proxyClient, progress, DBClusterStatus.Available))
                .then(progress -> addAssociatedRoles(proxy, proxyClient, progress, progress.getResourceModel().getAssociatedRoles()))
                .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }

    // Create or Restore DBCluster depends on the set of inputs
    protected CallChain.Stabilizer<?, ?, RdsClient, ResourceModel, CallbackContext> createDBCluster(final AmazonWebServicesClientProxy proxy,
                                                                                                    final ProxyClient<RdsClient> proxyClient,
                                                                                                    final ResourceModel model,
                                                                                                    final CallbackContext callbackContext) {
        if (!StringUtils.isNullOrEmpty(model.getSourceDBClusterIdentifier())) {
            // restore to point in time
            return proxy.initiate("rds::restore-dbcluster-in-time", proxyClient, model, callbackContext)
                    .request(Translator::restoreDbClusterToPointInTimeRequest)
                    .retry(CONSTANT)
                    .call((dbClusterRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(dbClusterRequest, proxyInvocation.client()::restoreDBClusterToPointInTime));
        } else if (!StringUtils.isNullOrEmpty(model.getSnapshotIdentifier())) {
            // restore from snapshot
            return proxy.initiate("rds::restore-dbcluster-snapshot", proxyClient, model, callbackContext)
                    .request(Translator::restoreDbClusterFromSnapshotRequest)
                    .retry(CONSTANT)
                    .call((dbClusterRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(dbClusterRequest, proxyInvocation.client()::restoreDBClusterFromSnapshot));
        } else {
            // regular create
            return proxy.initiate("rds::create-dbcluster", proxyClient, model, callbackContext)
                    .request(Translator::createDbClusterRequest)
                    .retry(CONSTANT)
                    .call((dbClusterRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(dbClusterRequest, proxyInvocation.client()::createDBCluster));
        }
    }
}
