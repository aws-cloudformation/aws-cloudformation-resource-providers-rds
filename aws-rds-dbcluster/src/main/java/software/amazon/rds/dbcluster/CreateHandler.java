package software.amazon.rds.dbcluster;

import com.amazonaws.util.StringUtils;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.CloudwatchLogsExportConfiguration;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.resource.IdentifierUtils;

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

        return ProgressEvent.progress(model, callbackContext)
            // Create or Restore DBCluster depends on the set of inputs
            .then(progress -> {
                if (!StringUtils.isNullOrEmpty(progress.getResourceModel().getSourceDBClusterIdentifier())) {
                    // restore to point in time
                    return proxy.initiate("rds::restore-dbcluster-in-time", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                        .translateToServiceRequest(Translator::restoreDbClusterToPointInTimeRequest)
                        .backoffDelay(BACKOFF_STRATEGY)
                        .makeServiceCall((dbClusterRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(dbClusterRequest, proxyInvocation.client()::restoreDBClusterToPointInTime))
                        .progress();
                }
                return progress;
            })
            .then(progress -> {
                if (!StringUtils.isNullOrEmpty(progress.getResourceModel().getSnapshotIdentifier()) &&
                    StringUtils.isNullOrEmpty(progress.getResourceModel().getSourceDBClusterIdentifier())) {
                    // restore from snapshot
                    return proxy.initiate("rds::restore-dbcluster-snapshot", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                        .translateToServiceRequest(Translator::restoreDbClusterFromSnapshotRequest)
                        .backoffDelay(BACKOFF_STRATEGY)
                        .makeServiceCall((dbClusterRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(dbClusterRequest, proxyInvocation.client()::restoreDBClusterFromSnapshot))
                        .progress();
                }
                return progress;
            })
            .then(progress -> {
                if(StringUtils.isNullOrEmpty(progress.getResourceModel().getSnapshotIdentifier()) &&
                    StringUtils.isNullOrEmpty(progress.getResourceModel().getSourceDBClusterIdentifier())) {
                    // regular create dbcluster
                    return proxy
                        .initiate("rds::create-dbcluster", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                        .translateToServiceRequest(Translator::createDbClusterRequest)
                        .backoffDelay(BACKOFF_STRATEGY)
                        .makeServiceCall((dbClusterRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(dbClusterRequest, proxyInvocation.client()::createDBCluster))
                        .progress();
                }
                return progress;
            })
            .then(progress -> waitForDBClusterAvailableStatus(proxy, proxyClient, progress))
            .then(progress -> {
                // check if db cluster was restored and needs post-restore update
                if (!StringUtils.isNullOrEmpty(progress.getResourceModel().getSnapshotIdentifier()))
                    return modifyDBCluster(proxy, proxyClient, progress, CloudwatchLogsExportConfiguration.builder().build());
                return progress;
            })
            .then(progress -> waitForDBClusterAvailableStatus(proxy, proxyClient, progress))
            .then(progress -> addAssociatedRoles(proxy, proxyClient, progress, progress.getResourceModel().getAssociatedRoles()))
            .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }
}
