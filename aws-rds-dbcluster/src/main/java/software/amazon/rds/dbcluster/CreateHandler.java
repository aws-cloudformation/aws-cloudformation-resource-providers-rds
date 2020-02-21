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
import software.amazon.rds.dbcluster.DBClusterStatus.Status;

import static software.amazon.rds.dbcluster.ModelAdapter.setDefaults;

public class CreateHandler extends BaseHandlerStd {

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(final AmazonWebServicesClientProxy proxy,
                                                                          final ResourceHandlerRequest<ResourceModel> request,
                                                                          final CallbackContext callbackContext,
                                                                          final ProxyClient<RdsClient> proxyClient,
                                                                          final Logger logger) {
        ResourceModel model = request.getDesiredResourceState();
        if (StringUtils.isNullOrEmpty(model.getDBClusterIdentifier()))
            model.setDBClusterIdentifier(IdentifierUtils.generateResourceIdentifier(request.getLogicalResourceIdentifier(), request.getClientRequestToken(), 63).toLowerCase());

        return createDBCluster(proxy, proxyClient, setDefaults(model), callbackContext)
                .stabilize((createDbClusterRequestCallback, createDbClusterResponseCallback, proxyInvocationCallback, modelCallback, callbackContextCallback) ->
                        isDBClusterStabilized(proxyInvocationCallback, modelCallback, Status.Available))
                .progress()
                .then(progress -> {
                    if (StringUtils.isNullOrEmpty(progress.getResourceModel().getSnapshotIdentifier()))
                        return ProgressEvent.defaultInProgressHandler(progress.getCallbackContext(), NO_CALLBACK_DELAY, progress.getResourceModel());
                    return modifyDBCluster(proxy, proxyClient, progress, CloudwatchLogsExportConfiguration.builder().build());
                })
                .then(progress -> waitForDBCluster(proxy, proxyClient, progress, Status.Available))
                .then(progress -> addAssociatedRoles(proxy, proxyClient, progress, progress.getResourceModel().getAssociatedRoles()))
                .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }
}
